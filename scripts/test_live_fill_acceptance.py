"""Run on the build host; no real account or credentials are used."""
import copy,json,sys,unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'python/titan-strategy-sdk/tests'))
from live_fill_acceptance import validate_parameters,checkpoint_has_position,position_quantity,protective_close_request

class PolicyTests(unittest.TestCase):
    def test_frozen_one_lot_parameters(self):
        v=json.loads((ROOT/'configs/live-fill-recovery-acceptance.parameters.json').read_text());validate_parameters(v)
        for name,value in (('max_position_lots',2),('grid_levels',2),('sell_levels',1),('order_qty_lots',2),('stop_action','cancel'),('max_slippage_ticks',6),('flatten_timeout_ns',10_000_000_000)):
            with self.subTest(name=name):
                altered=dict(v);altered[name]=value
                with self.assertRaises(AssertionError):validate_parameters(altered)
    def test_only_confirmed_consumed_checkpoint_is_eligible(self):
        v={'recovery':{'pending_execution':False,'public':{'positions':[{'account_no':0,'asset_no':0,'qty_lots':1}]}},'orders_list':[{'side':1,'cumulative_filled_lots':1,'fill_count':1}]}
        self.assertTrue(checkpoint_has_position(v))
        for change in ('pending','flat','no-fill','wrong-account'):
            q=copy.deepcopy(v)
            if change=='pending':q['recovery']['pending_execution']=True
            elif change=='flat':q['recovery']['public']['positions'][0]['qty_lots']=0
            elif change=='no-fill':q['orders_list'][0]['fill_count']=0
            else:q['recovery']['public']['positions'][0]['account_no']=1
            self.assertFalse(checkpoint_has_position(q))
    def test_safety_close_is_reduce_only_and_bounded(self):
        q=protective_close_request('0.0001','80800.0','80800.1','test-safe')
        self.assertEqual((q['side'],q['qty'],q['reduce_only'],q['time_in_force']),('SELL','0.0001','true','IOC'))
        for qty in ('0','0.00005','0.0002','NaN'):
            with self.assertRaises(AssertionError):protective_close_request(qty,'80800','80800.1','test-safe')
        with self.assertRaises(AssertionError):protective_close_request('.0001','1','100','test-safe')
    def test_position_scope_refuses_unexpected_exposure(self):
        self.assertEqual(str(position_quantity([{'position_qty':'.0001'}])),'0.0001')
        with self.assertRaises(AssertionError):position_quantity([{'position_qty':'.0002'}])
        with self.assertRaises(AssertionError):position_quantity([{'position_qty':'.0001'},{'position_qty':'-.0001'}])

    def test_reuse_requires_released_occupancy_and_positive_owner_proof(self):
        import sqlite3,tempfile
        from live_fill_acceptance import unactivated_fixture
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);managed=root/'node/managed/old-start';managed.mkdir(parents=True)
            (managed/'final.json').write_text(json.dumps({'source':'runtime_owner','run_id':'old-start','start_admission_absent':True}))
            (root/'runtime.toml').write_text('[[strategies]]\nruntime_strategy_no=77\n[strategies.runtime.checkpoint_interval]\nsecs=60\n[strategies.runtime.stop_timeout]\nsecs=10\n[strategies.runtime.max_handler_duration]\nsecs=1\n')
            with sqlite3.connect(root/'manager.sqlite3') as db:
                db.executescript('CREATE TABLE occupancy(id);CREATE TABLE operations(data);')
                db.execute('INSERT INTO operations VALUES(?)',(json.dumps({'id':'old-start','input':{'kind':'start'},'state':'failed'}),))
            self.assertEqual(unactivated_fixture(root),77)
            with sqlite3.connect(root/'manager.sqlite3') as db:db.execute('INSERT INTO occupancy VALUES(1)')
            with self.assertRaises(AssertionError):unactivated_fixture(root)
            with sqlite3.connect(root/'manager.sqlite3') as db:db.execute('DELETE FROM occupancy')
            (managed/'final.json').write_text(json.dumps({'source':'runtime_owner','run_id':'wrong','start_admission_absent':True}))
            with self.assertRaises(AssertionError):unactivated_fixture(root)

    def test_reuse_stopped_requires_every_original_owner_safe(self):
        import sqlite3,tempfile
        from live_fill_acceptance import stopped_fixture
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);managed=root/'node/managed/old-start';managed.mkdir(parents=True)
            proof={'run_id':'old-start','source':'trading_runtime','process_state':'stopped','release_proof':{'safe':True}}
            (managed/'final.json').write_text(json.dumps(proof))
            (root/'runtime.toml').write_text('[[strategies]]\nruntime_strategy_no=77\n[strategies.runtime.checkpoint_interval]\nsecs=60\n[strategies.runtime.stop_timeout]\nsecs=10\n[strategies.runtime.max_handler_duration]\nsecs=1\n')
            with sqlite3.connect(root/'manager.sqlite3') as db:
                db.executescript('CREATE TABLE occupancy(id);CREATE TABLE operations(data);')
                db.execute('INSERT INTO operations VALUES(?)',(json.dumps({'id':'old-start','input':{'kind':'start'},'state':'succeeded'}),))
            self.assertEqual(stopped_fixture(root),77)
            proof['release_proof']['safe']=False;(managed/'final.json').write_text(json.dumps(proof))
            with self.assertRaises(AssertionError):stopped_fixture(root)

    def test_operator_receipt_requires_audit_and_preserved_storage(self):
        import hashlib,sqlite3,tempfile
        from live_fill_acceptance import reconciled_fixture
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);(root/'runtime.toml').write_text('[[strategies]]\nruntime_strategy_no=77\n[strategies.runtime.checkpoint_interval]\nsecs=60\n[strategies.runtime.stop_timeout]\nsecs=10\n[strategies.runtime.max_handler_duration]\nsecs=1\n')
            with sqlite3.connect(root/'manager.sqlite3') as db:
                db.executescript('CREATE TABLE occupancy(id);CREATE TABLE operations(data);CREATE TABLE audit(actor,action,target,digest);')
                db.execute('INSERT INTO operations VALUES(?)',(json.dumps({'id':'old','state':'failed'}),))
            with sqlite3.connect(root/'runtime.db') as db:
                db.executescript("CREATE TABLE storage_meta(id,store_id);INSERT INTO storage_meta VALUES(1,'store');CREATE TABLE strategy_generations(strategy_no,generation);INSERT INTO strategy_generations VALUES(77,3);CREATE TABLE recovery_points(id,sha256);INSERT INTO recovery_points VALUES('point','sha');")
            receipt={'kind':'operator_external_flat_reconciliation','operator_authorized':True,'scope_root':str(root.resolve()),'original_run_id':'old-run','evidence_sha256':'evidence','operation_set_sha256':hashlib.sha256(json.dumps([('old','failed')],separators=(',',':')).encode()).hexdigest(),'recovery_database':'runtime.db','store_id':'store','runtime_strategy_no':77,'generation_floor':3,'preserved_record_id':'point','preserved_payload_sha256':'sha'}
            path=root/'receipt.json';path.write_text(json.dumps(receipt))
            with self.assertRaises(AssertionError):reconciled_fixture(root,path)
            with sqlite3.connect(root/'manager.sqlite3') as db:db.execute('INSERT INTO audit VALUES(?,?,?,?)',('operator',receipt['kind'],'old-run','evidence'))
            self.assertEqual(reconciled_fixture(root,path),77)
            with sqlite3.connect(root/'runtime.db') as db:db.execute('UPDATE strategy_generations SET generation=2')
            with self.assertRaises(AssertionError):reconciled_fixture(root,path)

class GridLifecycleTests(unittest.TestCase):
    def test_existing_grid_initializes_holds_and_flattens_exactly_one_lot(self):
        from numba.core.registry import CPUDispatcher
        from test_grid import Context,grid
        with ExitStack() as stack:
            for name,value in list(vars(grid).items()):
                if isinstance(value,CPUDispatcher):stack.enter_context(patch.object(grid,name,value.py_func))
            parameters=json.loads((ROOT/'configs/live-fill-recovery-acceptance.parameters.json').read_text());ctx=Context(**parameters);ctx.balance_row['available_units']=10**12;ctx.balance_row['currency_no']=1
            self.assertEqual(grid.on_start(ctx),0);ctx.tick(808000,808002)
            self.assertEqual(len(ctx.submits),1);order=ctx.submits[0];self.assertEqual((int(order['side']),int(order['qty_lots']),int(order['time_in_force']),int(order['reduce_only'])),(1,1,2,0))
            ctx.fill(int(order['order_id']),1,808002,final=True);ctx.terminal(int(order['order_id']),grid.FILLED,1)
            ctx.is_paused=True;ctx.now+=1_000_000_000;ctx.tick(808000,808002)
            self.assertEqual(int(ctx.state['grid']['expected_position_lots']),1);self.assertEqual(len(ctx.submits),1)
            # Recovered business state accepts the same authoritative inventory.
            ctx.account_row['account_epoch']=2;self.assertEqual(grid.on_recover(ctx),0);ctx.tick(808000,808002);self.assertEqual(len(ctx.submits),1)
            self.assertEqual(grid.on_stop(ctx),0);ctx.now+=500_000_000;grid.on_timer(ctx)
            self.assertEqual(len(ctx.submits),2);closing=ctx.submits[-1];self.assertEqual((int(closing['side']),int(closing['qty_lots']),int(closing['time_in_force']),int(closing['reduce_only'])),(2,1,2,1))
            ctx.fill(int(closing['order_id']),1,807999,final=True);self.assertEqual(int(ctx.position_row['qty_lots']),0);self.assertTrue(ctx.stop_complete)


class NativeOneLotTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import ctypes,os,subprocess,tempfile
        import numpy as np
        from titan_strategy.state import canonical_state_schema,state_schema_hash,validate_state_dtype
        from test_grid import grid
        cls.tmp=tempfile.TemporaryDirectory();cls.addClassCleanup(cls.tmp.cleanup)
        binary=os.environ['TITAN_TEST_BINARY'];target=Path(cls.tmp.name)/'one-lot'
        result=subprocess.run([binary,'strategy','compile','--strategy',os.environ.get('TITAN_GRID_TEST_SOURCE',str(ROOT/'strategies/grid/strategy.py')),'--parameters',str(ROOT/'configs/live-fill-recovery-acceptance.parameters.json'),'--target','x86_64-unknown-linux-gnu','--cpu-baseline','x86-64-v2','--artifact-format','pair','--output',str(target),'--json'],capture_output=True,text=True,timeout=360)
        assert result.returncode==0,result.stderr
        cls.library=ctypes.CDLL(next(p for p in json.loads(result.stdout)['paths'] if p.endswith('.so')))
        cls.schema_hash=state_schema_hash(canonical_state_schema(validate_state_dtype(grid.state_dtype,max_state_bytes=65536,max_alignment=8),schema_version=grid.SPEC.state_schema_version))
    def test_actual_native_one_lot_fill_restore_and_reduce_only_exit(self):
        import numpy as np
        from test_grid import Context,grid,row
        from titan_strategy import abi_v13
        import test_grid_native as helpers
        invoke=lambda name,ctx:helpers.TestGridNative.invoke(self,name,ctx)
        ctx=Context(**json.loads((ROOT/'configs/live-fill-recovery-acceptance.parameters.json').read_text()));ctx.balance_row['available_units']=10**12;ctx.balance_row['currency_no']=1;ctx.market_row['best_bid_ticks']=808000;ctx.market_row['best_ask_ticks']=808002
        invoke('on_start',ctx);ctx.tick_rows=np.array([row(abi_v13.tick_dtype,receive_ts_ns=ctx.now)],dtype=abi_v13.tick_dtype);invoke('on_tick',ctx)
        self.assertEqual(len(ctx.submits),1);opening=ctx.submits[0];self.assertEqual((int(opening['qty_lots']),int(opening['side']),int(opening['time_in_force'])),(1,1,2))
        ctx.active[0]['status']=grid.FILLED;ctx.active[0]['cumulative_filled_lots']=1;ctx.position_row['qty_lots']=1;ctx.position_row['average_price_ticks']=808002;ctx.position_row['account_sequence']=1
        ctx.fill_rows=np.array([row(abi_v13.fill_dtype,order_id=int(opening['order_id']),side=1,fill_qty_lots=1,cumulative_filled_lots=1,fill_price_ticks=808002,account_sequence=2,final_fill=1)],dtype=abi_v13.fill_dtype);invoke('on_fill',ctx)
        ctx.active=ctx.active[:0]
        # Real CrossEx can publish the matching position before its independent execution.
        ctx.position_row['account_sequence']=1;ctx.is_paused=True
        ctx.now+=60_000_000_000;ctx.tick_rows[0]['receive_ts_ns']=ctx.now;invoke('on_tick',ctx)
        self.assertEqual(int(ctx.state['grid']['last_error']),0)
        self.assertNotEqual(int(ctx.state['grid']['status']),grid.RECONCILING)
        ctx.account_row['account_epoch']=2;invoke('on_recover',ctx);ctx.now+=1_000_000_000;ctx.tick_rows[0]['receive_ts_ns']=ctx.now;invoke('on_tick',ctx)
        self.assertEqual(len(ctx.submits),1);self.assertEqual(int(ctx.state['grid']['expected_position_lots']),1)
        invoke('on_stop',ctx);self.assertEqual(len(ctx.submits),2);closing=ctx.submits[-1];self.assertEqual((int(closing['qty_lots']),int(closing['side']),int(closing['reduce_only']),int(closing['time_in_force'])),(1,2,1,2))
        ctx.active[0]['status']=grid.FILLED;ctx.active[0]['cumulative_filled_lots']=1;ctx.position_row['qty_lots']=0;ctx.position_row['average_price_ticks']=0;ctx.position_row['account_sequence']=3
        ctx.fill_rows[0]=row(abi_v13.fill_dtype,order_id=int(closing['order_id']),side=2,fill_qty_lots=1,cumulative_filled_lots=1,fill_price_ticks=808000,account_sequence=3,final_fill=1)
        flags=invoke('on_fill',ctx);self.assertTrue(flags&1);self.assertEqual(int(ctx.state['grid']['status']),grid.STOPPED)

if __name__=='__main__':unittest.main(verbosity=2)
