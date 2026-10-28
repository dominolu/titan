"""Pure admission/proof checks shared by the finite live-fill probe and server tests."""
from decimal import Decimal

def validate_parameters(parameters):
    expected={'direction':'long_bias','grid_levels':1,'buy_levels':1,'sell_levels':0,'max_position_lots':1,'order_qty_lots':1,'stop_action':'flatten','leverage':1,'quote_units_per_tick_lot':1000,'max_slippage_ticks':5,'flatten_timeout_ns':8_000_000_000}
    assert all(parameters.get(k)==v for k,v in expected.items()),'one-lot hold-to-stop fixture required'
    assert parameters['allocated_capital_tick_lots']==parameters['max_notional_tick_lots']==10_000_000,'100 USDT fixture budget changed'

def checkpoint_has_position(point):
    baseline=point.get('recovery') or {}
    if baseline.get('pending_execution',True):return False
    positions=baseline.get('public',{}).get('positions',[])
    positions=[p for p in positions if int(p['account_no'])==0 and int(p['asset_no'])==0]
    orders=point.get('orders_list',[])
    return (len(positions)==1 and int(positions[0]['qty_lots'])==1
            and any(int(o['side'])==1 and int(o['cumulative_filled_lots'])==1 and int(o['fill_count'])>=1 for o in orders))

def position_quantity(positions):
    values=[Decimal(p['position_qty']) for p in positions]
    assert all(v.is_finite() for v in values)
    nonzero=[v for v in values if v]
    assert len(nonzero)<=1 and all(abs(v)==Decimal('0.0001') for v in nonzero),'position exceeds the one-lot acceptance scope'
    total=sum(values,Decimal(0))
    return total

def protective_close_request(quantity,bid,ask,text):
    quantity,bid,ask=map(Decimal,(quantity,bid,ask))
    assert quantity.is_finite() and abs(quantity)==Decimal('0.0001')
    assert bid.is_finite() and ask.is_finite() and 0<bid<=ask
    tick=Decimal('0.1');price=bid-tick*5 if quantity>0 else ask+tick*5
    assert price>0 and abs(price-(bid+ask)/2)/((bid+ask)/2)<=Decimal('.005')
    assert abs(quantity)*price<=100
    return {'text':text,'symbol':'GATE_FUTURE_BTC_USDT','side':'SELL' if quantity>0 else 'BUY','type':'LIMIT','time_in_force':'IOC','qty':str(abs(quantity)),'price':str(price),'reduce_only':'true','position_side':'NONE'}

def unactivated_fixture(root):
    """Reuse only a proven never-activated fixture; never erase its receipt or storage."""
    import json,sqlite3,tomllib
    with sqlite3.connect('file:'+str(root/'manager.sqlite3')+'?mode=ro',uri=True) as db:
        assert db.execute('SELECT COUNT(*) FROM occupancy').fetchone()[0]==0,'original account reservation not safely released'
        operations=[json.loads(v[0]) for v in db.execute('SELECT data FROM operations')]
    starts=[v for v in operations if v['input']['kind']=='start'];assert starts
    assert all(v['state'] in ('succeeded','failed','cancelled') for v in operations),'unknown or active operation remains'
    for op in starts:
        assert op['state']=='failed'
        proof=json.loads((root/'node/managed'/op['id']/'final.json').read_text())
        assert proof.get('source')=='runtime_owner' and proof.get('run_id')==op['id'] and proof.get('start_admission_absent') is True,'missing positive E non-activation proof'
    for path in (root/'node/checkpoints').rglob('runtime.db'):
        with sqlite3.connect('file:'+str(path)+'?mode=ro',uri=True) as db:
            assert db.execute('SELECT COUNT(*) FROM recovery_points').fetchone()[0]==0
            assert db.execute('SELECT COUNT(*) FROM strategy_generations').fetchone()[0]==0,'execution generation was already reserved'
    cfg=tomllib.loads((root/'runtime.toml').read_text());runtime=cfg['strategies'][0]
    assert runtime['runtime']['checkpoint_interval']['secs']==60 and runtime['runtime']['stop_timeout']['secs']==10 and runtime['runtime']['max_handler_duration']['secs']==1
    return runtime['runtime_strategy_no']

def stopped_fixture(root):
    """Reuse the original namespace only after every earlier owner proved safe Stop."""
    import json,sqlite3,tomllib
    with sqlite3.connect('file:'+str(root/'manager.sqlite3')+'?mode=ro',uri=True) as db:
        assert db.execute('SELECT COUNT(*) FROM occupancy').fetchone()[0]==0,'original reservation not safely released'
        operations=[json.loads(v[0]) for v in db.execute('SELECT data FROM operations')]
    assert all(v['state'] in ('succeeded','failed','cancelled') for v in operations)
    starts=[v for v in operations if v['input']['kind']=='start'];assert starts
    for op in starts:
        proof=json.loads((root/'node/managed'/op['id']/'final.json').read_text())
        assert proof.get('run_id')==op['id']
        absent=op['state']=='failed' and proof.get('source')=='runtime_owner' and proof.get('start_admission_absent') is True
        safe=op['state']=='succeeded' and proof.get('source')=='trading_runtime' and proof.get('release_proof',{}).get('safe') is True and proof.get('process_state')=='stopped'
        assert absent or safe,'earlier owner lacks a positive non-activation or safe Stop proof'
    cfg=tomllib.loads((root/'runtime.toml').read_text());runtime=cfg['strategies'][0]
    assert runtime['runtime']['checkpoint_interval']['secs']==60 and runtime['runtime']['stop_timeout']['secs']==10 and runtime['runtime']['max_handler_duration']['secs']==1
    return runtime['runtime_strategy_no']

def reconciled_fixture(root, receipt_path):
    """Verify an explicit operator receipt; this function never releases a reservation."""
    import hashlib,json,sqlite3,tomllib
    receipt=json.loads(receipt_path.read_text());kind='operator_external_flat_reconciliation'
    assert receipt['kind']==kind and receipt['operator_authorized'] is True
    assert receipt['scope_root']==str(root.resolve())
    with sqlite3.connect('file:'+str(root/'manager.sqlite3')+'?mode=ro',uri=True) as db:
        assert db.execute('SELECT COUNT(*) FROM occupancy').fetchone()[0]==0
        operations=[json.loads(v[0]) for v in db.execute('SELECT data FROM operations')]
        assert all(v['state'] in ('succeeded','failed','cancelled') for v in operations)
        states=sorted((v['id'],v['state']) for v in operations)
        assert hashlib.sha256(json.dumps(states,separators=(',',':')).encode()).hexdigest()==receipt['operation_set_sha256']
        assert db.execute('SELECT COUNT(*) FROM audit WHERE actor=? AND action=? AND target=? AND digest=?',('operator',kind,receipt['original_run_id'],receipt['evidence_sha256'])).fetchone()[0]==1
    path=root/receipt['recovery_database'];assert path.resolve().is_relative_to(root.resolve())
    with sqlite3.connect('file:'+str(path)+'?mode=ro',uri=True) as db:
        assert db.execute('SELECT store_id FROM storage_meta WHERE id=1').fetchone()[0]==receipt['store_id']
        assert db.execute('SELECT generation FROM strategy_generations WHERE strategy_no=?',(receipt['runtime_strategy_no'],)).fetchone()[0]>=receipt['generation_floor']
        assert db.execute('SELECT sha256 FROM recovery_points WHERE id=?',(receipt['preserved_record_id'],)).fetchone()[0]==receipt['preserved_payload_sha256']
    cfg=tomllib.loads((root/'runtime.toml').read_text());runtime=cfg['strategies'][0]
    assert runtime['runtime_strategy_no']==receipt['runtime_strategy_no']
    assert runtime['runtime']['checkpoint_interval']['secs']==60 and runtime['runtime']['stop_timeout']['secs']==10 and runtime['runtime']['max_handler_duration']['secs']==1
    return runtime['runtime_strategy_no']
