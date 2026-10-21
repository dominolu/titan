"""Independent offline analysis on the build server; no account calls or credentials."""
import csv,json,math
from pathlib import Path
root=Path(__file__).parent

def stats(values):
    values=sorted(values)
    if not values:return None
    n=len(values)
    def q(p):return values[math.ceil(p*n)-1]
    return dict(n=n,min=values[0],mean=sum(values)/n,p50=q(.5),p95=q(.95),p99=q(.99),max=values[-1],negative=sum(v<0 for v in values))

runs=[]
for phase in ['iteration1','final']:
    base=root/phase
    if not base.exists():continue
    for summary_path in sorted(base.glob('*/summary.json')):
        directory=summary_path.parent
        summary=json.loads(summary_path.read_text())
        code_path=base/(directory.name+'.exit')
        code=int(code_path.read_text()) if code_path.exists() else None
        rows=list(csv.DictReader((directory/'samples.csv').open()))
        durations=[]
        for row in rows:
            delta=int(row['strategy_entry_ns'])-int(row['receive_ns'])
            assert delta==int(row['latency_ns'])
            assert int(row['depth_levels'])==10
            durations.append(delta/1000)
        measured=stats(durations)
        if measured:
            for key,value in measured.items():
                assert math.isclose(value,summary['latency_us'][key],rel_tol=1e-10,abs_tol=1e-8),(directory,key)
        else:assert summary['latency_us'] is None
        if code==0:
            assert measured and summary['strategy_state']=='Stopped'
            assert summary['drop_total']==0 and summary['resync_total']==0 and measured['negative']==0
        audit=summary['allocation_audit']
        count=audit['callbacks']
        runs.append(dict(phase=phase,run=directory.name,venue=summary['venue'],exit_code=code,latency_us=measured,
            allocations=audit['rust_alloc_or_realloc_calls'],audit_callbacks=count,
            allocations_per_callback=audit['rust_alloc_or_realloc_calls']/count if count else None,
            durations=durations))
comparisons={}
for phase in ['iteration1','final']:
    group=[v for v in runs if v['phase']==phase and v['exit_code']==0]
    values={name:[d for v in group if v['venue']==venue for d in v['durations']] for name,venue in [('legacy','gate-legacy'),('borrowed','gate-borrowed')]}
    if all(values.values()):
        a,b=stats(values['legacy']),stats(values['borrowed'])
        profiles={name:sum(v['allocations'] for v in group if v['venue']==venue)/sum(v['audit_callbacks'] for v in group if v['venue']==venue)
            for name,venue in [('legacy','gate-legacy'),('borrowed','gate-borrowed')]}
        comparisons[phase]=dict(legacy=a,borrowed=b,p50_reduction_pct=(1-b['p50']/a['p50'])*100,
            allocations_per_callback=profiles,allocation_reduction_pct=(1-profiles['borrowed']/profiles['legacy'])*100)
for run in runs:run.pop('durations')
report=dict(independent_recomputation_passed=True,percentiles='nearest-rank; warmup excluded; no outlier or negative filtering',runs=runs,comparisons=comparisons)
(root/'analysis.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report,indent=2))
