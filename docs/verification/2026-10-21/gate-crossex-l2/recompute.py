"""Offline CSV analysis; executed on 192.168.3.88, independently of the Rust collector."""
import csv
import json
import math
from collections import defaultdict
from pathlib import Path

root = Path(__file__).parent
rows = list(csv.DictReader((root / 'data/samples.csv').open()))
summary = json.loads((root / 'data/summary.json').read_text())
groups = defaultdict(list)
for row in rows:
    groups[row['path'], row['symbol']].append(row)

def stats(values):
    values = sorted(values)
    if not values:
        return None
    count = len(values)
    def quantile(q):
        return values[max(0, math.ceil(q * count) - 1)]
    return dict(n=count, min=values[0], mean=sum(values)/count,
                p50=quantile(.5), p90=quantile(.9), p95=quantile(.95),
                p99=quantile(.99), max=values[-1],
                negative=sum(v < 0 for v in values))

series = []
for (path, symbol), values in sorted(groups.items()):
    observed = [int(v['observed_ns']) for v in values]
    source = [int(v['source_ms']) for v in values]
    monotonic = [int(v['monotonic_elapsed_ns']) for v in values]
    out = dict(path=path, symbol=symbol, samples=len(values))
    out['source_age_ms'] = stats([(o-s*1_000_000)/1e6 for o, s in zip(observed, source)])
    out['push_age_ms'] = stats([(int(v['observed_ns'])-int(v['push_ms'])*1_000_000)/1e6 for v in values if v['push_ms']])
    out['push_minus_source_ms'] = stats([int(v['push_ms'])-int(v['source_ms']) for v in values if v['push_ms']])
    out['post_receive_to_callback_us'] = stats([(int(v['observed_ns'])-int(v['connector_receive_ns']))/1e3 for v in values if v['connector_receive_ns']])
    out['interarrival_ms'] = stats([(b-a)/1e6 for a, b in zip(monotonic, monotonic[1:])])
    out['source_timestamp_regressions'] = sum(b<a for a,b in zip(source,source[1:]))
    out['source_timestamp_repeats'] = sum(b==a for a,b in zip(source,source[1:]))
    out['epochs'] = sorted({int(v['epoch']) for v in values})
    out['bids_levels'] = sorted({int(v['bids']) for v in values})
    out['asks_levels'] = sorted({int(v['asks']) for v in values})
    assert out['bids_levels']==[5] and out['asks_levels']==[5]
    assert all(10_000_000_000<=v<=130_100_000_000 for v in monotonic)
    collected = next(v for v in summary['series'] if v['path']==path and v['symbol']==symbol)
    for metric in ['source_age_ms','push_age_ms','push_minus_source_ms','post_receive_to_callback_us','interarrival_ms']:
        if out[metric] is None:
            assert collected[metric] is None
        else:
            for key, value in out[metric].items():
                assert math.isclose(value, collected[metric][key], rel_tol=1e-9, abs_tol=0.0005), (path,symbol,metric,key,value,collected[metric][key])
    series.append(out)
report = dict(independent_csv_recomputation_passed=True, rows=len(rows),
              percentile_method='nearest-rank; no negative or tail samples removed', series=series)
(root / 'data/recomputed.json').write_text(json.dumps(report, indent=2)+'\n')
print(json.dumps(report, indent=2))
