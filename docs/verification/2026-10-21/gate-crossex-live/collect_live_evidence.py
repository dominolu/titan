"""Run only on the live server; never copy the credential or unsanitized account logs."""
import json
import os
from pathlib import Path
import tomllib

root = Path('/home/ubuntu/titan-crossex-live-20261021')
config = tomllib.loads((root / 'credentials.toml').read_text())
secrets = [str(config[k]) for k in ('api_key', 'secret') if config.get(k)]
output = root / 'evidence'
output.mkdir(mode=0o700, exist_ok=True)
os.chmod(output, 0o700)
TARGET = 'GATE_FUTURE_BTC_USDT'

def redact(text):
    for secret in secrets:
        text = text.replace(secret, '[REDACTED]')
    return text

def clean(value):
    if isinstance(value, dict):
        return {k: clean(v) for k, v in value.items()
                if k.lower() not in {'user_id', 'uid', 'api_key', 'secret', 'key', 'sign', 'signature', 'authorization'}}
    if isinstance(value, list):
        return [clean(v) for v in value]
    return value

def sanitize(row):
    name = row.get('check')
    result = row.get('result')
    if name == 'account_raw' and isinstance(result, dict):
        row['result'] = {
            'account_mode': result.get('account_mode'),
            'position_mode': result.get('position_mode'),
            'assets': [{k: a.get(k) for k in ('coin', 'exchange_type')} for a in result.get('assets', [])],
            'financial_amounts': '[REDACTED]',
        }
    elif name == 'account_unified' and isinstance(result, dict):
        row['result'] = {'currencies': result.get('currencies'), 'financial_amounts': '[REDACTED]'}
    elif name in {'positions_raw', 'positions_unified'} and isinstance(result, list):
        row['result'] = {'rows': len(result), 'target_positions': [v for v in result if v.get('symbol') == TARGET], 'other_positions': '[REDACTED]'}
    elif name == 'open_orders' and isinstance(result, dict):
        row['result'] = {'rows': result.get('rows'), 'target_open_orders': result.get('by_symbol', {}).get(TARGET, 0), 'other_symbols': '[REDACTED]'}
    return clean(row)

for stem in ('readonly-v1', 'readonly-v2', 'readonly-final', 'readonly-final-v5', 'passive-attempt-v1', 'passive-final', 'postflight-final'):
    source = root / (stem + '.jsonl')
    if source.exists():
        rows = []
        for line in source.read_text().splitlines():
            try:
                row = sanitize(json.loads(line))
            except json.JSONDecodeError:
                row = {'unstructured': '[OMITTED]'}
            rows.append(redact(json.dumps(row, ensure_ascii=False)))
        (output / source.name).write_text('\n'.join(rows) + '\n')
    for suffix in ('.stderr', '.exit'):
        source = root / (stem + suffix)
        if source.exists():
            (output / source.name).write_text(redact(source.read_text()))
for pattern in ('intent-titanlive-*.json', 'completed-titanlive-*.json'):
    for source in root.glob(pattern):
        value = clean(json.loads(source.read_text()))
        (output / source.name).write_text(redact(json.dumps(value, ensure_ascii=False, indent=2)) + '\n')
for source in root.glob('host-identity-*.json'):
    (output / source.name).write_text(redact(json.dumps(clean(json.loads(source.read_text())), indent=2)) + '\n')
for path in output.iterdir():
    if path.is_file():
        os.chmod(path, 0o600)
        text = path.read_text()
        assert not any(secret in text for secret in secrets), 'secret survived redaction'
print(json.dumps({'sanitized_files': len(list(output.iterdir())), 'credential_copied': False}))
