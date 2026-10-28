#!/usr/bin/env python3
"""GET-only P14 admission check. Credentials stay on the previously authorized live host."""
import argparse, hashlib, hmac, json, os, socket, sqlite3, subprocess, sys, time
from decimal import Decimal
from pathlib import Path
from urllib import parse, request

SYMBOL = "GATE_FUTURE_BTC_USDT"
POLICY = {"host":"43.130.234.54","hostname":"VM-0-33-ubuntu","account":"gate","account_id":1,"instrument":SYMBOL,"minimum_order_base":"0.0001","max_exposure_quote":"100","max_price_deviation_bps":50,"maximum_run_seconds":120,"stop_timeout_seconds":10,"handler_budget_seconds":1,"stop_action":"flatten"}

def admitted(rule, account, orders, positions, legacy_controllers, titan_processes, book):
    assert rule["symbol"] == SYMBOL and rule["business_type"] == "FUTURE" and rule["state"] == "live", "target is not a live future"
    for name, expected in (("tick_size","0.1"),("lot_size","0.0001"),("min_size","0.0001")):
        assert Decimal(rule[name]) == Decimal(expected), "instrument units changed"
    assert account["account_mode"] == "CROSS_EXCHANGE" and account["position_mode"] == "SINGLE", "account mode changed"
    assert not orders and not positions, "preexisting target exposure"
    assert not legacy_controllers and not titan_processes, "another trading owner exists"
    bid, ask = Decimal(book["bids"][0]["p"]), Decimal(book["asks"][0]["p"])
    assert bid.is_finite() and ask.is_finite() and 0 < bid <= ask, "invalid BBO"
    mid=(bid+ask)/2
    assert Decimal(60000) < mid < Decimal(110000), "market outside frozen test grid"
    assert Decimal(300)/mid < Decimal("0.005"), "grid quote outside price protection"
    assert ask*Decimal("0.0001")*2 <= Decimal(100), "two minimum quotes exceed exposure cap"
    return {"tick_size":"0.1","lot_size":"0.0001","min_size":"0.0001","public_bid":str(bid),"public_ask":str(ask),"minimum_order_notional_quote":str(ask*Decimal("0.0001")),"account_mode":account["account_mode"],"position_mode":account["position_mode"]}

def account_access():
    assert sys.platform=="linux" and socket.gethostname()==POLICY["hostname"] and Path('/data/gate_arb').is_dir(), "authorized live host required"
    sys.path.insert(0,'/data/gate_arb');os.chdir('/data/gate_arb')
    from app.core.config import settings
    from app.services.account_credentials import AccountCredentialService
    from sqlalchemy.engine import make_url
    path=Path(make_url(settings.resolved_database_url).database).resolve()
    with sqlite3.connect('file:'+parse.quote(str(path))+'?mode=ro',uri=True) as db:
        db.row_factory=sqlite3.Row;row=db.execute('SELECT * FROM exchange_accounts WHERE id=1').fetchone()
        assert row and row['name']=='gate' and row['account_type']=='crossex' and row['exchange']=='GATE' and row['enabled'], 'account identity changed'
        legacy=db.execute("SELECT id FROM contract_grid_strategies WHERE account_id=1 AND symbol LIKE '%BTC%' AND status IN ('running','starting','stopping','paused')").fetchall()
    credentials=AccountCredentialService().decrypt(row['credentials_encrypted'])
    return credentials, legacy

def venue_reader(credentials):
    key, secret=credentials['api_key'], credentials['api_secret']
    def venue(path,query=(),authenticated=True):
        assert path in ('/crossex/rule/symbols','/crossex/accounts','/crossex/positions','/crossex/open_orders','/futures/usdt/order_book')
        encoded=parse.urlencode(query);headers={'Accept':'application/json'}
        if authenticated:
            stamp=str(int(time.time()));payload='\n'.join(('GET','/api/v4'+path,encoded,hashlib.sha512(b'').hexdigest(),stamp));headers.update(KEY=key,Timestamp=stamp,SIGN=hmac.new(secret.encode(),payload.encode(),hashlib.sha512).hexdigest())
        with request.urlopen(request.Request('https://api.gateio.ws/api/v4'+path+('?' + encoded if encoded else ''),headers=headers),timeout=12) as response:
            body=response.read(8*1024*1024+1);assert len(body)<=8*1024*1024, 'venue response limit';return json.loads(body)
    return venue

def main():
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);args=p.parse_args();output=args.output.resolve();assert not output.exists()
    credentials, legacy=account_access();venue=venue_reader(credentials)
    rule=next(v for v in venue('/crossex/rule/symbols',authenticated=False) if v['symbol']==SYMBOL)
    account=venue('/crossex/accounts');orders=[v for v in venue('/crossex/open_orders',(('symbol',SYMBOL),)) if v.get('symbol')==SYMBOL];positions=[v for v in venue('/crossex/positions') if v.get('symbol')==SYMBOL and Decimal(v['position_qty'])!=0]
    processes=[line.split()[0] for line in subprocess.check_output(['ps','-eo','pid,comm']).decode().splitlines() if line.split()[-1]=='titan']
    book=venue('/futures/usdt/order_book',(('contract','BTC_USDT'),('limit',1)),False)
    evidence={**POLICY,**admitted(rule,account,orders,positions,legacy,processes,book),'passed':True,'checked_at_unix':int(time.time()),'venue_methods':['GET'],'preexisting_target_orders':len(orders),'preexisting_target_positions':len(positions),'legacy_target_controllers':len(legacy),'titan_processes':processes,'account_backend_access':'SQLite mode=ro, no service registry initialization','credentials_exported':False}
    output.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    with os.fdopen(os.open(output,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'w') as f:json.dump(evidence,f,indent=2)
    print(json.dumps(evidence),flush=True)
if __name__=='__main__':main()
