#!/usr/bin/env python3
"""Run the M1-M3 gates with ephemeral service secrets kept out of command arguments and logs."""
import argparse,json,os,subprocess
from pathlib import Path

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--services",type=Path,required=True);parser.add_argument("--output",type=Path,required=True);args=parser.parse_args()
    assert "192.168.3.88" in subprocess.check_output(["hostname","-I"]).decode().split()
    services=args.services.resolve();state=json.loads((services/"services.json").read_text());assert state["root"]==str(services)
    env=dict(os.environ,**json.loads((services/"client-env.json").read_text()),TITAN_M3_PROFILE=str(services/"profile.json"),TITAN_M3_SERVICES=str(services/"services.json"),TITAN_PERSISTENCE_STAGE="M3",CARGO_TARGET_DIR="/tmp/titan-deplugin-target",TITAN_SOURCE_BASELINE="5997364864861fdd5d8c649b93e54161018f703d",TITAN_PREVIOUS_RECOVERY_FIXTURE="/home/dominolu/dev/titan-observation-audit-20261027-638bae69/evidence/final-04/loopback",TITAN_BROWSER_EXECUTABLE="/home/dominolu/.cache/ms-playwright/chromium-1248/chrome-linux64/chrome",TITAN_HISTORY_DATA="/home/dominolu/dev/titan-grid-history-20261022-2aafc598/data/grid-history",TITAN_HISTORY_PARAMETERS="/home/dominolu/dev/titan-grid-history-20261022-2aafc598/verification-artifacts/grid-history/final/artifacts")
    raise SystemExit(subprocess.call(["bash","scripts/verify_data_persistence.sh",str(args.output)],env=env))
if __name__=="__main__":main()
