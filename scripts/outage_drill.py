"""Controlled outage of the dedicated teaching stack; restores service in finally."""
import json, subprocess, time
from pathlib import Path
import argparse
parser=argparse.ArgumentParser(description='Stop/restart only this lab database and verify health detection. Run when no DAG is active.')
parser.add_argument('--output', type=Path, required=True)
args=parser.parse_args()
args.output.mkdir(parents=True, exist_ok=False)
root=Path(__file__).resolve().parents[1]
base=['docker','compose','-f',str(root/'deployment/compose.yaml')]
check=base+['exec','-T','airflow','python','/opt/lab/healthcheck.py']
subprocess.run(check,check=True,capture_output=True)
record={'healthy_before':True}
try:
    subprocess.run(base+['stop','postgres'],check=True,capture_output=True)
    result=subprocess.run(check,capture_output=True,text=True)
    record['outage_detected']=result.returncode!=0
    if not record['outage_detected']: raise RuntimeError('Health probe failed to detect outage')
finally:
    subprocess.run(base+['start','postgres'],check=True,capture_output=True)
for _ in range(30):
    result=subprocess.run(check,capture_output=True)
    if result.returncode==0: break
    time.sleep(2)
record['healthy_after_restart']=result.returncode==0
(args.output/'outage.json').write_text(json.dumps(record,indent=2)+'\n')
print(json.dumps(record))
if not record['healthy_after_restart']: raise SystemExit(1)
