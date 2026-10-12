"""Fail container health if metadata or scheduler heartbeat is unhealthy."""
import json
import urllib.request

with urllib.request.urlopen('http://localhost:8080/api/v2/monitor/health', timeout=5) as response:
    status = json.load(response)
for component in ('metadatabase', 'scheduler'):
    if status.get(component, {}).get('status') != 'healthy':
        raise SystemExit(f'{component} unhealthy')
