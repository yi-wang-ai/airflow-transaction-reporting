"""Create local-only secrets without overwriting existing credentials."""
import base64
import os
from pathlib import Path
import secrets

root = Path(__file__).resolve().parents[1] / '.secrets'
root.mkdir(mode=0o700, exist_ok=True)
for name in ('postgres_password', 'payments_password', 'airflow_password', 'jwt_secret', 'fernet_key'):
    value = base64.urlsafe_b64encode(os.urandom(32)).decode() if name == 'fernet_key' else secrets.token_urlsafe(32)
    path = root / name
    try:
        with path.open('x') as stream:
            os.chmod(path, 0o444)
            stream.write(value + '\n')
    except FileExistsError:
        pass
print('Local secrets ready; existing credentials preserved. No values printed.')
