#!/usr/bin/env python3
"""
Get a fresh JWT token from OpenMetadata for use in ingestion configs.

Usage:
    python3 scripts/get_om_token.py
    python3 scripts/get_om_token.py | pbcopy   # copy to clipboard (Mac)
"""
import urllib.request, json, base64, sys

OM_URL = "http://localhost:8585/api/v1"
EMAIL = "admin@openmetadata.org"
PASSWORD = "admin"

b64 = base64.b64encode(PASSWORD.encode()).decode()
req = urllib.request.Request(
    f"{OM_URL}/users/login",
    data=json.dumps({"email": EMAIL, "password": b64}).encode(),
    headers={"Content-Type": "application/json"},
)
try:
    with urllib.request.urlopen(req) as r:
        token = json.loads(r.read())["accessToken"]
        print(token)
except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)
