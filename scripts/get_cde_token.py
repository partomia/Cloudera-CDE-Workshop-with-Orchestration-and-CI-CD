"""
Reads the cached CDE bearer token for the active vcluster.
Prints it to stdout so it can be used in shell: export CDE_TOKEN=$(python3 scripts/get_cde_token.py)
"""
import json
import sys
from pathlib import Path

TOKEN_CACHE = Path.home() / "Library/Caches/cloudera/cde/token-cache"
VCLUSTER_KEY_PREFIX = "https://t5c86ppm.cde-s9xvdpkr.go01-dem.ylcu-atmi.cloudera.site"

cache = json.loads(TOKEN_CACHE.read_text())
for key, value in cache.items():
    if key.startswith(VCLUSTER_KEY_PREFIX):
        print(value["access_token"])
        sys.exit(0)

print("ERROR: no token found in cache for this vcluster. Run a CDE CLI command first.", file=sys.stderr)
sys.exit(1)
