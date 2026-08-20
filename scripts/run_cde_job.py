"""
Trigger a CDE job from your laptop and wait for it to finish on the cluster.

This does NOT run Spark locally — it shells out to the CDE CLI, which submits
the job to run on CDE exactly as `cde job run` would from a terminal. Safe to
use without touching how the job actually executes on the cluster.

Usage:
    python scripts/run_cde_job.py workshop-ingest-raw
"""

import subprocess
import sys

if len(sys.argv) < 2:
    print("Usage: python scripts/run_cde_job.py <job-name>", file=sys.stderr)
    sys.exit(1)

job_name = sys.argv[1]

print(f"Submitting {job_name} to CDE and waiting for it to finish...")
result = subprocess.run(["cde", "job", "run", "--name", job_name, "--wait", "--hide-progress-bars"])
sys.exit(result.returncode)
