#!/usr/bin/env python3
"""
Loads ../.env then runs `dbt <args>`.

dbt does NOT read .env files on its own — only the Python pipeline scripts
do that (via config.py's load_dotenv()). The env_var() calls in
dbt_project.yml / profiles.yml (GCP_PROJECT_ID, BQ_SILVER_DATASET, etc.)
need those variables to already be in the process environment before dbt
starts. This wrapper does that, then hands off to the real `dbt` CLI so you
don't have to `export`/`$env:` everything by hand every session.

Usage (run from anywhere, or from inside dbt/ — path resolution handles both):
    python run_dbt.py parse
    python run_dbt.py run
    python run_dbt.py run --select silver_customers
    python run_dbt.py test
"""

import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

DBT_DIR = Path(__file__).resolve().parent
ENV_PATH = DBT_DIR.parent / ".env"

if not ENV_PATH.exists():
    print(f"[ERROR] {ENV_PATH} not found. Copy .env.example to .env in the "
          f"project root first.")
    sys.exit(1)

load_dotenv(ENV_PATH)

# GOOGLE_APPLICATION_CREDENTIALS in .env is typically a relative path
# (./service-account.json), relative to the project root — resolve it to an
# absolute path so it still works when dbt/subprocess run with a different
# cwd.
cred = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
if cred and not os.path.isabs(cred):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str((DBT_DIR.parent / cred).resolve())

os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))

missing = [v for v in ("GCP_PROJECT_ID", "GOOGLE_APPLICATION_CREDENTIALS") if not os.environ.get(v)]
if missing:
    print(f"[ERROR] Missing required env var(s) in .env: {', '.join(missing)}")
    sys.exit(1)

result = subprocess.run(["dbt"] + sys.argv[1:], cwd=DBT_DIR)
sys.exit(result.returncode)