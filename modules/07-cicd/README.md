# Module 07 — CI/CD with GitHub Actions

**Estimated time:** 30 minutes
**Prerequisite:** All previous modules completed; repo forked to your GitHub account
**No exercise file** — this module is configuration-based

---

## Learning Objectives

By the end of this module you will be able to:

- Explain what CI and CD mean and why they matter
- Trace exactly what happens when you open a Pull Request (CI pipeline)
- Trace exactly what happens when you merge to `main` (CD pipeline)
- Configure the required GitHub Secrets for deployment to CDE
- Intentionally break a CI check and fix it

---

## What is CI/CD?

**Continuous Integration (CI):** Every time someone pushes a change, automated checks run to catch bugs and style issues before they reach the main branch.

**Continuous Deployment (CD):** In a typical setup, every merge to `main` would automatically deploy the latest version to CDE. In this workshop, deployment is a **manual step after merge** — see the note below on why full automation isn't possible with a GitHub-hosted runner.

Together they mean: *you push code, tests run automatically, and once they pass you run one command to deploy.*

> **Why isn't CD fully automatic here?** The CDE CLI has no public download — Cloudera only offers it as an authenticated, per-virtual-cluster binary from the CDP Console (Cluster Details → CLI TOOL). A GitHub-hosted runner can't fetch it, and since this repo is public, redistributing Cloudera's binary as a release asset would be a licensing risk. `cd.yml` runs on every merge to `main` and prints a reminder instead of silently failing. Two ways to get true automation if you need it: register a **self-hosted runner** on a machine that already has the CDE CLI installed, or host the binary in a **private** mirror repo your org controls and pull it into CI with a token.

---

## CI Pipeline — `.github/workflows/ci.yml`

**Triggers:** Any Pull Request targeting `main`

The CI pipeline has two jobs that run in order:

### Job 1: Lint & Unit Tests

```yaml
- name: Lint with pylint
  run: pylint jobs/ dags/ --fail-under=8.0 --disable=C0114,C0115,C0116,W0611
```
> **What it does:** Scores your code on a scale of 0–10. A score below 8.0 fails the build.
> `--disable=C0114,C0115,C0116` suppresses missing-docstring warnings.

```yaml
- name: Format check with black
  run: black --check jobs/ dags/ tests/
```
> **What it does:** Checks that code is formatted consistently. `--check` mode does not
> modify files — it just reports which files *would* be reformatted and fails if any exist.

```yaml
- name: Run unit tests
  run: pytest tests/unit/ -v --cov=jobs --cov-report=xml
```
> **What it does:** Runs all tests in `tests/unit/`. `--cov=jobs` measures which lines
> in `jobs/` are covered. The coverage report is uploaded as a build artifact.

### Job 2: GE Expectation Suite Validation (runs after Job 1 passes)

```yaml
- name: Validate expectation suite syntax
  run: python - <<'EOF'
       import json
       with open("great_expectations/expectations/retail_raw_suite.json") as f:
           suite = json.load(f)
       assert "expectation_suite_name" in suite
       assert len(suite["expectations"]) > 0
       EOF
```
> **What it does:** Confirms the JSON file is valid and non-empty before spending time running GE.

```yaml
- name: Run GE against sample data
  run: python - <<'EOF'
       import pandas as pd, great_expectations as gx ...
       EOF
```
> **What it does:** Runs the full `retail_raw_suite` against `data/sample/retail_orders.csv`
> using pandas (not Spark) — fast and free of CDE dependencies. If any expectation fails,
> CI fails and the PR cannot be merged.

---

## CD Pipeline — `.github/workflows/cd.yml`

**Triggers:** Any push to `main` (i.e., a merged PR)

```yaml
- name: Manual deploy required
  run: |
    cat <<'EOF'
    main branch updated — CDE deployment is a MANUAL step.
    Run: ./scripts/deploy_jobs.sh && ./scripts/deploy_dag.sh
    EOF
```
> Prints a reminder instead of deploying automatically — see the note above on why. The
> workflow deliberately exits 0 (success) so it doesn't clutter the Actions tab with a
> failure on every merge; it's a visibility step, not a deploy step.

**To actually deploy after merging**, run this yourself from a machine with the CDE CLI
installed and configured:

```bash
./scripts/deploy_jobs.sh
./scripts/deploy_dag.sh
# or: make deploy
```

These scripts are idempotent — they create each job if it doesn't exist, or delete and
recreate it if it does. Safe to run after every merge.

---

## GitHub Secrets Required

Configure these in **your repo → Settings → Secrets and variables → Actions → New repository secret**:

| Secret name | What it is |
|-------------|-----------|
| `CDP_ENDPOINT` | Cloudera CDP control plane URL |
| `CDP_ACCESS_KEY` | Your CDP access key ID |
| `CDP_PRIVATE_KEY` | Your CDP private key |
| `CDE_VC_ENDPOINT` | Your CDE virtual cluster endpoint URL |

> `cd.yml` does not use these by default (it only prints a reminder). They're only needed
> if you wire up a self-hosted runner or a private CLI mirror for true automated deploys —
> see the note above.

---

## Steps

1. Fork this repo to your GitHub account

2. Open a Pull Request — go to the **Actions** tab and watch the CI workflow run

3. **Break CI intentionally:**
   - In a branch, add a line like `x=1` (unused variable) to any file in `jobs/`
   - Push and open a PR — watch pylint fail
   - Fix the issue and push again — watch CI pass

4. Merge your branch to `main` — watch the CD workflow print the deploy reminder in the Actions tab

5. Deploy manually and verify:
   ```bash
   ./scripts/deploy_jobs.sh
   ./scripts/deploy_dag.sh
   cde job list | grep workshop
   ```
   All four jobs should show an updated timestamp.

---

## The Connection Between CI and the Pipeline

Notice the parallel between the CI pipeline and the data pipeline:

| Data pipeline | CI pipeline |
|---------------|-------------|
| `validate_data` catches bad data before transform | `pylint` + `pytest` catches bad code before deploy |
| Exit code 1 stops Airflow from running downstream tasks | A failed CI check blocks the PR from merging |
| GE expectations define the rules for data | pylint + black define the rules for code |

Both use the same principle: **fail fast, fail early, and block downstream steps from running on bad input.**

---

## You Have Completed the Workshop

You have built an end-to-end data engineering pipeline:

```
Raw CSV  →  Ingest  →  Validate  →  Transform  →  Load  →  Hive Tables
                           ↑                                      ↑
                    Great Expectations                      Airflow DAG
                                                                  ↑
                                                          GitHub Actions CI/CD
```

Well done.
