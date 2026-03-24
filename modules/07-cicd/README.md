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

**Continuous Deployment (CD):** Every time code is merged to `main`, the latest version is automatically deployed to CDE — no manual `cde job run` commands needed.

Together they mean: *you push code, tests run, and if everything passes, the pipeline deploys itself.*

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
- name: Install CDE CLI
  run: curl -L "..." -o /usr/local/bin/cde && chmod +x /usr/local/bin/cde
```
> Downloads the CDE CLI binary into the GitHub Actions runner.

```yaml
- name: Configure CDE CLI
  run: cde configure --cdp-endpoint "${{ secrets.CDP_ENDPOINT }}" ...
```
> Authenticates the CLI using secrets stored in GitHub (never in code).

```yaml
- name: Deploy Spark jobs
  run: ./scripts/deploy_jobs.sh

- name: Deploy Airflow DAG
  run: ./scripts/deploy_dag.sh
```
> Runs the deployment scripts. These are idempotent — they create the job if it does not
> exist, or update it if it does. Safe to run on every merge.

---

## GitHub Secrets Required

Configure these in **your repo → Settings → Secrets and variables → Actions → New repository secret**:

| Secret name | What it is |
|-------------|-----------|
| `CDP_ENDPOINT` | Cloudera CDP control plane URL |
| `CDP_ACCESS_KEY` | Your CDP access key ID |
| `CDP_PRIVATE_KEY` | Your CDP private key |
| `CDE_VC_ENDPOINT` | Your CDE virtual cluster endpoint URL |

> Your instructor will provide these values for the workshop environment.

---

## Steps

1. Fork this repo to your GitHub account

2. Open a Pull Request — go to the **Actions** tab and watch the CI workflow run

3. **Break CI intentionally:**
   - In a branch, add a line like `x=1` (unused variable) to any file in `jobs/`
   - Push and open a PR — watch pylint fail
   - Fix the issue and push again — watch CI pass

4. Configure the four GitHub Secrets listed above

5. Merge your branch to `main` — watch the CD workflow in the Actions tab

6. Verify the deployment in CDE:
   ```bash
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
