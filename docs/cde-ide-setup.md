# CDE Spark Connect — Local IDE Setup Guide

Prerequisites for connecting IntelliJ (or any local IDE) to a Cloudera Data Engineering (CDE)
Spark Connect session.

---

## Requirements

| Requirement | Version |
|---|---|
| macOS | Monterey or later |
| Homebrew | latest |
| Python | 3.11 (via Homebrew) |
| CDE CLI | latest (downloaded from CDE Virtual Cluster UI) |
| IntelliJ IDEA | 2024.x or later with Python plugin |

---

## Step 1 — Install Homebrew Python 3.11

```bash
brew install python@3.11
```

Verify:

```bash
/opt/homebrew/bin/python3.11 --version
# Expected: Python 3.11.x
```

---

## Step 2 — Install the CDE CLI

1. Log in to the **CDP Console** → **Data Engineering** → select your Virtual Cluster
2. Click **Cluster Details** → download the **CDE CLI** binary for macOS
3. Move it to your PATH and make it executable:

```bash
mv ~/Downloads/cde /usr/local/bin/cde
chmod +x /usr/local/bin/cde
```

Verify:

```bash
cde --version
```

---

## Step 3 — Configure the CDE CLI

Create the config directory and files:

```bash
mkdir -p ~/.cde
```

Create `~/.cde/config.yaml` — replace the values with your cluster details:

```bash
cat > ~/.cde/config.yaml << 'EOF'
cdp-endpoint: https://console.us-west-1.cdp.cloudera.com
vcluster-endpoint: https://<YOUR-VCLUSTER-HOST>/dex/api/v1
credentials-file: /Users/<YOUR-USERNAME>/.cde/credentials
EOF
```

Create `~/.cde/credentials` — replace with your CDP access key:

```bash
cat > ~/.cde/credentials << 'EOF'
[default]
cdp_access_key_id=<YOUR-CDP-ACCESS-KEY-ID>
cdp_private_key=<YOUR-CDP-PRIVATE-KEY>
EOF
```

> **Where to get your CDP Access Key:**
> CDP Console → top-right user menu → **Profile** → **Access & Private Keys** → Generate

Verify CLI is working:

```bash
cde session list
```

You should see a table of sessions. If you get a YAML parse error, check that `config.yaml`
does **not** contain any `[default]` INI block — that belongs only in `credentials`.

---

## Step 4 — Create a Spark Connect Session

```bash
cde session create \
  --name <YOUR-SESSION-NAME> \
  --type spark-connect \
  --description "IDE session"
```

Wait ~30 seconds, then confirm it is `available`:

```bash
cde session list
```

> Sessions have a TTL (default 8h). Re-create if killed.

---

## Step 5 — Download the CDE Python Packages

These two packages are **not** on public PyPI. Download them from the CDE console:

1. Log in to **CDP Console** → **Data Engineering** → Virtual Cluster
2. Navigate to **Sessions** → **Spark Connect** tab → **Configuration** / **IDE Setup**
3. Download both tarballs to your machine:
   - `pyspark-3.5.4.tar.gz`
   - `cdeconnect.tar.gz`

---

## Step 6 — Create the Python Virtual Environment

```bash
/opt/homebrew/bin/python3.11 -m venv ~/venvs/cde-spark
source ~/venvs/cde-spark/bin/activate
```

Install the packages:

```bash
pip install /path/to/pyspark-3.5.4.tar.gz
pip install /path/to/cdeconnect.tar.gz
```

Verify:

```bash
pip show pyspark cdeconnect
# pyspark    3.5.4
# cdeconnect 0.1.0
```

---

## Step 7 — Test the Connection from Terminal

```bash
source ~/venvs/cde-spark/bin/activate
cd /path/to/your/project
python3 tests/test_cde_connect.py
```

Expected output:

```
Connecting to CDE session: <YOUR-SESSION-NAME>
Spark version  : 3.5.4.x.xx.xxx.x-xx
spark.range(10).count() = 10

SUCCESS: IDE -> CDE Spark Connect session is working.
```

If this fails, open the CDE console → **Sessions → your session → Connect** tab and compare it against your local setup:

![CDE Spark Connect session details](images/cde-spark-connect-session.png)

- **SPARK VERSION** must match the `pyspark-3.5.*.tar.gz` tarball installed in your venv (Step 5/6). If your venv has a different pyspark version — e.g. because `pip install -r requirements.txt` ran *after* the tarball and re-pinned it — you'll get `ModuleNotFoundError: No module named 'pyspark.sql.connect'`. Reinstall the tarball last to fix it.
- **STATUS** must read `Available`. `Killed` means the session's TTL (default 8h, see **EXPIRES ON**) expired — recreate it with the Step 4 command.
- The session name in the breadcrumb (top-left) must exactly match `SESSION_NAME` in `tests/test_cde_connect.py`.

---

## Step 8 — Configure IntelliJ

### 8a. Register the Python SDK

1. Open IntelliJ → `File → Project Structure` (`⌘;`)
2. Click **SDKs** (left panel) → `+` → **Add Python SDK**
3. Select **Existing environment**
4. Set the interpreter path to:
   ```
   /Users/<YOUR-USERNAME>/venvs/cde-spark/bin/python3
   ```
5. Click **OK**

### 8b. Set the Project SDK

1. Still in Project Structure → click **Project** (left panel)
2. Set **SDK** to the `cde-spark` entry you just added
3. Click **Apply → OK**

### 8c. Verify the Run Configuration

Right-click `tests/test_cde_connect.py` → **Run**.

If IntelliJ shows "Cannot find Python interpreter":

1. `Run → Edit Configurations`
2. Find the `test_cde_connect` configuration
3. Change **Python interpreter** to `cde-spark`
4. Apply and re-run

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `Could not parse config file` | INI block in `config.yaml` | Remove `[default]` section from `config.yaml` — keep it only in `credentials` |
| `No module named 'grpc'` | Missing gRPC packages | `pip install grpcio grpcio-status googleapis-common-protos` |
| `INVALID_CONNECT_URL: path must be empty` | Wrong Spark Connect URL format | Use `cdeconnect` package, not `SparkSession.builder.remote()` directly |
| `spark-connect sessions don't support interact` | Wrong CLI command | Use `cdeconnect` package (not `cde session interact`) |
| `Cannot find Python interpreter` in IntelliJ | Stale SDK registry after venv rebuild | Re-add the SDK in `File → Project Structure → SDKs` |
| Session state is `killed` | TTL expired (default 8h) | Re-create: `cde session create --name <name> --type spark-connect` |
| `ModuleNotFoundError: No module named 'pyspark.sql.connect'` | `pip install -r requirements.txt` ran after the CDE `pyspark-3.5.*.tar.gz` tarball and silently downgraded pyspark below 3.4 | Reinstall the CDE tarball last: `pip install /path/to/pyspark-3.5.*.tar.gz` |
