# Europe 100% Continental Cloud Extraction Swarm
**20 Parallel GitHub Actions Runners (700 Concurrent HTTP/2 TLS Workers) • \$0 Compute Cost • Direct Google Drive Sync (100% Bitwise MD5 Parity)**

---

## Architecture Overview
- **Zero Local Disk / Zero Local CPU**: All extraction runs in GitHub Actions ephemeral Ubuntu runners (`20 parallel shards` $\times$ `35 curl_cffi threads`).
- **Storage-Optimized Deliverables**: Merges all shards directly into `europe_master.sqlite` + `extraction_audit_summary.json` on Google Drive (`Europe_Property_Database_Exports/latest/`), eliminating redundant CSV/JSONL storage overhead.
- **Automated 1,000-Listing Gap Loop**: Runs every 6 hours (`cron: '0 */6 * * *'`) and supports one-click manual triggers via `workflow_dispatch`.

---

## 3-Step Cloud Activation Guide

### Step 1: Generate Your `rclone` Google Drive Config (Once, 60 Seconds)
On your Mac terminal:
```bash
brew install rclone
rclone config
```
Follow the prompts:
1. Type `n` (New remote) $\to$ Name it **`gdrive`** (exact name required by the workflow).
2. Choose **`drive`** (Google Drive).
3. Leave `client_id` and `client_secret` blank, choose scope `1` (Full access), complete browser OAuth with `almazrooei@gmail.com`.
4. Print your config to copy it:
```bash
cat ~/.config/rclone/rclone.conf
```

### Step 2: Create a Private GitHub Repository & Add Secret
1. Go to [github.com/new](https://github.com/new) and create a **Private** repository (e.g., `europe-cloud-swarm`).
2. Inside the repo, navigate to **Settings $\to$ Secrets and variables $\to$ Actions $\to$ New repository secret**:
   - **Name**: `RCLONE_CONFIG`
   - **Value**: Paste the full contents of `~/.config/rclone/rclone.conf` from Step 1.

### Step 3: Push This Repository & Launch the 20-Runner Swarm
```bash
cd /Users/amirahamed/Downloads/Bayut/europe_cloud_swarm
git remote add origin https://github.com/<YOUR_USERNAME>/europe-cloud-swarm.git
git push -u origin main
```
Then open the **Actions** tab in GitHub $\to$ select **Europe 100% Continental Extraction Swarm** $\to$ click **Run workflow** (leave `max_tasks = 0` for a full continental sweep).
