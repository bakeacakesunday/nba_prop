# NBA Player Props Research Tool — Complete Setup Guide

## What This Tool Does

Pulls NBA player game logs via `nba_api`, computes stats (avg/median/min/max/std/hit-rate) over last-5/10/20 games, matches your prop lines, and writes everything to a Google Sheet — all from your terminal, no paid services required.

---

## PART 1: Project Structure

```
nba_props/
├── main.py           ← Run this
├── nba_data.py       ← nba_api calls + SQLite cache
├── metrics.py        ← Stat computations
├── sheets.py         ← Google Sheets helpers
├── utils.py          ← Name normalization + fuzzy match
├── requirements.txt  ← Python dependencies
└── nba_cache.db      ← Auto-created SQLite cache
```

---

## PART 2: Install Python

### Windows
1. Go to https://www.python.org/downloads/
2. Download Python **3.11** or newer.
3. Run the installer. **CHECK the box that says "Add Python to PATH"** before clicking Install.
4. Open **Command Prompt** (`Win + R` → type `cmd` → Enter).
5. Verify: type `python --version` and press Enter. You should see `Python 3.11.x`.

### Mac
1. Open **Terminal** (Applications → Utilities → Terminal).
2. Install Homebrew if you don't have it:
   ```
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
   ```
3. Install Python:
   ```
   brew install python@3.11
   ```
4. Verify: `python3 --version`

> **Note:** On Mac, use `python3` instead of `python` in all commands below.

---

## PART 3: Set Up the Project

### 1. Place the Files

Create a folder called `nba_props` wherever you like (e.g., Desktop or Documents) and copy all 5 `.py` files and `requirements.txt` into it.

### 2. Open a Terminal in That Folder

**Windows:** Hold Shift and right-click inside the `nba_props` folder → "Open PowerShell window here" (or Command Prompt).

**Mac:** Right-click the folder in Finder → "New Terminal at Folder". Or open Terminal and type:
```
cd ~/Desktop/nba_props
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

If that fails on Mac, use:
```bash
pip3 install -r requirements.txt
```

This installs: `nba_api`, `gspread`, `google-auth`, `rapidfuzz`, `pandas`, `numpy`.

---

## PART 4: Google Cloud Setup (One-Time, ~10 Minutes)

You need a **service account** — a robot Google identity that can write to Sheets without browser login.

### Step 1: Create a Google Cloud Project

1. Go to: https://console.cloud.google.com/
2. Sign in with any Google account.
3. Click the project dropdown at the top → **"New Project"**.
4. Name it `nba-props` (or anything). Click **Create**.
5. Make sure your new project is selected in the dropdown.

### Step 2: Enable the Required APIs

1. In the left sidebar, go to **APIs & Services → Library**.
2. Search for **"Google Sheets API"** → Click it → Click **Enable**.
3. Go back to Library. Search for **"Google Drive API"** → Click it → Click **Enable**.

### Step 3: Create a Service Account

1. In the left sidebar, go to **APIs & Services → Credentials**.
2. Click **"+ Create Credentials"** → **"Service Account"**.
3. Fill in:
   - Name: `nba-props-bot` (anything)
   - Service account ID: auto-filled, leave it
   - Description: optional
4. Click **Create and Continue**.
5. On the "Grant this service account access to project" step, select role: **"Editor"**. Click **Continue**.
6. Click **Done**.

### Step 4: Download the JSON Key

1. On the Credentials page, find your new service account under "Service Accounts".
2. Click the **pencil icon** (Edit) to the right of it.
3. Go to the **"Keys"** tab.
4. Click **"Add Key"** → **"Create New Key"**.
5. Choose **JSON** → Click **Create**.
6. A file like `nba-props-bot-abc123.json` downloads to your computer.
7. **Move this file** into your `nba_props` project folder and rename it to `service_account.json` for simplicity.

### Step 5: Set the Environment Variable

This tells the tool where to find your key.

**Windows (Command Prompt):**
```cmd
set GOOGLE_APPLICATION_CREDENTIALS=C:\path\to\nba_props\service_account.json
```

Replace `C:\path\to\nba_props\` with your actual path.

To make this permanent (so you don't re-run every session):
1. Search "Environment Variables" in Start Menu.
2. Click "Edit the system environment variables".
3. Click "Environment Variables".
4. Under "User variables", click "New".
5. Variable name: `GOOGLE_APPLICATION_CREDENTIALS`
6. Variable value: full path to your `service_account.json`
7. Click OK × 3.

**Mac/Linux (Terminal):**
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/Users/yourname/Desktop/nba_props/service_account.json"
```

To make this permanent, add that line to `~/.zshrc` (Mac) or `~/.bashrc` (Linux):
```bash
echo 'export GOOGLE_APPLICATION_CREDENTIALS="/Users/yourname/Desktop/nba_props/service_account.json"' >> ~/.zshrc
source ~/.zshrc
```

**Verify it's set:**
- Windows: `echo %GOOGLE_APPLICATION_CREDENTIALS%`
- Mac: `echo $GOOGLE_APPLICATION_CREDENTIALS`

You should see the path to your JSON file.

---

## PART 5: Google Sheet Setup

### Option A: Let the Tool Create the Sheet (Easiest)

Just run the tool — it will automatically create a new spreadsheet named whatever you pass to `--sheet`. You'll find it in your Google Drive.

**BUT:** You need to share the sheet with yourself so you can view it. After the first run, open Google Drive, find the spreadsheet, and it'll be there (the service account created it and owns it). To view it as yourself:
1. Open the spreadsheet.
2. Click **Share** in the top right.
3. Add your own Gmail address with Viewer or Editor access.

### Option B: Use an Existing Sheet

1. Create (or open) the sheet manually in Google Drive.
2. Find your service account email — it looks like `nba-props-bot@your-project-id.iam.gserviceaccount.com`. You can find it in the Google Cloud Console under IAM & Admin → Service Accounts.
3. Share the sheet with that service account email (give it **Editor** access).
4. Pass the exact sheet name with `--sheet "Your Sheet Name"`.

---

## PART 6: Running the Tool

### Basic Run (Today's Games)
```bash
python main.py --sheet "NBA Props"
```

### Specific Date
```bash
python main.py --date 2025-03-15 --sheet "NBA Props"
```

### Change Roster Lookback Window
```bash
python main.py --date 2025-03-15 --sheet "NBA Props" --roster-lookback 5
```

### Force Re-fetch (Ignore Cache)
```bash
python main.py --date 2025-03-15 --sheet "NBA Props" --refresh-cache
```

### Self-Test (Verify Everything Works)
```bash
python main.py --self-test
```
This pulls LeBron James's game log and prints computed metrics. Requires internet but does NOT write to Google Sheets.

---

## PART 7: Using the Lines Tab

After the first run, open your Google Sheet. Go to the **"Lines"** tab.

It has 4 columns:
| player_name | team | stat_type | line |
|---|---|---|---|
| LeBron James | LAL | PTS | 24.5 |
| Stephen Curry | GSW | FG3M | 3.5 |
| Nikola Jokic | DEN | PRA | 48.5 |

**Fill in your prop lines here, then re-run the script.** The Last5/10/20 tabs will gain new columns like `PTS_line`, `PTS_hit_rate` showing what % of last games that player went OVER that number.

**Supported stat_type values:**
- `PTS` — Points
- `REB` — Rebounds
- `AST` — Assists
- `FG3M` — 3-Pointers Made
- `STL` — Steals
- `BLK` — Blocks
- `TOV` — Turnovers
- `PRA` — Points + Rebounds + Assists
- `PR` — Points + Rebounds
- `PA` — Points + Assists
- `RA` — Rebounds + Assists

---

## PART 8: Understanding the Output Tabs

### Today Slate
| Date | Away Team | Home Team | Game Time |
|---|---|---|---|
| 2025-03-15 | BOS | MIA | 7:30 pm ET |

### Roster
All players on today's teams who logged minutes in the last N games.

### Last5 / Last10 / Last20
One row per player. Columns include:
- `player_name`, `team`, `games_count`, `minutes_avg`
- For each stat type: `{STAT}_avg`, `{STAT}_median`, `{STAT}_min`, `{STAT}_max`, `{STAT}_std`
- If you have lines entered: `{STAT}_line`, `{STAT}_hit_rate` (values 0–1, e.g., 0.800 = went over 8 of last 10 times)

### Notes
Any name-matching issues from the Lines tab. If your line entry for "Lebron James" (missing uppercase) couldn't be matched, it shows up here with suggestions.

---

## PART 9: Troubleshooting

### ❌ `GOOGLE_APPLICATION_CREDENTIALS environment variable is not set`

**Cause:** The env variable isn't set in this terminal session.

**Fix:**
- Windows: `set GOOGLE_APPLICATION_CREDENTIALS=C:\path\to\service_account.json`
- Mac: `export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service_account.json"`
- Make sure you're in the same terminal window where you set it (env vars don't carry between windows unless made permanent).

---

### ❌ `Service account key not found at: /path/to/file.json`

**Cause:** The path in your env variable is wrong, or the file was moved/renamed.

**Fix:** Verify the file exists at exactly that path. On Mac: `ls /path/to/service_account.json`. On Windows: `dir C:\path\to\service_account.json`.

---

### ❌ `gspread.exceptions.APIError: [403] The caller does not have permission`

**Cause:** The service account doesn't have Editor access to the spreadsheet.

**Fix:**
1. Open the spreadsheet in Google Drive.
2. Click Share.
3. Add the service account email (find it in `service_account.json` under the `"client_email"` field) with **Editor** access.

---

### ❌ `gspread.exceptions.SpreadsheetNotFound`

**Cause:** The `--sheet` name doesn't match any sheet the service account can access, and creation failed.

**Fix:** Either pass a slightly different name (it will create new), or check that Drive API is enabled in your Google Cloud project.

---

### ❌ `nba_api` Timeout / Connection Error

**Cause:** nba_api hits rate limits or NBA's servers are slow.

**Fix:** The tool automatically retries 3 times with backoff. If it keeps failing:
1. Wait a few minutes and retry.
2. Try `--refresh-cache` in case stale cached data is causing issues.
3. Check your internet connection.
4. Note that nba_api sometimes has outages during high-traffic times (game nights).

---

### ❌ `ModuleNotFoundError: No module named 'nba_api'`

**Cause:** Dependencies not installed in this Python environment.

**Fix:** Make sure you're in your project folder, then:
```bash
pip install -r requirements.txt
```
If you have multiple Python versions, make sure you're using the right `pip`:
```bash
python -m pip install -r requirements.txt
```

---

### ❌ No players showing up in Roster tab

**Cause:** Could be an off-day (no games), or `nba_api` returned empty rosters.

**Fix:**
1. Check that `--date` is a real game day. Try `--self-test` first to confirm nba_api is working.
2. Try increasing `--roster-lookback 7` to look back further.
3. Check the terminal output for warning messages about specific teams.

---

### ❌ Lines tab names not matching / showing up in Notes tab

**Cause:** Player name in Lines tab doesn't fuzzy-match any roster player above the 80% threshold.

**Fix:**
1. Check the **Notes** tab — it shows what it found and why it failed.
2. Make sure the player actually played recently (they need to be on today's slate).
3. Use the exact name format from the **Roster** tab (copy-paste to be safe).
4. Common issues: "LeBron" vs "LeBron James", "P.J. Tucker" vs "PJ Tucker" — the tool handles most of these automatically, but very short names or nicknames may fail.

---

### ❌ `ValueError: time data` or date parsing errors

**Cause:** nba_api occasionally returns dates in unexpected formats.

**Fix:** This is handled internally. If it persists, try `--refresh-cache` to re-fetch clean data.

---

### ❌ Script runs but Google Sheet is blank

**Cause:** Usually a permissions issue where the write succeeded but you're viewing as a different account.

**Fix:**
1. Make sure you shared the sheet with your own Google account (not just the service account).
2. Hard-refresh the browser (Ctrl+Shift+R or Cmd+Shift+R).
3. Check the terminal output for any error messages during the "Writing to Google Sheets" step.

---

## PART 10: Tips for Daily Use

1. **Schedule it:** Set up a task scheduler (Windows Task Scheduler or Mac `cron`) to run it each morning.

2. **Cache saves time:** After the first run for a day, reruns are fast because game logs are cached in `nba_cache.db`.

3. **Lines tab is persistent:** Your Lines tab data is never overwritten by the script — only read. Fill it in once and it stays.

4. **Multiple lines for same player+stat:** Add two rows for the same player+stat with different line values — the tool will show `PTS_line1`, `PTS_hit_rate1`, `PTS_line2`, `PTS_hit_rate2`.

5. **Hit rate interpretation:** A `PTS_hit_rate` of `0.800` means the player went OVER that line in 80% of games in that window (e.g., 8 of last 10). This is the "over" hit rate — useful for over bets. For under, subtract from 1.0.

6. **Off-day runs:** On days with no games, run with a game-day date using `--date` to analyze players for an upcoming game.
