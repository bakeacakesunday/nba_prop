"""
fetch_game_logs.py — Auto-downloads all pages from your Basketball Reference
Stathead search and saves them as game_logs.csv in the nba_props folder.

Usage:
    python3 fetch_game_logs.py

Put this file in your nba_props folder and run it from there.
It will overwrite game_logs.csv when done.
"""

import time
import random
from typing import Optional
import pandas as pd
import requests
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL = (
    "https://www.sports-reference.com/stathead/basketball/player-game-finder.cgi"
    "?request=1&player_game_max=9999&season_start=1&team_game_min=1"
    "&player_game_min=1&order_by=pts&comp_id=NBA&previous_days=60"
    "&team_game_max=84&match=player_game&season_end=-1&timeframe=last_n_days"
    "&comp_type=reg"
)

OUTPUT_FILE = Path("game_logs.csv")
PAGE_SIZE   = 200
MAX_PAGES   = 50      # safety cap — 50 × 200 = 10,000 rows max
DELAY_MIN   = 8.0     # seconds between requests (well under 10 req/min limit)
DELAY_MAX   = 12.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.sports-reference.com/",
    "Cookie": "is_live=true; osano_consentmanager_uuid=37541983-0992-4b40-a752-ba0c4814b60f; hubspotutk=9a0088e5051d3d774b14ec81ed6db7db; __hssrc=1; _ga=GA1.1.69922928.1771862085; refresh_token=eyJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.uIiaY3U4cA5h_Gw3YcEj-5tHJmHGEOCIc-zlnJ8kfHM6bbkhQP-ZOKzBTBJdKrJC_rDpPFkG09eugjuAGu5X_NH3Ib2GCQZ2DZVeu90NLuATzy_J8TdGt5bWS-zGVJD8dzMKyeBA8NTWC8NFGBfT9fHypIGTyh9KkVgcpcP2hv22NKonIg0zOliK8fCpRDGt73iakDDQKHZ1ED5Gz8tTAf53IzQZ1Ht6AgyR9nf3absCpG8b2XoXl71khCq8E3RjUpU0z7vwukvetEITr3VRFgLQ8YuQNwVjnP1bVBTMJi0zyR7coU15RTNxIr0MPpNE_iryi1m6F5lAGGO-kIB9CQ.naEGaF4imj3KPcfY.iChC0eHk0HtZwgQPYTXaYA-rpCufz5LtgkuxnLrgp3DkIbETDOANFCVIN2yQkxlZBIIVFt1q-xQjdnToglvJWj_fC678aKZ2FuId3K-wAxsj9NyNKm8RM-yVeJkLzWqiIXZ3yY1Td1onFK-CIQeThMu9IdspkxCKFpkXaa8AS9lkwqHUU0xdi28a3t-IAl2XvaX05I_52Sq5VpVVvopGYkt20hDRY-I2wMpZu85KjTIw2A3WEiZHweNQRoaWBPAu7eiP-lzIdQkSnGYaZ-J-CAfumwuhA9mayNvDIouuMKUZD0dxo5f9gZLivgijPC67HYnQvfGLDKaPPGwf_bNvZybzpwE1IBxSiG_7KmO-6wGqUqxPjKAnz9D9W2K-msMEZKlPIhWR7TLPA_2EJbNwvIl8rWz5ErlxThCJrB6gS2TWnFhn6zxVdVrW8kyXuVfjS5L1P1shH2EvOz5RMl5bAuWY7OUa3PRS41qe8ubIpDbC9RwScKCNKiR7Qgy3WP9_OwTwyU3ZL7h4J9odeunCGdosGHrGUkORnrg8adzBNvo8CQmJO58m9A1bEbmBLU40QQCk5yn6fEIHNN5HJOWii40htGxWOkOuiRAhPnDkwJOkQamfeweyyeWs7LgPRCfs-MPHC09_DPEXvVIA515R4WgnZ1BpQB7q-N6VK4rb7i6LOm_aK0DTAed-2CsHiLIuCR703jI7wGgwA9iuiX18cVGhMkGSZ1M-p7wBZZWl_EBnD50ovE5d_R-LFMIXpc1Tx2lP0gCwX_pFl1c5eXOlcE7QJ-7oyEfAHC3M7dKsrA94kkAcVFhK_cLYPjybFcz_FtaJyL4BfdoJMYnB-HB1UJX7dwh9xO4pAwpEgtGK17-RoXVxoimVAnYtGE8E6StdGsZy0KszhE-P35pb100zC92-tUEy_CsuOKoluXeHhWFeneRH57v5P93zqGFp8upkvpH3EBRbjPha5ZeSs_jJRzht_CT0NgcGJUgtZqd_-d_PCkJBdMFeGPRUb6Tx_17ZFl51NE8nCNwJEPsr5pAXD0rPYoHButs_q1GIi_7vbBIGdHWHDr3zc9s-XE5DD5iYoPhAv0_weZ0eXsEEwiLCT60Xc47TruJTIGx5CpUoZgDXoG4p-Bz92Z7qn75m3JRpkwR02AKHu-IbSflu7rT165VnEWEx9AhL8YHbR1k3M79LkhI7YB2a0Y6Ddx9ccxKmeB7t0vKnJvwxK3b1OS03fUy4lXmdq3eiz12pWnSciw5PG3QWechfkM8f1rrj9AA4jfIxLjiaVi6FDOY.9UTUdnUl1mpAphNWu1hswg; username=google_104589838558254988532; csrf_token=2dc65166b419fc6135f91e10172d42c3; stathead_site=bbr; stathead_type=trial; __cf_bm=t8skXRp2dSCOg439MY_RrE29yK.M37F1cgfC9o6Bgzk-1771950062-1.0.1.1-Pq_OuE3YQEStnU0x6Jt1TmvLCnJQqfFsoLPBxuXn.6AOTL0ic0BNhiQtKBlth4cq_w29afs4QTkuABNaSXuJzjGtFssewIjSNHy1KjuHZjk; srcssfull=yes; access_token=eyJraWQiOiJCMjRoeldcLzU3N0Roa2N1RUpabW1DZkl5dWNiQkRHakR6VG5ydGtRdkZsYz0iLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIzNDA4ZTRjOC05MGUxLTcwYTUtY2NkMS1kNDRmZDVjZWJlNTciLCJjb2duaXRvOmdyb3VwcyI6WyJ1cy1lYXN0LTFfVjVHR0tKcFFDX0dvb2dsZSJdLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtZWFzdC0xLmFtYXpvbmF3cy5jb21cL3VzLWVhc3QtMV9WNUdHS0pwUUMiLCJ2ZXJzaW9uIjoyLCJjbGllbnRfaWQiOiI3YnVmZjlsMGM4N2ZiNXZqdTh2azltaTJoMCIsIm9yaWdpbl9qdGkiOiJhNzBiM2M3OS0xMDA5LTRiMGMtODllYS1kMTg0NDg0NGIwMTgiLCJ0b2tlbl91c2UiOiJhY2Nlc3MiLCJzY29wZSI6ImF3cy5jb2duaXRvLnNpZ25pbi51c2VyLmFkbWluIG9wZW5pZCBwcm9maWxlIGVtYWlsIiwiYXV0aF90aW1lIjoxNzcxODYzNTc4LCJleHAiOjE3NzE5NTM2OTQsImlhdCI6MTc3MTk1MDA5NCwianRpIjoiNzk5ZmU0MzgtOWY2Zi00ZjI1LWEwN2MtNzYxM2FlNmRmYjBiIiwidXNlcm5hbWUiOiJnb29nbGVfMTA0NTg5ODM4NTU4MjU0OTg4NTMyIn0.sP0JqItNMnx3_YK3xvLvRJdgRKhcRipCU2p1-6GbgACYQhyMvKSdV5DJsNAUOIYBhtg6vAWtEDe6FQfOfP3pIBrMCAyS5-9VLXWNXjNmlAMGY7dw3iOJV2HCp5ExXTibrt3lHGoSdreLl7ePGQNlIIHdjDXiUpb75AF0vA7_VG5F09ozcOccdGhjg0IKkAMEFZuBogpr6cY7siRm1guKWALFsx2tbnRifChtV2Quvzmz_PkGq8C_3fGl6_8pFHwVdwfzIIrKUArNrabPG5RLaiF9uTJkIFwaPbnMCT6grDpzNfZUgs_j4XTIulFVkf7jpdgrvxEdD5UdC4tXt0ox_g; id_token=eyJraWQiOiI4S3lRbUYydG02RGtOQUxZcUhCdjZpQWpVVnh2cHlPU0k2UWNsd0JUK2U4PSIsImFsZyI6IlJTMjU2In0.eyJhdF9oYXNoIjoibGVuMlhNem5haHozYWZfaEpaVFZDZyIsInN1YiI6IjM0MDhlNGM4LTkwZTEtNzBhNS1jY2QxLWQ0NGZkNWNlYmU1NyIsImNvZ25pdG86Z3JvdXBzIjpbInVzLWVhc3QtMV9WNUdHS0pwUUNfR29vZ2xlIl0sImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtZWFzdC0xLmFtYXpvbmF3cy5jb21cL3VzLWVhc3QtMV9WNUdHS0pwUUMiLCJjdXN0b206dXNlcl9pZCI6IjM0MDhlNGM4LTkwZTEtNzBhNS1jY2QxLWQ0NGZkNWNlYmU1NyIsImNvZ25pdG86dXNlcm5hbWUiOiJnb29nbGVfMTA0NTg5ODM4NTU4MjU0OTg4NTMyIiwiZ2l2ZW5fbmFtZSI6IkpvZSIsImN1c3RvbTpjcmVhdGVkX29uIjoiMjAyNi0wMi0yMyIsImN1c3RvbTpzdGF0aGVhZF9zaXRlcyI6ImJiciIsIm9yaWdpbl9qdGkiOiJhNzBiM2M3OS0xMDA5LTRiMGMtODllYS1kMTg0NDg0NGIwMTgiLCJhdWQiOiI3YnVmZjlsMGM4N2ZiNXZqdTh2azltaTJoMCIsImlkZW50aXRpZXMiOlt7ImRhdGVDcmVhdGVkIjoiMTc3MTg2MzU3NzUyMSIsInVzZXJJZCI6IjEwNDU4OTgzODU1ODI1NDk4ODUzMiIsInByb3ZpZGVyTmFtZSI6Ikdvb2dsZSIsInByb3ZpZGVyVHlwZSI6Ikdvb2dsZSIsImlzc3VlciI6bnVsbCwicHJpbWFyeSI6InRydWUifV0sInRva2VuX3VzZSI6ImlkIiwiYXV0aF90aW1lIjoxNzcxODYzNTc4LCJleHAiOjE3NzE5NTM2OTQsImlhdCI6MTc3MTk1MDA5NCwiZmFtaWx5X25hbWUiOiJEaWxsYXJkIiwiY3VzdG9tOnN0YXRoZWFkX3N1Yl9zdGF0dXMiOiJpbl90cmlhbCIsImp0aSI6ImU4OTc5N2UzLTIzMmEtNDAyYy1hYzlkLTY0Zjk0ZDdhMWNkMCIsImVtYWlsIjoiam9zZXBob2RpbGxhcmRAZ21haWwuY29tIn0.el-z-9fDGPFG6zaOciNb4AFqefOWDSXgBfWb2CaZR-9pI8o0__1fxyjE9LJ9eDDz92L8CLTxkz6krbRQd3afOzL-OonBJEwhoMC3rf6NP8_5hABGNvIVF-jdltZ0qBq2w5EzCzq7dFf2u5dvieZAlCv-aeKbhMaFUh2C94XkuQFnSdM5ue2cbr3mZqFz0nRz92zSDmBqmQp9JUDICOXNI-rnhHK4-Ek8mSHedCcUB8dZXYAW2Jc01gdNcwzVftlNxoZFIhRiu5e9LBnZBg7K3Tv1l-mJCSyH98IyVThowGE_WE0MNFixCKVzX_w6hRiGEZQfozooKDYczhAZQce0Jg; stathead_user=Joe%3A%3A3408e4c8-90e1-70a5-ccd1-d44fd5cebe57%3A%3A8475f3a4c27f9f58aa3d5d3a03b6d7c4%3A%3Acharge%3A%3Anew; cf_clearance=1TQIWi6FF65kCRdACXchFssHqaaetsPI6E9R3bYBTF4-1771950665-1.2.1.1-J0uJjMH4xYwahmIVJfTXSTlMSl.7wYFqgNmb7rTC80RgVtg3jREY7kAh.GcGl0PRsy_OVWC94s6mPzdNCQ1o.Z8zPcxla3tnUp0h0sJJAiGkJQqv9ezIyTw_z74A5Sqy4omP3FgVoGexePAezS9ZzZcJh3LJhjK0oWJkVpVpOAqB4fGfVeQ1Sq.p38vKPhb7lPXk63YHqkkDnnInSvHm4u9zU7bb1TwGaxJhW.eBQwk",
}

# ── Fetcher ───────────────────────────────────────────────────────────────────

def fetch_page(offset: int) -> Optional[pd.DataFrame]:
    url = f"{BASE_URL}&offset={offset}"
    print(f"  Fetching offset={offset}  →  {url[:80]}...")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  ❌ Request failed: {e}")
        return None

    try:
        from io import StringIO
        tables = pd.read_html(StringIO(resp.text))
    except ValueError:
        print("  ⚠️  No table found on this page — probably reached the end.")
        return None

    if not tables:
        return None

    df = tables[0]

    # Drop repeated header rows (BR inserts them every 25 rows)
    if "Rk" in df.columns:
        df = df[df["Rk"] != "Rk"]
        df = df[df["Rk"].notna()]

    # Drop rows where Player is blank or NaN
    if "Player" in df.columns:
        df = df[df["Player"].notna()]
        df = df[df["Player"].str.strip() != ""]

    if df.empty:
        return None

    print(f"  ✓ Got {len(df)} rows")
    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("\n🏀 NBA Game Log Fetcher")
    print(f"   Output: {OUTPUT_FILE.resolve()}")
    print(f"   Window: last 60 days")
    print(f"   Delay between pages: {DELAY_MIN}–{DELAY_MAX}s (~5 req/min, well under their limit)\n")

    all_frames = []
    total_rows = 0

    for page_num in range(MAX_PAGES):
        offset = page_num * PAGE_SIZE
        df = fetch_page(offset)

        if df is None or df.empty:
            print(f"\n  Reached end of results after {page_num} page(s).")
            break

        all_frames.append(df)
        total_rows += len(df)
        print(f"  Running total: {total_rows} rows")

        # If we got fewer than PAGE_SIZE rows, we're on the last page
        if len(df) < PAGE_SIZE:
            print("\n  Last page reached (partial page).")
            break

        # Polite delay — important to avoid getting blocked
        delay = random.uniform(DELAY_MIN, DELAY_MAX)
        print(f"  Waiting {delay:.1f}s...")
        time.sleep(delay)

    if not all_frames:
        print("\n❌ No data fetched. Check your internet connection or try again later.")
        return

    combined = pd.concat(all_frames, ignore_index=True)

    # Deduplicate (same player + date shouldn't appear twice)
    before = len(combined)
    if "Player" in combined.columns and "Date" in combined.columns:
        combined = combined.drop_duplicates(subset=["Player", "Date"])
    after = len(combined)
    if before != after:
        print(f"\n  Dropped {before - after} duplicate rows.")

    combined.to_csv(OUTPUT_FILE, index=False)

    print(f"\n{'='*50}")
    print(f"✅ Done!  Saved {len(combined)} rows to {OUTPUT_FILE.name}")

    if "Date" in combined.columns:
        dates = pd.to_datetime(combined["Date"], errors="coerce").dropna()
        print(f"   Date range: {dates.min().date()} → {dates.max().date()}")

    if "Player" in combined.columns:
        print(f"   Unique players: {combined['Player'].nunique()}")

    print(f"\n   Now run your main tool:")
    print(f"   python3 main.py --teams X,Y,Z --sheet \"NBA Props\"")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    main()
