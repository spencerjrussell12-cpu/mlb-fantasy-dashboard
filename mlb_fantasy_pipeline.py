"""
MLB Stats + Yahoo Fantasy Baseball Data Pipeline
================================================
Pulls real MLB performance data + your Yahoo Fantasy league data
and exports clean CSVs (local) + Google Sheets (cloud-ready).

Requirements:
    pip install pybaseball yfpy pandas requests python-dotenv gspread google-auth

Setup:
    1. Create a .env file in the same directory (see .env.example)
    2. Register a Yahoo app at: https://developer.yahoo.com/apps/
    3. Place google_credentials.json in the same directory
    4. Run this script — it will walk you through Yahoo OAuth on first run
"""

import os
import json
import time
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

# ── pybaseball ────────────────────────────────────────────────────────────────
from pybaseball import (
    batting_stats,
    pitching_stats,
    batting_stats_range,
    pitching_stats_range,
    schedule_and_record,
    playerid_lookup,
    statcast,
)
from pybaseball import cache
cache.enable()

# ── Yahoo Fantasy ─────────────────────────────────────────────────────────────
from yfpy.query import YahooFantasySportsQuery
from pathlib import Path

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

TODAY           = date.today()
CURRENT_YEAR    = TODAY.year
DATE_FROM       = date(2025, 3, 27)
DATE_TO         = TODAY

OUTPUT_DIR      = "/Users/spencerrussell/OneDrive - G&G Outfitters/power_bi_data"
YAHOO_GAME_ID   = "469"
YAHOO_LEAGUE_ID = os.getenv("YAHOO_LEAGUE_ID", "YOUR_LEAGUE_ID_HERE")

# ── Google Sheets config ──────────────────────────────────────────────────────
SHEET_ID        = "1RVPs1V-2T6-XmZEi4AMnbWfo3RyI5ZYKxA0pkbsn5aA"
CREDS_FILE      = os.path.join(os.path.dirname(__file__), "google_credentials.json")
SCOPES          = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

YAHOO_STAT_MAP = {
    7:  "Runs",
    12: "HR",
    13: "RBI",
    16: "SB",
    28: "Wins",
    32: "Saves",
    42: "K",
    26: "ERA",
    27: "WHIP",
    50: "IP",
    4:  "AVG",
    60: "NSB",
}

os.makedirs(OUTPUT_DIR, exist_ok=True)

print(f"\n{'='*60}")
print(f"  MLB + Fantasy Pipeline  |  {TODAY}")
print(f"  Season: {CURRENT_YEAR}  |  Stats from: {DATE_FROM} → {DATE_TO}")
print(f"{'='*60}\n")


# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS HELPER
# ─────────────────────────────────────────────────────────────────────────────

def init_sheets():
    """Initialize Google Sheets client."""
    try:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SHEET_ID)
        print("   ✓ Google Sheets connected")
        return sheet
    except Exception as e:
        print(f"   ✗ Google Sheets connection failed: {e}")
        return None


def export_to_sheet(sheet, df: pd.DataFrame, tab_name: str):
    """
    Write a DataFrame to a named tab in Google Sheets.
    Creates the tab if it doesn't exist. Clears and rewrites each run.
    """
    if df.empty:
        print(f"   ⚠ Skipping Sheets tab '{tab_name}' — no data")
        return
    try:
        # Get or create the worksheet tab
        try:
            ws = sheet.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            ws = sheet.add_worksheet(title=tab_name, rows=5000, cols=50)

        # Clean the dataframe — convert all values to strings for Sheets compatibility
        df_clean = df.fillna("").astype(str)

        # Build data: header row + all data rows
        data = [df_clean.columns.tolist()] + df_clean.values.tolist()

        # Clear and rewrite
        ws.clear()
        ws.update(data, value_input_option="USER_ENTERED")
        print(f"   ☁️  Sheets → '{tab_name}'  ({len(df)} rows)")

    except Exception as e:
        print(f"   ✗ Sheets export failed for '{tab_name}': {e}")


# ─────────────────────────────────────────────────────────────────────────────
# PART 1 — MLB STATS VIA PYBASEBALL
# ─────────────────────────────────────────────────────────────────────────────

def pull_mlb_batting(year: int) -> pd.DataFrame:
    """Pull season-level batting stats from FanGraphs via pybaseball."""
    print(f"📊 Pulling MLB batting stats ({year})...")
    try:
        df = batting_stats(year, qual=1)
        cols = {
            "Name": "player_name", "Team": "team", "G": "games",
            "PA": "plate_appearances", "AB": "at_bats", "H": "hits",
            "HR": "home_runs", "RBI": "rbi", "R": "runs",
            "SB": "stolen_bases", "AVG": "avg", "OBP": "obp",
            "SLG": "slg", "OPS": "ops", "wRC+": "wrc_plus",
            "WAR": "war_bat", "K%": "k_pct", "BB%": "bb_pct",
            "BABIP": "babip", "Hard%": "hard_hit_pct", "xFIP": "xfip",
        }
        existing = {k: v for k, v in cols.items() if k in df.columns}
        df = df.rename(columns=existing)[list(existing.values())]
        df["season"] = year
        df["stat_type"] = "batting"
        print(f"   ✓ {len(df)} batters loaded")
        return df
    except Exception as e:
        print(f"   ✗ Batting stats failed: {e}")
        return pd.DataFrame()


def pull_mlb_pitching(year: int) -> pd.DataFrame:
    """Pull season-level pitching stats from FanGraphs via pybaseball."""
    print(f"⚾ Pulling MLB pitching stats ({year})...")
    try:
        df = pitching_stats(year, qual=1)
        cols = {
            "Name": "player_name", "Team": "team", "G": "games",
            "GS": "games_started", "IP": "innings_pitched", "W": "wins",
            "L": "losses", "SV": "saves", "SO": "strikeouts",
            "ERA": "era", "WHIP": "whip", "K/9": "k_per_9",
            "BB/9": "bb_per_9", "HR/9": "hr_per_9", "FIP": "fip",
            "xFIP": "xfip", "WAR": "war_pitch", "K%": "k_pct",
            "BB%": "bb_pct", "BABIP": "babip",
        }
        existing = {k: v for k, v in cols.items() if k in df.columns}
        df = df.rename(columns=existing)[list(existing.values())]
        df["season"] = year
        df["stat_type"] = "pitching"
        print(f"   ✓ {len(df)} pitchers loaded")
        return df
    except Exception as e:
        print(f"   ✗ Pitching stats failed: {e}")
        return pd.DataFrame()


def pull_recent_batting(start: date, end: date) -> pd.DataFrame:
    """Pull last-N-days batting splits."""
    print(f"🔥 Pulling recent batting ({start} → {end})...")
    try:
        df = batting_stats_range(str(start), str(end))
        df["date_from"] = str(start)
        df["date_to"]   = str(end)
        df.columns = [c.lower().replace(" ", "_").replace("%", "_pct").replace("/", "_per_")
                      for c in df.columns]
        print(f"   ✓ {len(df)} recent batter rows loaded")
        return df
    except Exception as e:
        print(f"   ✗ Recent batting failed: {e}")
        return pd.DataFrame()


def pull_recent_pitching(start: date, end: date) -> pd.DataFrame:
    """Pull last-N-days pitching splits."""
    print(f"🔥 Pulling recent pitching ({start} → {end})...")
    try:
        df = pitching_stats_range(str(start), str(end))
        df["date_from"] = str(start)
        df["date_to"]   = str(end)
        df.columns = [c.lower().replace(" ", "_").replace("%", "_pct").replace("/", "_per_")
                      for c in df.columns]
        print(f"   ✓ {len(df)} recent pitcher rows loaded")
        return df
    except Exception as e:
        print(f"   ✗ Recent pitching failed: {e}")
        return pd.DataFrame()


def pull_upcoming_schedule() -> pd.DataFrame:
    """Pull this week's MLB schedule via the official MLB Stats API."""
    print("📅 Pulling upcoming schedule...")
    try:
        url = "https://statsapi.mlb.com/api/v1/schedule"
        params = {
            "sportId": 1,
            "startDate": str(TODAY),
            "endDate": str(TODAY + timedelta(days=7)),
            "gameType": "R",
            "hydrate": "team,venue,probablePitcher",
        }
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()

        rows = []
        for day in data.get("dates", []):
            for game in day.get("games", []):
                away_pitcher = (game.get("teams", {}).get("away", {})
                                    .get("probablePitcher", {}).get("fullName", "TBD"))
                home_pitcher = (game.get("teams", {}).get("home", {})
                                    .get("probablePitcher", {}).get("fullName", "TBD"))
                rows.append({
                    "game_date":    day["date"],
                    "game_pk":      game["gamePk"],
                    "away_team":    game["teams"]["away"]["team"]["name"],
                    "home_team":    game["teams"]["home"]["team"]["name"],
                    "venue":        game.get("venue", {}).get("name", ""),
                    "away_pitcher": away_pitcher,
                    "home_pitcher": home_pitcher,
                    "status":       game["status"]["detailedState"],
                })

        df = pd.DataFrame(rows)
        print(f"   ✓ {len(df)} upcoming games loaded")
        return df
    except Exception as e:
        print(f"   ✗ Schedule pull failed: {e}")
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# PART 2 — YAHOO FANTASY DATA
# ─────────────────────────────────────────────────────────────────────────────

def init_yahoo():
    print("🏆 Connecting to Yahoo Fantasy...")
    try:
        env_path = Path(os.path.dirname(__file__))
        query = YahooFantasySportsQuery(
            league_id=YAHOO_LEAGUE_ID,
            game_code="mlb",
            game_id=469,
            yahoo_consumer_key=os.getenv("YAHOO_CONSUMER_KEY"),
            yahoo_consumer_secret=os.getenv("YAHOO_CONSUMER_SECRET"),
            env_var_fallback=True,
            env_file_location=env_path,
            save_token_data_to_env_file=True,
        )
        print("   ✓ Yahoo Fantasy connected")
        return query
    except Exception as e:
        print(f"   ✗ Yahoo connection failed: {e}")
        return None


def pull_my_roster(query: YahooFantasySportsQuery) -> pd.DataFrame:
    """Pull current fantasy roster for all teams."""
    print("📋 Pulling fantasy rosters...")
    try:
        roster = query.get_league_teams()
        rows = []
        for team in roster:
            team_data = query.get_team_roster_by_week(team.team_id)
            for player in team_data.players:
                rows.append({
                    "fantasy_team_id":   team.team_id,
                    "fantasy_team_name": team.name,
                    "player_name":       player.name.full,
                    "player_id":         player.player_id,
                    "position":          player.display_position,
                    "status":            getattr(player, "status", "Active"),
                    "ownership_pct":     getattr(player, "percent_owned", None),
                })

        df = pd.DataFrame(rows)

        # Yahoo returns traded players on both old and new team — dedup on player_id.
        # Keep the row with the HIGHER team_id (acquiring team).
        before = len(df)
        df["fantasy_team_id"] = pd.to_numeric(df["fantasy_team_id"], errors="coerce")
        df = (
            df.sort_values("fantasy_team_id", ascending=False)
              .drop_duplicates(subset="player_id", keep="first")
              .sort_values(["fantasy_team_id", "player_name"])
              .reset_index(drop=True)
        )
        dupes = before - len(df)
        if dupes:
            print(f"   ⚠️  Removed {dupes} duplicate player row(s) (traded players)")

        print(f"   ✓ {len(df)} roster players loaded")
        return df

    except Exception as e:
        print(f"   ✗ Roster pull failed: {e}")
        return pd.DataFrame()


def pull_my_roster_stats(query: YahooFantasySportsQuery) -> pd.DataFrame:
    """Pull current week stats for all players across all 12 fantasy teams."""
    print("📊 Pulling weekly stats for all fantasy teams...")
    try:
        teams = query.get_league_teams()
        rows = []
        for team in teams:
            team_name = str(team.name).strip("b'").strip("'")
            roster_stats = query.get_team_roster_player_stats_by_week(
                team.team_id, chosen_week="current"
            )
            for player in roster_stats:
                row = {
                    "player_name":       player.name.full,
                    "position":          player.display_position,
                    "fantasy_team_name": team_name,
                    "fantasy_team_id":   team.team_id,
                }
                if hasattr(player, "player_stats") and player.player_stats is not None:
                    for s in player.player_stats.stats:
                        stat_name = YAHOO_STAT_MAP.get(int(s.stat_id), f"stat_{s.stat_id}")
                        row[stat_name] = s.value
                rows.append(row)
        df = pd.DataFrame(rows)
        print(f"   ✓ Weekly stats pulled for {len(df)} players across {len(teams)} teams")
        return df
    except Exception as e:
        print(f"   ✗ Roster stats pull failed: {e}")
        return pd.DataFrame()


def pull_my_team_stats(query: YahooFantasySportsQuery) -> pd.DataFrame:
    """Pull accumulated fantasy stats for all teams."""
    print("📈 Pulling fantasy team stats...")
    try:
        teams = query.get_league_teams()
        rows = []
        for team in teams:
            row = {
                "fantasy_team_id":   team.team_id,
                "fantasy_team_name": team.name,
                "wins":              getattr(team, "wins", None),
                "losses":            getattr(team, "losses", None),
                "ties":              getattr(team, "ties", None),
                "points":            getattr(team, "points", None),
                "standing":          getattr(team, "team_standings", {}).get("rank", None)
                                     if hasattr(team, "team_standings") and isinstance(team.team_standings, dict)
                                     else None,
            }
            rows.append(row)
        df = pd.DataFrame(rows)
        print(f"   ✓ Fantasy team stats loaded for {len(df)} teams")
        return df
    except Exception as e:
        print(f"   ✗ Fantasy team stats failed: {e}")
        return pd.DataFrame()


def pull_waiver_wire(query: YahooFantasySportsQuery, top_n: int = 60) -> pd.DataFrame:
    """Pull top available free agents on the waiver wire."""
    print(f"📡 Pulling top {top_n} waiver wire players...")
    try:
        roster = query.get_league_teams()
        rostered_ids = set()
        for team in roster:
            team_roster = query.get_team_roster_by_week(team.team_id)
            for player in team_roster.players:
                rostered_ids.add(str(player.player_id))

        players = query.get_league_players(player_count_limit=200)
        rows = []
        for p in players:
            if str(p.player_id) in rostered_ids:
                continue
            try:
                team = str(p.editorial_team_abbr).replace("b'", "").replace("'", "").strip()
            except:
                team = ""
            try:
                pct = p.percent_owned
                ownership = float(pct.value) if hasattr(pct, "value") else None
            except:
                ownership = None

            rows.append({
                "player_name": p.name.full,
                "player_id":   p.player_id,
                "position":    p.display_position,
                "status":      getattr(p, "status", "Active"),
                "team":        team,
            })
            if len(rows) >= top_n:
                break

        df = pd.DataFrame(rows)
        print(f"   ✓ {len(df)} waiver wire players loaded")
        return df
    except Exception as e:
        print(f"   ✗ Waiver wire pull failed: {e}")
        return pd.DataFrame()


def pull_matchup(query: YahooFantasySportsQuery) -> pd.DataFrame:
    """Pull current week's matchup info."""
    print("⚔️  Pulling current matchup...")
    try:
        matchups = query.get_league_scoreboard_by_week(chosen_week="current")
        rows = []
        for m in matchups.matchups:
            t1 = m.teams[0]
            t2 = m.teams[1]
            rows.append({
                "team_1":       str(t1.name).strip("b'").strip("'"),
                "team_1_score": getattr(t1.team_points, "total", 0),
                "team_2":       str(t2.name).strip("b'").strip("'"),
                "team_2_score": getattr(t2.team_points, "total", 0),
                "week":         m.week,
            })
        df = pd.DataFrame(rows)
        print(f"   ✓ Matchup data loaded")
        return df
    except Exception as e:
        print(f"   ✗ Matchup pull failed: {e}")
        return pd.DataFrame()

def pull_matchup_category_stats(week: int = None) -> pd.DataFrame:
    """Pull real category stats per team per week directly from Yahoo API."""
    print("📊 Pulling category stats from Yahoo API...")
    try:
        from dotenv import load_dotenv
        load_dotenv()
        
        token    = os.getenv("YAHOO_ACCESS_TOKEN")
        week_num = week or TODAY.isocalendar()[1]  # fallback to current ISO week

        # First get current week from scoreboard
        url = f"https://fantasysports.yahooapis.com/fantasy/v2/league/469.l.{YAHOO_LEAGUE_ID}/scoreboard?format=json"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        data      = r.json()
        week_num  = int(data["fantasy_content"]["league"][1]["scoreboard"]["week"])

        # Now pull that week's stats
        url  = f"https://fantasysports.yahooapis.com/fantasy/v2/league/469.l.{YAHOO_LEAGUE_ID}/scoreboard;week={week_num}?format=json"
        r    = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()

        matchups = data["fantasy_content"]["league"][1]["scoreboard"]["0"]["matchups"]
        
        STAT_MAP = {
            "7":  "Runs",
            "12": "HR",
            "13": "RBI",
            "16": "SB",
            "4":  "OBP",
            "28": "Wins",
            "32": "Saves",
            "42": "K",
            "26": "ERA",
            "27": "WHIP",
            "50": "IP",
            "60": "NSB",
        }

        rows = []
        for i in range(6):
            key = str(i)
            if key not in matchups:
                break
            matchup = matchups[key]["matchup"]
            teams   = matchup["0"]["teams"]
            
            # get stat winners for this matchup
            stat_winners = {}
            for sw in matchup.get("stat_winners", []):
                s = sw["stat_winner"]
                sid = s["stat_id"]
                if "is_tied" in s:
                    stat_winners[sid] = "tied"
                else:
                    stat_winners[sid] = s.get("winner_team_key", "")

            for t_key in ["0", "1"]:
                team_data  = teams[t_key]["team"]
                team_info  = team_data[0]
                team_name  = next((x["name"] for x in team_info if "name" in x), "")
                team_key   = next((x["team_key"] for x in team_info if "team_key" in x), "")
                team_stats = team_data[1]["team_stats"]["stats"]
                
                row = {
                    "fantasy_team_name": team_name,
                    "team_key":          team_key,
                    "week":              week_num,
                    "team_points":       float(team_data[1]["team_points"]["total"]),
                }

                for stat in team_stats:
                    s    = stat["stat"]
                    sid  = s["stat_id"]
                    val  = s["value"]
                    name = STAT_MAP.get(sid)
                    if name:
                        # skip fraction values like "12/46" (NSB)
                        try:
                            row[name] = float(val)
                        except (ValueError, TypeError):
                            row[name] = None

                # add win/loss/tie per category
                for sid, winner in stat_winners.items():
                    cat = STAT_MAP.get(sid)
                    if cat:
                        if winner == "tied":
                            row[f"{cat}_result"] = "tied"
                        elif winner == team_key:
                            row[f"{cat}_result"] = "win"
                        else:
                            row[f"{cat}_result"] = "loss"

                rows.append(row)

        df = pd.DataFrame(rows)
        print(f"   ✓ Category stats loaded for {len(df)} teams (week {week_num})")
        return df

    except Exception as e:
        print(f"   ✗ Category stats pull failed: {e}")
        return pd.DataFrame()

def pull_injuries() -> pd.DataFrame:
    """Pull current IL and injury transactions from MLB Stats API."""
    print("🏥 Pulling injury/transaction data...")
    try:
        url = "https://statsapi.mlb.com/api/v1/transactions"
        params = {
            "startDate": str(date(2026, 1, 1)),
            "endDate":   str(TODAY),
            "sportId": 1,
        }
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()

        rows = []
        for t in data.get("transactions", []):
            type_code   = t.get("typeCode", "")
            description = t.get("description", "").lower()
            is_injury = (
                type_code == "SC" and (
                    "injured list" in description or
                    "il" in description or
                    "day il" in description
                )
            ) or type_code in ["DL", "IL"]
            if is_injury:
                rows.append({
                    "player_name":      t.get("person", {}).get("fullName", ""),
                    "team":             t.get("toTeam", {}).get("name", "") or t.get("fromTeam", {}).get("name", ""),
                    "transaction_type": t.get("typeDesc", ""),
                    "description":      t.get("description", ""),
                    "date":             t.get("date", ""),
                })

        df = pd.DataFrame(rows)
        print(f"   ✓ {len(df)} injury/transaction records loaded")
        return df
    except Exception as e:
        print(f"   ✗ Injury pull failed: {e}")
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# PART 3 — MERGE & ENRICH
# ─────────────────────────────────────────────────────────────────────────────

def merge_fantasy_with_mlb(
    roster_df: pd.DataFrame,
    waiver_df: pd.DataFrame,
    batting_df: pd.DataFrame,
    pitching_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Join fantasy player lists with real MLB stats."""
    print("🔗 Merging fantasy + MLB data...")

    # Use only most recent season per player for roster/waiver enrichment
    batting_df  = batting_df.sort_values("season", ascending=False).drop_duplicates(subset="player_name", keep="first")
    pitching_df = pitching_df.sort_values("season", ascending=False).drop_duplicates(subset="player_name", keep="first")

    bat_cols = ["player_name", "team", "avg", "obp", "slg", "ops",
                "wrc_plus", "war_bat", "k_pct", "bb_pct", "hard_hit_pct",
                "home_runs", "runs", "rbi", "stolen_bases"]
    pit_cols = ["player_name", "team", "era", "whip", "k_per_9",
                "bb_per_9", "fip", "xfip", "war_pitch", "k_pct",
                "bb_pct", "strikeouts", "saves", "wins"]

    bat_slim = batting_df[[c for c in bat_cols if c in batting_df.columns]].copy()
    pit_slim = pitching_df[[c for c in pit_cols if c in pitching_df.columns]].copy()

    # Position sets for routing the merge
    PITCHER_POSITIONS = {"SP", "RP", "P"}

    def is_pitcher(pos_str):
        positions = {p.strip() for p in str(pos_str).split(",")}
        return bool(positions & PITCHER_POSITIONS)

    def enrich(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        # Split into pitchers and position players by the position column
        pitcher_mask = df["position"].apply(is_pitcher)
        pos_players  = df[~pitcher_mask].copy()
        pitchers     = df[pitcher_mask].copy()

        # Position players: merge batting stats only
        if not pos_players.empty:
            pos_players = pos_players.merge(bat_slim, on="player_name", how="left")

        # Pitchers: merge pitching stats only
        if not pitchers.empty:
            pitchers = pitchers.merge(pit_slim, on="player_name", how="left", suffixes=("", "_pitch"))

        # Recombine and restore original row order
        merged = pd.concat([pos_players, pitchers], ignore_index=True)
        return merged

    enriched_roster = enrich(roster_df)
    enriched_waiver = enrich(waiver_df)

    print(f"   ✓ Merged — roster: {len(enriched_roster)} rows | waiver: {len(enriched_waiver)} rows")
    return enriched_roster, enriched_waiver
    
# ─────────────────────────────────────────────────────────────────────────────
# PART 4 — EXPORT (CSV + Google Sheets)
# ─────────────────────────────────────────────────────────────────────────────

def export_csv(df: pd.DataFrame, filename: str):
    """Save DataFrame to local CSV."""
    if df.empty:
        print(f"   ⚠ Skipping {filename} — no data")
        return
    path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(path, index=False)
    print(f"   💾 CSV → {path}  ({len(df)} rows)")


def export_all(datasets: dict, sheet=None):
    """
    Export all datasets to both CSV and Google Sheets.
    datasets: { "tab_name / filename_stem": dataframe }
    """
    # Map: tab name → csv filename
    file_map = {
        "mlb_batting_season":   "mlb_batting_season.csv",
        "mlb_pitching_season":  "mlb_pitching_season.csv",
        "mlb_batting_recent":   "mlb_batting_recent.csv",
        "mlb_pitching_recent":  "mlb_pitching_recent.csv",
        "mlb_schedule":         "mlb_schedule.csv",
        "mlb_injuries":         "mlb_injuries.csv",
        "fantasy_roster":       "fantasy_my_roster.csv",
        "fantasy_roster_stats": "fantasy_roster_stats.csv",
        "fantasy_team_stats":   "fantasy_team_stats.csv",
        "fantasy_waiver_wire":  "fantasy_waiver_wire.csv",
        "fantasy_matchup":      "fantasy_matchup.csv",
        "fantasy_matchup_cats": "fantasy_matchup_cats.csv",

    }

    print("\n💾 Exporting to CSV + Google Sheets...")
    for tab_name, df in datasets.items():
        csv_file = file_map.get(tab_name, f"{tab_name}.csv")
        export_csv(df, csv_file)
        if sheet:
            export_to_sheet(sheet, df, tab_name)

    # Write a metadata tab so the dashboard knows when data was last refreshed
    if sheet:
        try:
            meta_tab = "metadata"
            try:
                ws = sheet.worksheet(meta_tab)
            except gspread.exceptions.WorksheetNotFound:
                ws = sheet.add_worksheet(title=meta_tab, rows=10, cols=5)
            ws.clear()
            ws.update([
                ["key", "value"],
                ["last_updated", str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))],
                ["season", str(CURRENT_YEAR)],
                ["date_from", str(DATE_FROM)],
                ["date_to", str(DATE_TO)],
            ])
            print("   ☁️  Sheets → 'metadata' tab updated")
        except Exception as e:
            print(f"   ✗ Metadata tab failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    # ── Google Sheets ──────────────────────────────────────────────────────────
    print("☁️  Connecting to Google Sheets...")
    sheet = init_sheets()

    # ── MLB Stats ──────────────────────────────────────────────────────────────
    batting_season_2023  = pull_mlb_batting(2023)
    batting_season_2024  = pull_mlb_batting(2024)
    batting_season_2025  = pull_mlb_batting(2025)
    batting_season_2026  = pull_mlb_batting(2026)
    pitching_season_2023 = pull_mlb_pitching(2023)
    pitching_season_2024 = pull_mlb_pitching(2024)
    pitching_season_2025 = pull_mlb_pitching(2025)
    pitching_season_2026 = pull_mlb_pitching(2026)
    batting_recent       = pull_recent_batting(DATE_FROM, DATE_TO)
    pitching_recent      = pull_recent_pitching(DATE_FROM, DATE_TO)
    injuries             = pull_injuries()
    batting_season  = pd.concat([batting_season_2023, batting_season_2024, batting_season_2025, batting_season_2026], ignore_index=True)
    pitching_season = pd.concat([pitching_season_2023, pitching_season_2024, pitching_season_2025, pitching_season_2026], ignore_index=True)
    schedule             = pull_upcoming_schedule()

    print()

    # ── Yahoo Fantasy ──────────────────────────────────────────────────────────
    yahoo        = init_yahoo()
    roster_df    = pd.DataFrame()
    team_stats   = pd.DataFrame()
    waiver_df    = pd.DataFrame()
    matchup_df   = pd.DataFrame()
    roster_stats = pd.DataFrame()
    matchup_cats = pull_matchup_category_stats()


    if yahoo:
        roster_df    = pull_my_roster(yahoo)
        team_stats   = pull_my_team_stats(yahoo)
        waiver_df    = pull_waiver_wire(yahoo, top_n=75)
        matchup_df   = pull_matchup(yahoo)
        roster_stats = pull_my_roster_stats(yahoo)

    print()

    # ── Merge ──────────────────────────────────────────────────────────────────
    enriched_roster, enriched_waiver = merge_fantasy_with_mlb(
        roster_df, waiver_df, batting_season, pitching_season
    )

    print()

    # ── Export everything ──────────────────────────────────────────────────────
    datasets = {
        "mlb_batting_season":   batting_season,
        "mlb_pitching_season":  pitching_season,
        "mlb_batting_recent":   batting_recent,
        "mlb_pitching_recent":  pitching_recent,
        "mlb_schedule":         schedule,
        "mlb_injuries":         injuries,
        "fantasy_roster":       enriched_roster,
        "fantasy_roster_stats": roster_stats,
        "fantasy_team_stats":   team_stats,
        "fantasy_waiver_wire":  enriched_waiver,
        "fantasy_matchup":      matchup_df,
        "fantasy_matchup_cats": matchup_cats,
    }

    export_all(datasets, sheet=sheet)

    print(f"\n✅ All done!")
    print(f"   Local CSVs → {OUTPUT_DIR}/")
    print(f"   Google Sheets → https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
