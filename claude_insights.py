"""
Claude Fantasy Analyst
======================
Reads your Power BI CSVs and asks Claude for:
  - Weekly starter recommendations
  - Waiver wire adds / drops
  - Trade analysis

Run this after mlb_fantasy_pipeline.py has generated the CSVs.
Output is saved to power_bi_data/claude_insights.json
(Power BI reads this via a JSON connector or you paste into a card visual)

Requirements:
    pip install anthropic pandas python-dotenv
"""

import os
import json
import anthropic
import pandas as pd
from datetime import date
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path

load_dotenv()

DATA_DIR    = "/Users/spencerrussell/OneDrive - G&G Outfitters/power_bi_data"
OUTPUT_FILE = os.path.join(DATA_DIR, "claude_insights.json")
TODAY       = str(date.today())

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def load_csv(name: str, max_rows: int = 30) -> str:
    """Load a CSV and return it as a compact string for the prompt."""
    path = os.path.join(DATA_DIR, name)
    if not os.path.exists(path):
        return f"[{name} not found — run mlb_fantasy_pipeline.py first]"
    df = pd.read_csv(path)
    
    # Filter to S&P only for roster and roster stats files
    if name in ("fantasy_my_roster.csv", "fantasy_roster_stats.csv"):
        if "fantasy_team_name" in df.columns:
            df = df[df["fantasy_team_name"].str.contains("S&P", na=False)]
    
    # Filter out rostered players from waiver wire
    if name == "fantasy_waiver_wire.csv":
        roster_path = os.path.join(DATA_DIR, "fantasy_my_roster.csv")
        if os.path.exists(roster_path):
            rostered = pd.read_csv(roster_path)["player_name"].str.strip().tolist()
            df = df[~df["player_name"].str.strip().isin(rostered)]
    
    return df.head(max_rows).to_string(index=False)

def ask_claude(system: str, user: str, max_tokens: int = 1500) -> str:
    """Single call to Claude Sonnet."""
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    return message.content[0].text

def export_insights_to_sheets(insights: dict):
    """Write claude insights to Google Sheets."""
    print("☁️  Exporting insights to Google Sheets...")
    try:
        SHEET_ID   = "1RVPs1V-2T6-XmZEi4AMnbWfo3RyI5ZYKxA0pkbsn5aA"
        CREDS_FILE = "/Users/spencerrussell/mlb_fantasy_dashboard/google_credentials.json"
        SCOPES     = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]

        creds  = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)
        sheet  = client.open_by_key(SHEET_ID)

        try:
            ws = sheet.worksheet("claude_insights")
        except gspread.exceptions.WorksheetNotFound:
            ws = sheet.add_worksheet(title="claude_insights", rows=50, cols=2)

        ws.clear()
        ws.update([
            ["key", "value"],
            ["generated_at",   insights.get("generated_at", "")],
            ["weekly_summary", insights.get("weekly_summary", "")],
            ["starters",       insights.get("starters", "")],
            ["waiver_wire",    insights.get("waiver_wire", "")],
            ["trade_give",     insights.get("trade_analysis", {}).get("give", "")],
            ["trade_receive",  insights.get("trade_analysis", {}).get("receive", "")],
            ["trade_analysis", insights.get("trade_analysis", {}).get("analysis", "")],
        ])
        print("   ✓ Insights exported to Google Sheets → 'claude_insights' tab")
    except Exception as e:
        print(f"   ✗ Sheets export failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# INSIGHT GENERATORS
# ─────────────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an elite fantasy baseball analyst managing the team S&P in a 12-team Yahoo Fantasy Baseball league. You have deep knowledge of this league's specific rules and use them to drive every recommendation.

LEAGUE FORMAT:
- 12 teams, Head-to-Head Categories (H2H Cats)
- You face one opponent per week, competing across 10 stat categories
- Each category is won, lost, or tied independently (e.g. 6-3-1)
- You do NOT need to outscore them overall — you need to win more categories

ROSTER POSITIONS:
- Hitters: C, 1B, 2B, 3B, SS, 3×OF, 3×Util (9 active hitters)
- Pitchers: 2×SP, 2×RP, 5×P flex (7 total pitcher slots)
- Bench: 4 slots | IL: 3 slots (use aggressively for upside stashes)

THE 10 SCORING CATEGORIES:
HITTING: R (Runs), HR (Home Runs), RBI, SB (Stolen Bases), OBP (On-Base %)
PITCHING: W (Wins), SV (Saves), K (Strikeouts), ERA, WHIP

CATEGORY STRATEGY NOTES:
- SB is the scarcest category — elite SB guys are extremely valuable, never fully punt it
- OBP is a rate stat — quality over quantity, bench low-OBP players in tough matchups
- ERA/WHIP are rate stats — bad starts crater your week, never fully punt these
- Wins are luck-dependent — most expendable pitching category
- Saves are volatile — always roster 2+ proven closers, monitor handcuffs
- K is a volume stat — more innings = more Ks, 7 pitcher slots help here
- R/HR/RBI are volume stats — playing time and lineup position matter

ACQUISITION RULES:
- Max 7 acquisitions per week — do NOT burn all 7 early on speculative streamers
- Rolling waiver list — using a claim drops you to the bottom, save priority for high-value targets
- Free agents (cleared waivers) can be grabbed without losing priority — use this distinction
- 2-day waiver period — plan ahead, claim Friday for Sunday use
- Trade deadline: August 6, 2026

ROSTER CONSTRUCTION RULES:
- Always check positional coverage before making recommendations
- Required positions: C×1, 1B×1, 2B×1, 3B×1, SS×1, OF×3, Util×3, SP×2, RP×2, P×5
- Flag any position with 0 or 1 healthy active players as a CRITICAL NEED
- Injured players on IL do NOT count toward positional coverage
- When recommending waiver adds, ALWAYS prioritize filling positional holes first
- When recommending trades, factor in positional balance
- 3 IL slots available — stashing injured players with upside is valid strategy

PITCHING MINIMUM:
- 30 IP minimum per week — you cannot punt ERA/WHIP by not pitching
- Need 3-4 SP starts minimum per week plus RP appearances
- Don't bench starters to protect ERA unless well over 30 IP

LINEUP MANAGEMENT:
- Lineups lock DAILY — this is a major advantage, use it
- Check who has games each day, rotate Util spots with best available hitters
- Monitor ERA mid-week — if losing ERA badly by Wednesday, consider skipping risky starts

PLAYOFF INFO:
- Top 6 of 12 teams qualify (Weeks 24-25-26, ends Sept 27)
- Reseeding each round — #1 seed always plays weakest remaining opponent
- Target the #1 seed all season

STRATEGIC PRIORITIES:
1. Elite power hitters anchor R, HR, RBI
2. OBP monsters — patient hitters who get on base
3. Speed/SB contributors — at least 2-3 guys with 20+ SB pace
4. Workhorse SP — high innings, good ERA/WHIP/K ratios
5. Proven closers — minimum 2 at all times
6. IL stashes — use all 3 IL spots aggressively for upside

WEEKLY MATCHUP MINDSET:
- Always check opponent's weak categories — free wins are available
- Identify safe wins early and battles that need attention
- Don't overcommit to a category you're already winning
- ERA/WHIP are swingable — attack when opponent is struggling
- Categories you can situationally punt: Wins, sometimes Saves
- Never fully punt ERA, WHIP, or SB

Be direct, specific, and back every recommendation with stats from the data provided.
Never make up stats not present in the data. Always frame advice around WINNING CATEGORIES this week."""


def get_weekly_summary() -> str:
    print("  🧠 Asking Claude for weekly summary...")
    team_stats = load_csv("fantasy_team_stats.csv")
    matchup    = load_csv("fantasy_matchup.csv")
    roster     = load_csv("fantasy_my_roster.csv")
    roster_stats = load_csv("fantasy_roster_stats.csv")

    prompt = f"""
Today is {TODAY}. Give me a concise weekly fantasy baseball report for my team S&P.

CURRENT MATCHUP:
{matchup}

MY TEAM STATS THIS WEEK (by player):
{roster_stats}

MY ROSTER:
{roster}

LEAGUE STANDINGS:
{team_stats}

Please provide:
1. MATCHUP OUTLOOK — am I projected to win or lose this week and why? Which categories are safe wins, which are battles, which am I likely losing?
2. CATEGORY BREAKDOWN — for each of the 10 categories (R, HR, RBI, SB, OBP, W, SV, K, ERA, WHIP), tell me if I'm winning, losing, or too early to tell
3. PITCHING CHECK — am I on pace for the 30 IP minimum? Any ERA/WHIP concerns?
4. THIS WEEK'S PRIORITY — the single most important move I should make today
5. INJURY WATCH — flag any players with IL or questionable status
"""
    return ask_claude(SYSTEM_PROMPT, prompt)


def get_starter_recommendations() -> str:
    print("  🧠 Asking Claude for starter recommendations...")
    roster   = load_csv("fantasy_my_roster.csv")
    schedule = load_csv("mlb_schedule.csv", max_rows=50)
    roster_stats = load_csv("fantasy_roster_stats.csv")

    prompt = f"""
Today is {TODAY}. Here is my current fantasy baseball roster for team S&P:

{roster}

MY CURRENT WEEKLY STATS:
{roster_stats}

THIS WEEK'S MLB SCHEDULE (probable pitchers):
{schedule}

Based on matchup quality, pitcher ratios, and my current category standings:

1. TOP 3 PITCHERS TO START this week — consider opponent offense, ERA/xFIP/WHIP, and K upside. Flag any I should skip to protect ERA/WHIP.
2. TOP 5 BATTERS TO START — prioritize by OBP, R, HR upside and number of games this week
3. 2 PLAYERS TO SIT — explain which category they hurt or why they're a liability this week
4. STREAMING SUGGESTION — one pitcher available on waivers worth picking up this week (consider my 7 move limit)
"""
    return ask_claude(SYSTEM_PROMPT, prompt)


def get_waiver_recommendations() -> str:
    print(" 🧠 Asking Claude for waiver wire picks...")
    roster = load_csv("fantasy_my_roster.csv")
    waiver = load_csv("fantasy_waiver_wire.csv", max_rows=60)
    roster_stats = load_csv("fantasy_roster_stats.csv")

    prompt = f"""
Today is {TODAY}. Here is my current fantasy roster for team S&P:

{roster}

MY CURRENT WEEKLY STATS (shows where I need help):
{roster_stats}

TOP AVAILABLE FREE AGENTS:
{waiver}

Remember: I have a max of 7 acquisitions this week and a rolling waiver list (using a claim costs priority).

1. TOP 3 PLAYERS TO ADD — what specific category need do they fill? Is this a waiver claim (costs priority) or free agent pickup (free)? Do I need a starter for any position this week
2. TOP 2 PLAYERS TO DROP — why are they expendable given my current roster construction?
3. SPECULATIVE ADD — one lower-owned player with a breakout indicator in the stats
4. CLOSER ALERT — flag any closer situations on the waiver wire worth monitoring (saves are volatile)
5. WAIVER PRIORITY ADVICE — should I burn waiver priority this week or save it?

POSITIONAL ANALYSIS — check my roster for these gaps:
- Count active healthy players at each position
- Flag any position where I have 0 or 1 active player as PRIORITY NEED
- Lead your recommendations with any critical positional gaps before general advice

"""
    return ask_claude(SYSTEM_PROMPT, prompt)


def get_trade_analysis(player_give: str, player_receive: str) -> str:
    print(f"  🧠 Asking Claude to analyze trade: Give {player_give} | Get {player_receive}…")
    roster   = load_csv("fantasy_my_roster.csv")
    batting  = load_csv("mlb_batting_season.csv", max_rows=50)
    pitching = load_csv("mlb_pitching_season.csv", max_rows=50)
    roster_stats = load_csv("fantasy_roster_stats.csv")

    prompt = f"""
Today is {TODAY}. Analyze this potential trade for my fantasy team S&P:

MY TEAM GIVES: {player_give}
MY TEAM RECEIVES: {player_receive}

MY CURRENT ROSTER:
{roster}

MY CURRENT WEEKLY CATEGORY STANDINGS:
{roster_stats}

SEASON BATTING STATS:
{batting}

SEASON PITCHING STATS:
{pitching}

Remember this is a 10-category H2H league (R, HR, RBI, SB, OBP, W, SV, K, ERA, WHIP).

1. VERDICT: Accept / Decline / Counter (bold this)
2. CATEGORY IMPACT — for each of the 10 categories, does this trade help, hurt, or neutral?
3. ROSTER FIT — how does this change my team construction and positional balance?
4. COUNTER OFFER — if declining, what would make this trade fair?
5. LONG TERM VIEW — does this help me for the playoffs (top 6 qualify, weeks 24-26)?
"""
    return ask_claude(SYSTEM_PROMPT, prompt, max_tokens=1500)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}")
    print(f"  Claude Fantasy Analyst  |  {TODAY}")
    print(f"{'='*60}\n")

    # ── Collect all insights ──────────────────────────────────────────────────
    insights = {
        "generated_at":    TODAY,
        "weekly_summary":  "",
        "starters":        "",
        "waiver_wire":     "",
        "trade_analysis":  {},
    }

    print("Generating insights...\n")

    try:
        insights["weekly_summary"] = get_weekly_summary()
    except Exception as e:
        insights["weekly_summary"] = f"Error: {e}"

    try:
        insights["starters"] = get_starter_recommendations()
    except Exception as e:
        insights["starters"] = f"Error: {e}"

    try:
        insights["waiver_wire"] = get_waiver_recommendations()
    except Exception as e:
        insights["waiver_wire"] = f"Error: {e}"

    # ── Example trade — edit player names here ────────────────────────────────
    # TODO: Replace with actual trade proposals from your league
    TRADE_GIVE    = ""
    TRADE_RECEIVE = ""

    if TRADE_GIVE and TRADE_RECEIVE:
        try:
            insights["trade_analysis"] = {
                "give":    TRADE_GIVE,
                "receive": TRADE_RECEIVE,
                "analysis": get_trade_analysis(TRADE_GIVE, TRADE_RECEIVE),
            }
        except Exception as e:
            insights["trade_analysis"] = {"error": str(e)}

    # ── Save output ───────────────────────────────────────────────────────────
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(insights, f, indent=2)
        
    export_insights_to_sheets(insights)

    print(f"\n✅ Insights saved → {OUTPUT_FILE}")
    print(   "   In Power BI: Get Data → JSON → point to this file")
    print(   "   Schedule both scripts daily with Windows Task Scheduler\n")

    # ── Print to console too ──────────────────────────────────────────────────
    print("─" * 60)
    print("WEEKLY SUMMARY")
    print("─" * 60)
    print(insights["weekly_summary"])
    print("\n" + "─" * 60)
    print("STARTERS")
    print("─" * 60)
    print(insights["starters"])
    print("\n" + "─" * 60)
    print("WAIVER WIRE")
    print("─" * 60)
    print(insights["waiver_wire"])


if __name__ == "__main__":
    main()
