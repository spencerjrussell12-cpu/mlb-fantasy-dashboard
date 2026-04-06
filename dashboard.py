"""
S&P MLB Fantasy Dashboard — Streamlit App
==========================================
Run with: streamlit run dashboard.py

Requirements:
    pip install streamlit pandas plotly gspread google-auth anthropic python-dotenv
"""

import streamlit as st
import pandas as pd
import json
import os
from datetime import date

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

DATA_DIR   = "/Users/spencerrussell/OneDrive - G&G Outfitters/power_bi_data"
SHEET_ID   = "1RVPs1V-2T6-XmZEi4AMnbWfo3RyI5ZYKxA0pkbsn5aA"
CREDS_FILE = "/Users/spencerrussell/mlb_fantasy_dashboard/google_credentials.json"
SCOPES     = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

st.set_page_config(
    page_title="S&P Analytics",
    page_icon="⚾",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────────────────────────────────────
# THEME
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=IBM+Plex+Mono:wght@400;600&family=DM+Sans:wght@300;400;500&display=swap');

html, body, [class*="css"] {
    background-color: #0A0E14;
    color: #E6EDF3;
    font-family: 'DM Sans', sans-serif;
}

#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 0 2rem 2rem 2rem; max-width: 1400px; }

.stTabs [data-baseweb="tab-list"] {
    background: #0D1117;
    border-bottom: 1px solid rgba(255,161,16,0.2);
    gap: 0;
    padding: 0 16px;
}
.stTabs [data-baseweb="tab"] {
    background: transparent;
    color: rgba(230,237,243,0.4);
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11px;
    letter-spacing: 2px;
    text-transform: uppercase;
    padding: 16px 24px;
    border: none;
    border-bottom: 2px solid transparent;
}
.stTabs [aria-selected="true"] {
    background: transparent !important;
    color: #FFA110 !important;
    border-bottom: 2px solid #FFA110 !important;
}
.stTabs [data-baseweb="tab-panel"] { padding: 24px 0; }

.stDataFrame { border: 1px solid rgba(255,255,255,0.07); border-radius: 4px; }

[data-testid="metric-container"] {
    background: #0D1117;
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 4px;
    padding: 16px;
    border-bottom: 2px solid rgba(255,161,16,0.4);
}
[data-testid="metric-container"] label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 10px;
    letter-spacing: 2px;
    color: rgba(230,237,243,0.4) !important;
    text-transform: uppercase;
}
[data-testid="metric-container"] [data-testid="metric-value"] {
    font-family: 'Bebas Neue', cursive;
    font-size: 32px;
    color: #FFA110;
}

.stSelectbox label { color: rgba(230,237,243,0.5); font-size: 12px; }
h1, h2, h3 { font-family: 'Bebas Neue', cursive; letter-spacing: 2px; color: #FFA110; }

.panel {
    background: #0D1117;
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 4px;
    padding: 20px;
    margin-bottom: 16px;
}

.panel-title {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 10px;
    letter-spacing: 2.5px;
    color: rgba(255,161,16,0.7);
    text-transform: uppercase;
    margin-bottom: 16px;
    padding-bottom: 8px;
    border-bottom: 1px solid rgba(255,161,16,0.15);
}

.scoreboard {
    background: #0D1117;
    border: 1px solid rgba(255,161,16,0.25);
    border-radius: 4px;
    padding: 28px 32px;
    margin-bottom: 20px;
    position: relative;
}

.score-big {
    font-family: 'Bebas Neue', cursive;
    font-size: 72px;
    line-height: 1;
    color: #FFA110;
    letter-spacing: -2px;
}

.score-opp {
    font-family: 'Bebas Neue', cursive;
    font-size: 72px;
    line-height: 1;
    color: rgba(230,237,243,0.4);
    letter-spacing: -2px;
}

.team-label {
    font-family: 'Bebas Neue', cursive;
    font-size: 28px;
    letter-spacing: 3px;
    color: #FFA110;
}

.team-label-opp {
    font-family: 'Bebas Neue', cursive;
    font-size: 28px;
    letter-spacing: 3px;
    color: rgba(230,237,243,0.85);
}

.ai-section {
    background: #0D1117;
    border: 1px solid rgba(255,161,16,0.2);
    border-radius: 4px;
    padding: 20px 24px;
    margin-bottom: 16px;
}

.topbar {
    background: #0A0E14;
    border-bottom: 1px solid rgba(255,161,16,0.2);
    padding: 16px 32px;
    margin: -2rem -2rem 0 -2rem;
    display: flex;
    align-items: center;
    justify-content: space-between;
}

.data-source-badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 4px 10px;
    border-radius: 3px;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 9px;
    letter-spacing: 1px;
    text-transform: uppercase;
}

.badge-sheets {
    background: rgba(52,168,83,0.1);
    border: 1px solid rgba(52,168,83,0.3);
    color: rgba(52,168,83,0.9);
}

.badge-local {
    background: rgba(255,161,16,0.1);
    border: 1px solid rgba(255,161,16,0.3);
    color: rgba(255,161,16,0.9);
}

/* ── Chat bubbles (Tab 5) ─────────────────────────────── */
.chat-wrap {
    max-height: 520px;
    overflow-y: auto;
    display: flex;
    flex-direction: column;
    gap: 12px;
    padding: 4px 2px 16px 2px;
}

.bubble-user {
    align-self: flex-end;
    background: rgba(255,161,16,0.12);
    border: 1px solid rgba(255,161,16,0.3);
    border-radius: 12px 12px 2px 12px;
    padding: 12px 16px;
    max-width: 78%;
    font-family: 'DM Sans', sans-serif;
    font-size: 13px;
    color: #E6EDF3;
    line-height: 1.6;
}

.bubble-assistant {
    align-self: flex-start;
    background: #0D1117;
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 12px 12px 12px 2px;
    padding: 14px 18px;
    max-width: 88%;
    font-family: 'DM Sans', sans-serif;
    font-size: 13px;
    color: rgba(230,237,243,0.85);
    line-height: 1.75;
}

.bubble-label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 9px;
    letter-spacing: 1.5px;
    text-transform: uppercase;
    margin-bottom: 6px;
}

/* ── Category pills (Tab 6) ───────────────────────────── */
.cat-pill {
    display: inline-block;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 10px;
    letter-spacing: 1px;
    padding: 3px 8px;
    border-radius: 3px;
    margin: 2px;
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS CONNECTION
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_resource(ttl=300)
def get_sheets_client():
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        try:
            if "gcp_service_account" in st.secrets:
                creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)
            elif os.path.exists(CREDS_FILE):
                creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
            else:
                return None, "no_creds"
        except Exception:
            if os.path.exists(CREDS_FILE):
                creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
            else:
                return None, "no_creds"
        client = gspread.authorize(creds)
        sheet  = client.open_by_key(SHEET_ID)
        return sheet, "sheets"
    except Exception as e:
        return None, f"error: {e}"


@st.cache_data(ttl=300)
def load_from_sheets(tab_name: str):
    sheet, status = get_sheets_client()
    if sheet is None:
        return pd.DataFrame(), "local"
    try:
        ws   = sheet.worksheet(tab_name)
        data = ws.get_all_records()
        df   = pd.DataFrame(data)
        df   = df.replace("", pd.NA)
        return df, "sheets"
    except Exception:
        return pd.DataFrame(), "local"


@st.cache_data(ttl=300)
def load_from_csv(filename: str) -> pd.DataFrame:
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path)


def load(tab_name: str, csv_filename: str = None) -> pd.DataFrame:
    if csv_filename is None:
        csv_filename = f"{tab_name}.csv"
    df, _ = load_from_sheets(tab_name)
    if df.empty:
        return load_from_csv(csv_filename)
    return df


@st.cache_data(ttl=300)
def load_metadata() -> dict:
    sheet, _ = get_sheets_client()
    if sheet is None:
        return {}
    try:
        ws   = sheet.worksheet("metadata")
        rows = ws.get_all_records()
        return {r["key"]: r["value"] for r in rows}
    except Exception:
        return {}


@st.cache_data(ttl=300)
def load_insights():
    sheet, _ = get_sheets_client()
    if sheet:
        try:
            ws   = sheet.worksheet("claude_insights")
            rows = ws.get_all_records()
            data = {r["key"]: r["value"] for r in rows}
            return {
                "generated_at":   data.get("generated_at", ""),
                "weekly_summary": data.get("weekly_summary", ""),
                "starters":       data.get("starters", ""),
                "waiver_wire":    data.get("waiver_wire", ""),
                "trade_analysis": {
                    "give":     data.get("trade_give", ""),
                    "receive":  data.get("trade_receive", ""),
                    "analysis": data.get("trade_analysis", ""),
                },
            }
        except Exception:
            pass
    path = os.path.join(DATA_DIR, "claude_insights.json")
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)


def snp_roster(df):
    if "fantasy_team_name" in df.columns:
        return df[df["fantasy_team_name"].str.contains("S&P", na=False)]
    return df


def get_anthropic_key():
    from dotenv import load_dotenv
    load_dotenv("/Users/spencerrussell/mlb_fantasy_dashboard/.env")
    key = os.getenv("ANTHROPIC_API_KEY")
    if not key:
        try:
            key = st.secrets["anthropic"]["api_key"]
        except Exception:
            pass
    return key


# ─────────────────────────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────────────────────────

_, sheets_status = get_sheets_client()
using_sheets     = sheets_status == "sheets"
meta             = load_metadata()
last_updated     = meta.get("last_updated", "Unknown")

source_badge = (
    '<span class="data-source-badge badge-sheets">● Live — Google Sheets</span>'
    if using_sheets else
    '<span class="data-source-badge badge-local">● Local CSV</span>'
)

st.markdown(f"""
<div class="topbar">
  <div style="display:flex;align-items:center;gap:14px">
    <div style="width:38px;height:38px;background:#FFA110;transform:rotate(45deg);
                display:flex;align-items:center;justify-content:center;flex-shrink:0">
      <span style="transform:rotate(-45deg);font-family:'Bebas Neue',cursive;font-size:13px;
                   color:#0A0E14;letter-spacing:1px">S&P</span>
    </div>
    <div>
      <div style="font-family:'Bebas Neue',cursive;font-size:22px;letter-spacing:3px;color:#FFA110">S&P Analytics</div>
      <div style="font-size:10px;color:rgba(230,237,243,0.35);letter-spacing:2px;text-transform:uppercase">MLB Fantasy Dashboard</div>
    </div>
  </div>
  <div style="display:flex;align-items:center;gap:16px">
    {source_badge}
    <div style="font-family:'IBM Plex Mono',monospace;font-size:11px;color:rgba(230,237,243,0.35);letter-spacing:1px">
      {date.today().strftime('%b %d, %Y').upper()}
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

if last_updated != "Unknown":
    st.markdown(f"""
    <div style="font-family:'IBM Plex Mono',monospace;font-size:9px;letter-spacing:1px;
                color:rgba(230,237,243,0.7);text-align:right;padding:6px 0 0 0">
      Data refreshed: {last_updated}
    </div>
    """, unsafe_allow_html=True)

st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "⚾  Fantasy Hub",
    "📊  League Overview",
    "🤖  AI Insights",
    "🔍  Player Deep Dive",
    "💬  Roster Q&A",
    "🕵️  Opponent Scouting",
])

# ═════════════════════════════════════════════════════════════════════════════
# TAB 1 — FANTASY HUB
# ═════════════════════════════════════════════════════════════════════════════

with tab1:
    matchup_df   = load("fantasy_matchup",      "fantasy_matchup.csv")
    roster_df    = load("fantasy_roster",        "fantasy_my_roster.csv")
    roster_stats = load("fantasy_roster_stats",  "fantasy_roster_stats.csv")
    injuries_df  = load("mlb_injuries",          "mlb_injuries.csv")

    week_num = matchup_df["week"].iloc[0] if not matchup_df.empty and "week" in matchup_df.columns else "?"
    my_row = None

    if not matchup_df.empty:
        for _, row in matchup_df.iterrows():
            t1 = str(row.get("team_1", "")).strip("b'").strip("'")
            t2 = str(row.get("team_2", "")).strip("b'").strip("'")
            if "S&P" in t1:
                my_row = {"name": t1, "score": row.get("team_1_score", 0), "opp": t2, "opp_score": row.get("team_2_score", 0)}
            elif "S&P" in t2:
                my_row = {"name": t2, "score": row.get("team_2_score", 0), "opp": t1, "opp_score": row.get("team_1_score", 0)}

        if my_row:
            my_score  = float(my_row["score"] or 0)
            opp_score = float(my_row["opp_score"] or 0)
            outlook   = "🟢 WINNING" if my_score > opp_score else ("🔴 LOSING" if my_score < opp_score else "⚪ TIED")

            st.markdown(f"""
            <div class="scoreboard">
              <div style="font-family:'IBM Plex Mono',monospace;font-size:10px;letter-spacing:3px;
                          color:rgba(255,161,16,0.6);margin-bottom:20px">● LIVE MATCHUP — WEEK {week_num}</div>
              <div style="display:flex;align-items:flex-end;justify-content:space-between;gap:24px">
                <div>
                  <div class="team-label">S&P</div>
                  <div class="score-big">{int(my_score)}</div>
                </div>
                <div style="text-align:center;padding-bottom:12px">
                  <div style="font-family:'IBM Plex Mono',monospace;font-size:11px;color:rgba(230,237,243,0.7);letter-spacing:2px;margin-bottom:8px">{outlook}</div>
                  <div style="font-family:'IBM Plex Mono',monospace;font-size:13px;color:rgba(230,237,243,0.7);letter-spacing:3px">VS</div>
                </div>
                <div style="text-align:right">
                  <div class="team-label-opp">{my_row['opp']}</div>
                  <div class="score-opp">{int(opp_score)}</div>
                </div>
              </div>
            </div>
            """, unsafe_allow_html=True)

    matchup_cats = load("fantasy_matchup_cats", "fantasy_matchup_cats.csv")

    if not matchup_cats.empty and my_row:
        cats    = ["Runs", "HR", "RBI", "SB", "OBP", "Wins", "Saves", "K", "ERA", "WHIP"]
        abbrevs = ["R",    "HR", "RBI", "SB", "OBP", "W",    "SV",    "K", "ERA", "WHIP"]

        for cat in cats:
            if cat in matchup_cats.columns:
                matchup_cats[cat] = pd.to_numeric(matchup_cats[cat], errors="coerce")

        snp_row = matchup_cats[matchup_cats["fantasy_team_name"].str.contains("S&P", na=False)]
        opp_row = matchup_cats[matchup_cats["fantasy_team_name"].str.contains(my_row["opp"].strip(), na=False, regex=False)]

        if not snp_row.empty and not opp_row.empty:
            snp = snp_row.iloc[0]
            opp = opp_row.iloc[0]

            for cat in cats:
                try: snp[cat] = float(snp[cat])
                except: snp[cat] = None
                try: opp[cat] = float(opp[cat])
                except: opp[cat] = None

            def fmt(val, cat):
                if val is None or pd.isna(val): return "—"
                return f"{val:.2f}" if cat in ["ERA", "WHIP", "OBP"] else str(int(val))

            def snp_wins(cat, sv, ov):
                if pd.isna(sv) or pd.isna(ov): return None
                if sv == ov: return None
                return sv < ov if cat in ["ERA", "WHIP"] else sv > ov

            header_cols = st.columns([1] + [1]*10)
            with header_cols[0]:
                st.markdown(f"<div style='font-family:IBM Plex Mono,monospace;font-size:9px;letter-spacing:1px;color:rgba(230,237,243,0.3);padding-top:8px'>WEEK {week_num}</div>", unsafe_allow_html=True)
            for i, abbr in enumerate(abbrevs):
                with header_cols[i+1]:
                    st.markdown(f"<div style='font-family:IBM Plex Mono,monospace;font-size:9px;letter-spacing:1px;color:rgba(230,237,243,0.4);text-align:center'>{abbr}</div>", unsafe_allow_html=True)

            snp_cols = st.columns([1] + [1]*10)
            with snp_cols[0]:
                st.markdown("<div style='font-family:Bebas Neue,cursive;font-size:16px;color:#FFA110;padding-top:4px'>S&P</div>", unsafe_allow_html=True)
            for i, cat in enumerate(cats):
                sv = snp.get(cat); ov = opp.get(cat); win = snp_wins(cat, sv, ov)
                if win is True:
                    style = "background:rgba(255,161,16,0.12);border:1px solid rgba(255,161,16,0.4);border-radius:3px;padding:6px 4px;text-align:center"
                    color = "#FFA110"; weight = "700"
                elif win is False:
                    style = "background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.06);border-radius:3px;padding:6px 4px;text-align:center"
                    color = "rgba(230,237,243,0.4)"; weight = "400"
                else:
                    style = "background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.06);border-radius:3px;padding:6px 4px;text-align:center"
                    color = "rgba(230,237,243,0.5)"; weight = "400"
                with snp_cols[i+1]:
                    st.markdown(f"<div style='{style}'><span style='font-family:Bebas Neue,cursive;font-size:18px;color:{color};font-weight:{weight};line-height:1'>{fmt(sv, cat)}</span></div>", unsafe_allow_html=True)

            opp_name_t1 = my_row["opp"]
            opp_cols = st.columns([1] + [1]*10)
            with opp_cols[0]:
                st.markdown(f"<div style='font-family:Bebas Neue,cursive;font-size:16px;color:rgba(230,237,243,0.5);padding-top:4px'>{opp_name_t1[:8]}</div>", unsafe_allow_html=True)
            for i, cat in enumerate(cats):
                sv = snp.get(cat); ov = opp.get(cat); win = snp_wins(cat, sv, ov)
                if win is False:
                    style = "background:rgba(255,80,80,0.08);border:1px solid rgba(255,80,80,0.3);border-radius:3px;padding:6px 4px;text-align:center"
                    color = "rgba(255,120,120,0.9)"; weight = "700"
                else:
                    style = "background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.06);border-radius:3px;padding:6px 4px;text-align:center"
                    color = "rgba(230,237,243,0.3)"; weight = "400"
                with opp_cols[i+1]:
                    st.markdown(f"<div style='{style}'><span style='font-family:Bebas Neue,cursive;font-size:18px;color:{color};font-weight:{weight};line-height:1'>{fmt(ov, cat)}</span></div>", unsafe_allow_html=True)

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.markdown('<div class="panel-title">S&P ROSTER</div>', unsafe_allow_html=True)
        if not roster_df.empty:
            snp = snp_roster(roster_df)
            display_cols = [c for c in ["player_name", "position", "status", "ops", "era", "war_bat", "war_pitch"] if c in snp.columns]
            st.dataframe(snp[display_cols].rename(columns={"player_name": "Player", "position": "Pos", "status": "Status", "ops": "OPS", "era": "ERA", "war_bat": "WAR(B)", "war_pitch": "WAR(P)"}), hide_index=True, use_container_width=True, height=420)

    with col_right:
        st.markdown('<div class="panel-title">WAIVER WIRE — URGENCY SCORES</div>', unsafe_allow_html=True)
        waiver_df   = load("fantasy_waiver_wire", "fantasy_waiver_wire.csv")
        schedule_df = load("mlb_schedule", "mlb_schedule.csv")

        if not waiver_df.empty:
            for col in ["ops", "era", "wrc_plus", "stolen_bases", "saves"]:
                if col in waiver_df.columns:
                    waiver_df[col] = pd.to_numeric(waiver_df[col], errors="coerce")

            snp_for_needs = snp_roster(roster_df) if not roster_df.empty else pd.DataFrame()
            snp_active    = snp_for_needs[~snp_for_needs["status"].astype(str).str.contains("IL", na=False)] if not snp_for_needs.empty else pd.DataFrame()

            position_counts = {}
            if not snp_active.empty and "position" in snp_active.columns:
                for _, r in snp_active.iterrows():
                    for pos in str(r["position"]).split(","):
                        pos = pos.strip()
                        position_counts[pos] = position_counts.get(pos, 0) + 1
            need_positions = {p for p, c in position_counts.items() if c <= 1}

            team_games = {}
            if not schedule_df.empty:
                for _, g in schedule_df.iterrows():
                    for t in [g.get("away_team", ""), g.get("home_team", "")]:
                        if t: team_games[t] = team_games.get(t, 0) + 1

            def score_player(row):
                score = 0; reason = []
                team  = str(row.get("team_x", row.get("team", "")))
                games = 0
                for t, g in team_games.items():
                    if team and team.lower() in t.lower(): games = g; break
                score += min(int((games / 7) * 20), 20)
                if games >= 6: reason.append(f"{games} games")
                pos   = str(row.get("position", ""))
                ops   = pd.to_numeric(row.get("ops"),          errors="coerce")
                wrc   = pd.to_numeric(row.get("wrc_plus"),     errors="coerce")
                sb    = pd.to_numeric(row.get("stolen_bases"), errors="coerce")
                era   = pd.to_numeric(row.get("era"),          errors="coerce")
                saves = pd.to_numeric(row.get("saves"),        errors="coerce")
                if pd.notna(ops) and ops > 0:
                    score += min(int((ops / 1.2) * 15), 15)
                    if ops >= 0.800: reason.append(f".{int(ops*1000)} OPS")
                if pd.notna(wrc) and wrc > 0:
                    score += min(int((wrc / 160) * 7), 7)
                    if wrc >= 120: reason.append(f"{int(wrc)} wRC+")
                if pd.notna(sb) and sb > 5:
                    score += min(int((sb / 40) * 8), 8)
                    if sb >= 15: reason.append(f"{int(sb)} SB")
                if pd.notna(era) and era > 0 and any(p in pos for p in ["SP", "RP", "P"]):
                    score += min(int(max(0, (4.5 - era) / 4.5) * 15), 15)
                    if era <= 3.50: reason.append(f"{era:.2f} ERA")
                if pd.notna(saves) and saves > 0:
                    score += min(int((saves / 40) * 15), 15)
                    if saves >= 10: reason.append(f"{int(saves)} SV")
                for p in pos.split(","):
                    p = p.strip()
                    if p in need_positions: score += 30; reason.append(f"fills {p} need"); break
                return min(score, 100), (" · ".join(reason) if reason else "depth")

            waiver_df[["score", "reason"]] = waiver_df.apply(lambda r: pd.Series(score_player(r)), axis=1)
            waiver_df = waiver_df.sort_values("score", ascending=False).head(20)

            scroll_html = ""
            for _, row in waiver_df.iterrows():
                score  = int(row["score"])
                name   = row.get("player_name", "")
                pos    = row.get("position", "")
                team   = str(row.get("team_x", row.get("team", "")))
                reason = row.get("reason", "")
                if score >= 70:
                    bar_color = "#FFA110"; badge_bg = "rgba(255,161,16,0.15)"; badge_border = "rgba(255,161,16,0.4)"
                elif score >= 45:
                    bar_color = "rgba(52,168,83,0.8)"; badge_bg = "rgba(52,168,83,0.1)"; badge_border = "rgba(52,168,83,0.3)"
                else:
                    bar_color = "rgba(230,237,243,0.2)"; badge_bg = "rgba(255,255,255,0.03)"; badge_border = "rgba(255,255,255,0.1)"
                scroll_html += f"""
<div style="background:{badge_bg};border:1px solid {badge_border};border-radius:4px;padding:10px 14px;margin-bottom:6px">
<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">
<div><span style="font-family:'DM Sans',sans-serif;font-size:13px;color:#E6EDF3;font-weight:500">{name}</span>
<span style="font-family:'IBM Plex Mono',monospace;font-size:9px;color:rgba(230,237,243,0.4);margin-left:8px">{pos} · {team}</span></div>
<span style="font-family:'Bebas Neue',cursive;font-size:22px;color:{bar_color};letter-spacing:1px">{score}</span>
</div>
<div style="background:rgba(255,255,255,0.06);border-radius:2px;height:3px;margin-bottom:6px">
<div style="background:{bar_color};width:{score}%;height:3px;border-radius:2px"></div>
</div>
<div style="font-family:'IBM Plex Mono',monospace;font-size:9px;color:rgba(230,237,243,0.4);letter-spacing:0.5px">{reason}</div>
</div>"""
            st.markdown(f'<div style="height:420px;overflow-y:auto;padding-right:4px">{scroll_html}</div>', unsafe_allow_html=True)

    st.markdown('<div class="panel-title" style="margin-top:20px">INJURY TRACKER</div>', unsafe_allow_html=True)
    if not injuries_df.empty:
        snp_names = snp_roster(roster_df)["player_name"].tolist() if not roster_df.empty else []
        il_df = injuries_df[injuries_df["player_name"].isin(snp_names)] if snp_names else injuries_df.head(20)
        if not il_df.empty:
            st.dataframe(il_df[["player_name", "transaction_type", "description", "date"]].rename(columns={"player_name": "Player", "transaction_type": "Type", "description": "Description", "date": "Date"}), hide_index=True, use_container_width=True)
        else:
            st.markdown("<p style='color:rgba(230,237,243,0.4);font-size:13px'>No injury records found for S&P roster.</p>", unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 2 — LEAGUE OVERVIEW
# ═════════════════════════════════════════════════════════════════════════════

with tab2:
    import plotly.express as px

    batting_df  = load("mlb_batting_season",  "mlb_batting_season.csv")
    pitching_df = load("mlb_pitching_season", "mlb_pitching_season.csv")
    team_stats  = load("fantasy_team_stats",  "fantasy_team_stats.csv")

    for col in ["ops", "home_runs", "wrc_plus"]:
        if not batting_df.empty and col in batting_df.columns:
            batting_df[col] = pd.to_numeric(batting_df[col], errors="coerce")
    for col in ["era", "whip"]:
        if not pitching_df.empty and col in pitching_df.columns:
            pitching_df[col] = pd.to_numeric(pitching_df[col], errors="coerce")

    if not batting_df.empty:
        col1, col2, col3, col4 = st.columns(4)
        with col1: st.metric("Avg OPS", f"{batting_df['ops'].mean():.3f}" if "ops" in batting_df.columns else "—")
        with col2: st.metric("Total HRs", f"{int(batting_df['home_runs'].sum())}" if "home_runs" in batting_df.columns else "—")
        with col3: st.metric("Avg wRC+", f"{batting_df['wrc_plus'].mean():.0f}" if "wrc_plus" in batting_df.columns else "—")
        with col4: st.metric("Avg ERA", f"{pitching_df['era'].mean():.2f}" if not pitching_df.empty and "era" in pitching_df.columns else "—")

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown('<div class="panel-title">TOP 15 BATTERS BY OPS</div>', unsafe_allow_html=True)
        if not batting_df.empty and "ops" in batting_df.columns:
            top_bat = batting_df.nlargest(15, "ops")[["player_name", "ops", "home_runs", "wrc_plus"]].dropna()
            fig = px.bar(top_bat, x="ops", y="player_name", orientation="h", color="ops", color_continuous_scale=["#161B22", "#FFA110"], labels={"ops": "OPS", "player_name": ""})
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font=dict(color="#E6EDF3", family="IBM Plex Mono", size=11), coloraxis_showscale=False, margin=dict(l=0,r=0,t=0,b=0), height=420, yaxis=dict(gridcolor="rgba(255,255,255,0.05)"), xaxis=dict(gridcolor="rgba(255,255,255,0.05)"))
            fig.update_traces(marker_line_width=0)
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.markdown('<div class="panel-title">TOP 15 PITCHERS BY ERA (min 20 IP)</div>', unsafe_allow_html=True)
        if not pitching_df.empty and "era" in pitching_df.columns:
            top_pit = pitching_df.nsmallest(15, "era")[["player_name", "era", "whip", "k_per_9"]].dropna()
            fig2 = px.bar(top_pit, x="era", y="player_name", orientation="h", color="era", color_continuous_scale=["#FFA110", "#161B22"], labels={"era": "ERA", "player_name": ""})
            fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font=dict(color="#E6EDF3", family="IBM Plex Mono", size=11), coloraxis_showscale=False, margin=dict(l=0,r=0,t=0,b=0), height=420, yaxis=dict(gridcolor="rgba(255,255,255,0.05)"), xaxis=dict(gridcolor="rgba(255,255,255,0.05)"))
            fig2.update_traces(marker_line_width=0)
            st.plotly_chart(fig2, use_container_width=True)

    st.markdown('<div class="panel-title" style="margin-top:8px">LEAGUE STANDINGS</div>', unsafe_allow_html=True)
    if not team_stats.empty:
        display_cols = [c for c in ["fantasy_team_name", "wins", "losses", "ties", "points", "standing"] if c in team_stats.columns]
        st.dataframe(team_stats[display_cols].rename(columns={"fantasy_team_name": "Team", "wins": "W", "losses": "L", "ties": "T", "points": "Pts", "standing": "Rank"}), hide_index=True, use_container_width=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 3 — AI INSIGHTS
# ═════════════════════════════════════════════════════════════════════════════

with tab3:
    insights = load_insights()

    if not insights:
        st.markdown("""
        <div style='text-align:center;padding:60px;color:rgba(230,237,243,0.7)'>
            <div style='font-family:Bebas Neue,cursive;font-size:32px;color:rgba(255,161,16,0.4);margin-bottom:12px'>No Insights Yet</div>
            <div style='font-family:IBM Plex Mono,monospace;font-size:12px;letter-spacing:1px'>Run claude_insights.py to generate recommendations</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        generated = insights.get("generated_at", "Unknown")
        st.markdown(f"""
        <div style='display:flex;align-items:center;gap:12px;margin-bottom:24px'>
          <div style='width:8px;height:8px;background:#FFA110;border-radius:50%'></div>
          <div style='font-family:IBM Plex Mono,monospace;font-size:10px;letter-spacing:2px;color:rgba(255,161,16,0.7);text-transform:uppercase'>
            Claude AI — Last updated {generated}
          </div>
        </div>
        """, unsafe_allow_html=True)

        for title, key in [
            ("📋  WEEKLY SUMMARY",             "weekly_summary"),
            ("⚾  START / SIT RECOMMENDATIONS", "starters"),
            ("📡  WAIVER WIRE",                 "waiver_wire"),
        ]:
            content = insights.get(key, "")
            if content and not content.startswith("Error"):
                with st.expander(title, expanded=True):
                    st.markdown(f"<div style='font-size:13px;line-height:1.8;color:rgba(230,237,243,0.8);font-family:DM Sans,sans-serif'>{content.replace(chr(10), '<br>')}</div>", unsafe_allow_html=True)

        trade = insights.get("trade_analysis", {})
        if trade and "analysis" in trade:
            with st.expander("🔄  TRADE ANALYSIS", expanded=False):
                st.markdown(f"""
                <div style='margin-bottom:16px;padding:12px 16px;background:rgba(255,161,16,0.08);border:1px solid rgba(255,161,16,0.2);border-radius:4px'>
                  <span style='font-family:IBM Plex Mono,monospace;font-size:10px;color:rgba(255,161,16,0.7);letter-spacing:2px'>GIVE</span>
                  <span style='font-family:Bebas Neue,cursive;font-size:18px;color:#FFA110;margin:0 16px'>{trade.get('give','')}</span>
                  <span style='font-family:IBM Plex Mono,monospace;font-size:10px;color:rgba(255,161,16,0.7);letter-spacing:2px'>RECEIVE</span>
                  <span style='font-family:Bebas Neue,cursive;font-size:18px;color:#FFA110;margin-left:16px'>{trade.get('receive','')}</span>
                </div>
                <div style='font-size:13px;line-height:1.8;color:rgba(230,237,243,0.8)'>{trade['analysis'].replace(chr(10), '<br>')}</div>
                """, unsafe_allow_html=True)

    st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)
    st.markdown('<div class="panel-title">LIVE TRADE ANALYZER</div>', unsafe_allow_html=True)

    col_give, col_receive = st.columns(2)
    with col_give:
        trade_give = st.text_input("I GIVE", placeholder="e.g. Elly De La Cruz", key="trade_give")
    with col_receive:
        trade_receive = st.text_input("I RECEIVE", placeholder="e.g. Spencer Strider + Pete Fairbanks", key="trade_receive")

    if st.button("⚾ Analyze Trade", type="primary"):
        if trade_give and trade_receive:
            with st.spinner("Asking Claude..."):
                try:
                    import anthropic
                    roster_raw   = load("fantasy_roster",       "fantasy_my_roster.csv")
                    batting_raw  = load("mlb_batting_season",   "mlb_batting_season.csv")
                    pitching_raw = load("mlb_pitching_season",  "mlb_pitching_season.csv")
                    r_stats_raw  = load("fantasy_roster_stats", "fantasy_roster_stats.csv")
                    for col in ["ops", "home_runs", "wrc_plus"]:
                        if col in batting_raw.columns: batting_raw[col] = pd.to_numeric(batting_raw[col], errors="coerce")
                    for col in ["era", "whip"]:
                        if col in pitching_raw.columns: pitching_raw[col] = pd.to_numeric(pitching_raw[col], errors="coerce")
                    snp       = snp_roster(roster_raw)
                    snp_stats = snp_roster(r_stats_raw)
                    prompt = f"""Today is {date.today()}. Analyze this potential trade for my fantasy team S&P:
MY TEAM GIVES: {trade_give}
MY TEAM RECEIVES: {trade_receive}
MY CURRENT ROSTER:\n{snp.to_string(index=False)}
MY CURRENT WEEKLY CATEGORY STANDINGS:\n{snp_stats.head(30).to_string(index=False)}
SEASON BATTING STATS (top 50):\n{batting_raw.head(50).to_string(index=False)}
SEASON PITCHING STATS (top 50):\n{pitching_raw.head(50).to_string(index=False)}
Remember this is a 10-category H2H league (R, HR, RBI, SB, OBP, W, SV, K, ERA, WHIP).
1. VERDICT: Accept / Decline / Counter
2. CATEGORY IMPACT — for each of the 10 categories, does this trade help, hurt, or neutral?
3. ROSTER FIT — how does this change my team construction?
4. COUNTER OFFER — if declining, what would make this trade fair?
5. LONG TERM VIEW — does this help me for the playoffs (top 6 qualify, weeks 24-26)?"""
                    api_key = get_anthropic_key()
                    client  = anthropic.Anthropic(api_key=api_key)
                    message = client.messages.create(
                        model="claude-sonnet-4-6", max_tokens=1500,
                        system="You are an elite fantasy baseball analyst managing S&P in a 12-team H2H Categories league. Scoring: R, HR, RBI, SB, OBP (hitting) and W, SV, K, ERA, WHIP (pitching). Be direct, back every recommendation with stats, never make up numbers not in the data.",
                        messages=[{"role": "user", "content": prompt}],
                    )
                    result = message.content[0].text
                    st.markdown(f"""
                    <div style='background:#0D1117;border:1px solid rgba(255,161,16,0.25);border-radius:4px;padding:24px;margin-top:16px'>
                      <div style='font-family:IBM Plex Mono,monospace;font-size:10px;letter-spacing:2px;color:rgba(255,161,16,0.7);margin-bottom:16px'>
                        ● TRADE ANALYSIS — {trade_give.upper()} FOR {trade_receive.upper()}
                      </div>
                      <div style='font-size:13px;line-height:1.8;color:rgba(230,237,243,0.8)'>{result.replace(chr(10), '<br>')}</div>
                    </div>
                    """, unsafe_allow_html=True)
                except Exception as e:
                    st.error(f"Analysis failed: {e}")
        else:
            st.warning("Enter both sides of the trade first.")


# ═════════════════════════════════════════════════════════════════════════════
# TAB 4 — PLAYER DEEP DIVE
# ═════════════════════════════════════════════════════════════════════════════

with tab4:
    batting_df  = load("mlb_batting_season",  "mlb_batting_season.csv")
    pitching_df = load("mlb_pitching_season", "mlb_pitching_season.csv")
    injuries_df = load("mlb_injuries",        "mlb_injuries.csv")
    schedule_df = load("mlb_schedule",        "mlb_schedule.csv")

    for col in ["ops", "home_runs", "wrc_plus"]:
        if not batting_df.empty and col in batting_df.columns:
            batting_df[col] = pd.to_numeric(batting_df[col], errors="coerce")
    for col in ["era", "whip"]:
        if not pitching_df.empty and col in pitching_df.columns:
            pitching_df[col] = pd.to_numeric(pitching_df[col], errors="coerce")

    all_players = []
    if not batting_df.empty and "player_name" in batting_df.columns:
        all_players += batting_df["player_name"].dropna().tolist()
    if not pitching_df.empty and "player_name" in pitching_df.columns:
        all_players += pitching_df["player_name"].dropna().tolist()
    all_players = sorted(set(all_players))

    selected = st.selectbox("Search for any MLB player", options=[""] + all_players, index=0, placeholder="Type a player name...")

    if selected:
        st.markdown(f"<div style='margin-bottom:24px'><span style='font-family:Bebas Neue,cursive;font-size:36px;letter-spacing:2px;color:#FFA110'>{selected}</span></div>", unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        with col1:
            bat_row = batting_df[batting_df["player_name"] == selected] if not batting_df.empty else pd.DataFrame()
            if not bat_row.empty:
                st.markdown('<div class="panel-title">BATTING STATS</div>', unsafe_allow_html=True)
                bat_display = bat_row.T.reset_index(); bat_display.columns = ["Stat", "Value"]
                bat_display = bat_display[~bat_display["Stat"].isin(["player_name", "season", "stat_type"])]
                st.dataframe(bat_display, hide_index=True, use_container_width=True)
        with col2:
            pit_row = pitching_df[pitching_df["player_name"] == selected] if not pitching_df.empty else pd.DataFrame()
            if not pit_row.empty:
                st.markdown('<div class="panel-title">PITCHING STATS</div>', unsafe_allow_html=True)
                pit_display = pit_row.T.reset_index(); pit_display.columns = ["Stat", "Value"]
                pit_display = pit_display[~pit_display["Stat"].isin(["player_name", "season", "stat_type"])]
                st.dataframe(pit_display, hide_index=True, use_container_width=True)
        if bat_row.empty and pit_row.empty:
            st.markdown("<p style='color:rgba(230,237,243,0.4)'>No stats found for this player.</p>", unsafe_allow_html=True)

        st.markdown('<div class="panel-title" style="margin-top:20px">INJURY HISTORY</div>', unsafe_allow_html=True)
        if not injuries_df.empty:
            player_il = injuries_df[injuries_df["player_name"] == selected]
            if not player_il.empty:
                st.dataframe(player_il[["transaction_type", "description", "date"]].rename(columns={"transaction_type": "Type", "description": "Description", "date": "Date"}), hide_index=True, use_container_width=True)
            else:
                st.markdown("<p style='color:rgba(230,237,243,0.4);font-size:13px'>No injury records found.</p>", unsafe_allow_html=True)

        st.markdown('<div class="panel-title" style="margin-top:20px">UPCOMING SCHEDULE</div>', unsafe_allow_html=True)
        if not schedule_df.empty:
            player_team = None
            if not bat_row.empty and "team" in bat_row.columns: player_team = bat_row["team"].iloc[0]
            elif not pit_row.empty and "team" in pit_row.columns: player_team = pit_row["team"].iloc[0]
            if player_team:
                player_games = schedule_df[(schedule_df["away_team"].str.contains(str(player_team), na=False, case=False)) | (schedule_df["home_team"].str.contains(str(player_team), na=False, case=False))]
            else:
                player_games = schedule_df[(schedule_df.get("away_pitcher", pd.Series()) == selected) | (schedule_df.get("home_pitcher", pd.Series()) == selected)] if "away_pitcher" in schedule_df.columns else pd.DataFrame()
            if not player_games.empty:
                st.dataframe(player_games[["game_date", "away_team", "home_team", "away_pitcher", "home_pitcher", "venue", "status"]].rename(columns={"game_date": "Date", "away_team": "Away", "home_team": "Home", "away_pitcher": "Away SP", "home_pitcher": "Home SP", "venue": "Venue", "status": "Status"}), hide_index=True, use_container_width=True)
            else:
                st.markdown("<p style='color:rgba(230,237,243,0.4);font-size:13px'>No upcoming games found.</p>", unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style='text-align:center;padding:60px;color:rgba(230,237,243,0.2)'>
          <div style='font-family:Bebas Neue,cursive;font-size:48px;color:rgba(255,161,16,0.15)'>Search any player</div>
          <div style='font-family:IBM Plex Mono,monospace;font-size:11px;letter-spacing:2px;margin-top:8px'>537 batters · 542 pitchers</div>
        </div>
        """, unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 5 — ROSTER Q&A
# ═════════════════════════════════════════════════════════════════════════════

with tab5:
    if "qa_messages" not in st.session_state:
        st.session_state.qa_messages = []

    qa_roster_df    = load("fantasy_roster",       "fantasy_my_roster.csv")
    qa_waiver_df    = load("fantasy_waiver_wire",  "fantasy_waiver_wire.csv")
    qa_standings_df = load("fantasy_team_stats",   "fantasy_team_stats.csv")
    qa_matchup_df   = load("fantasy_matchup_cats", "fantasy_matchup_cats.csv")
    qa_injuries_df  = load("mlb_injuries",         "mlb_injuries.csv")
    qa_schedule_df  = load("mlb_schedule",         "mlb_schedule.csv")

    def build_qa_system_prompt():
        def df_snippet(df, label, max_rows=150):
            if df is None or df.empty: return f"[{label}: no data available]\n"
            return f"### {label}\n{df.head(max_rows).to_string(index=False)}\n\n"

        snp_inj = qa_injuries_df
        if not qa_injuries_df.empty and not qa_roster_df.empty:
            snp_names_set = set(snp_roster(qa_roster_df).get("player_name", pd.Series(dtype=str)).tolist())
            name_col = next((c for c in ["player_name", "Name", "name"] if c in qa_injuries_df.columns), None)
            if name_col and snp_names_set:
                snp_inj = qa_injuries_df[qa_injuries_df[name_col].isin(snp_names_set)]

        return f"""You are an expert fantasy baseball analyst for the team S&P.
You have full access to live league data as of {date.today()}.

LEAGUE RULES:
- Format: 12-team H2H Categories, Yahoo Fantasy Baseball, League ID 37872
- Scoring: R, HR, RBI, SB, OBP (hitting) | W, SV, K, ERA, WHIP (pitching)
- Roster: C×1, 1B×1, 2B×1, 3B×1, SS×1, OF×3, Util×3, SP×2, RP×2, P×5, BN×4, IL×3
- Max 7 acquisitions/week, rolling waiver priority, 30 IP minimum for pitching cats
- Trade deadline: August 6, 2026
- Playoffs: Top 6 qualify, Weeks 24–26, reseeding each round

YOUR ROLE:
- Answer any question about S&P's roster, trade targets, waiver pickups, matchup strategy, opponent scouting, or the rest of the league
- Be direct and opinionated — give a clear recommendation, not just options
- Back every recommendation with specific stats from the data below
- When evaluating trades, identify which categories are helped/hurt and whether the deal improves playoff chances
- Flag positions where S&P has 0–1 healthy active players as critical roster holes
- Never invent stats — only reference numbers present in the data

LIVE LEAGUE DATA:
{df_snippet(qa_roster_df,    "All 12 Rosters (fantasy_team_name, player_name, position, status, ops, era, war, etc.)")}
{df_snippet(qa_waiver_df,    "Waiver Wire — Top 75 Free Agents")}
{df_snippet(qa_standings_df, "League Standings")}
{df_snippet(qa_matchup_df,   "Current Week Category Stats (all teams)")}
{df_snippet(snp_inj,         "S&P Injury / IL Log")}
{df_snippet(qa_schedule_df,  "This Week's MLB Schedule + Probable Pitchers", max_rows=50)}
"""

    hdr_col, btn_col = st.columns([5, 1])
    with hdr_col:
        st.markdown("""
        <div style='margin-bottom:4px'>
          <span style='font-family:Bebas Neue,cursive;font-size:28px;letter-spacing:2px;color:#FFA110'>Roster Q&amp;A</span>
          <span style='font-family:IBM Plex Mono,monospace;font-size:10px;letter-spacing:2px;color:rgba(255,161,16,0.5);margin-left:12px;text-transform:uppercase'>Powered by Claude</span>
        </div>
        <div style='font-family:DM Sans,sans-serif;font-size:12px;color:rgba(230,237,243,0.4);margin-bottom:20px'>
          Ask anything — trade targets, waiver pickups, start/sit, opponent scouting, roster holes.
        </div>
        """, unsafe_allow_html=True)
    with btn_col:
        st.markdown("<div style='height:18px'></div>", unsafe_allow_html=True)
        if st.button("Clear chat", key="qa_clear"):
            st.session_state.qa_messages = []
            st.rerun()

    if st.session_state.qa_messages:
        bubbles_html = '<div class="chat-wrap">'
        for msg in st.session_state.qa_messages:
            content = msg["content"].replace("\n", "<br>")
            if msg["role"] == "user":
                bubbles_html += f"<div style='display:flex;flex-direction:column;align-items:flex-end'><div class='bubble-label' style='color:rgba(255,161,16,0.5);text-align:right'>You</div><div class='bubble-user'>{content}</div></div>"
            else:
                bubbles_html += f"<div style='display:flex;flex-direction:column;align-items:flex-start'><div class='bubble-label' style='color:rgba(230,237,243,0.3)'>S&P Analyst</div><div class='bubble-assistant'>{content}</div></div>"
        bubbles_html += "</div>"
        st.markdown(bubbles_html, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style='text-align:center;padding:48px 24px;border:1px dashed rgba(255,161,16,0.15);border-radius:6px;margin-bottom:16px'>
          <div style='font-family:Bebas Neue,cursive;font-size:22px;color:rgba(255,161,16,0.3);letter-spacing:2px;margin-bottom:10px'>Ask your first question</div>
          <div style='font-family:IBM Plex Mono,monospace;font-size:10px;letter-spacing:1px;color:rgba(230,237,243,0.2);line-height:2'>
            "Team X wants [player] from my roster — what can I ask for in return?"<br>
            "Who should I start at SP this week?"<br>
            "What's my weakest category and who on the wire can help?"<br>
            "Scout my opponent — where can I win this week?"
          </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    input_col, send_col = st.columns([6, 1])
    with input_col:
        user_input = st.text_input(label="question", label_visibility="collapsed", placeholder="Ask anything about your roster, trades, waivers, or matchups...", key="qa_input")
    with send_col:
        send_clicked = st.button("Ask ⚾", type="primary", use_container_width=True)

    if send_clicked and user_input.strip():
        st.session_state.qa_messages.append({"role": "user", "content": user_input.strip()})
        with st.spinner("Thinking..."):
            try:
                import anthropic
                api_key  = get_anthropic_key()
                client   = anthropic.Anthropic(api_key=api_key)
                response = client.messages.create(
                    model="claude-sonnet-4-6", max_tokens=1500,
                    system=build_qa_system_prompt(),
                    messages=[{"role": m["role"], "content": m["content"]} for m in st.session_state.qa_messages],
                )
                st.session_state.qa_messages.append({"role": "assistant", "content": response.content[0].text})
            except Exception as e:
                st.session_state.qa_messages.append({"role": "assistant", "content": f"⚠️ Error reaching Claude: {e}"})
        st.rerun()


# ═════════════════════════════════════════════════════════════════════════════
# TAB 6 — OPPONENT SCOUTING
# ═════════════════════════════════════════════════════════════════════════════

with tab6:

    # ── Load data ────────────────────────────────────────────────────────────
    sc_roster_df    = load("fantasy_roster",       "fantasy_my_roster.csv")
    sc_matchup_cats = load("fantasy_matchup_cats", "fantasy_matchup_cats.csv")
    sc_matchup_raw  = load("fantasy_matchup",      "fantasy_matchup.csv")
    sc_standings_df = load("fantasy_team_stats",   "fantasy_team_stats.csv")
    sc_schedule_df  = load("mlb_schedule",         "mlb_schedule.csv")
    sc_injuries_df  = load("mlb_injuries",         "mlb_injuries.csv")

    # ── Identify opponent ─────────────────────────────────────────────────────
    opp_name  = None
    snp_cats  = None
    opp_cats  = None
    week_label = sc_matchup_cats["week"].iloc[0] if not sc_matchup_cats.empty and "week" in sc_matchup_cats.columns else "?"

    if not sc_matchup_raw.empty:
        for _, row in sc_matchup_raw.iterrows():
            t1 = str(row.get("team_1", "")).strip("b'").strip("'")
            t2 = str(row.get("team_2", "")).strip("b'").strip("'")
            if "S&P" in t1: opp_name = t2
            elif "S&P" in t2: opp_name = t1

    if not sc_matchup_cats.empty:
        snp_rows = sc_matchup_cats[sc_matchup_cats["fantasy_team_name"].str.contains("S&P", na=False)]
        if not snp_rows.empty: snp_cats = snp_rows.iloc[0]
        if opp_name:
            opp_rows = sc_matchup_cats[sc_matchup_cats["fantasy_team_name"].str.contains(opp_name.strip(), na=False, regex=False)]
            if not opp_rows.empty: opp_cats = opp_rows.iloc[0]

    # ── Header ───────────────────────────────────────────────────────────────
    st.markdown(f"""
    <div style='margin-bottom:4px'>
      <span style='font-family:Bebas Neue,cursive;font-size:28px;letter-spacing:2px;color:#FFA110'>Opponent Scouting</span>
      <span style='font-family:IBM Plex Mono,monospace;font-size:10px;letter-spacing:2px;color:rgba(255,161,16,0.5);margin-left:12px;text-transform:uppercase'>Week {week_label}</span>
    </div>
    """, unsafe_allow_html=True)

    if not opp_name:
        st.markdown("""
        <div style='text-align:center;padding:60px;border:1px dashed rgba(255,161,16,0.15);border-radius:6px;margin-top:16px'>
          <div style='font-family:Bebas Neue,cursive;font-size:24px;color:rgba(255,161,16,0.3);letter-spacing:2px'>No matchup data found</div>
          <div style='font-family:IBM Plex Mono,monospace;font-size:11px;color:rgba(230,237,243,0.2);margin-top:8px'>Run the pipeline to load current week matchup</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        # ── Opponent identity bar ─────────────────────────────────────────────
        st.markdown(f"""
        <div style='background:#0D1117;border:1px solid rgba(255,161,16,0.2);border-radius:4px;
                    padding:16px 24px;margin-bottom:20px;display:flex;align-items:center;gap:16px'>
          <div style='font-family:IBM Plex Mono,monospace;font-size:10px;letter-spacing:2px;color:rgba(255,161,16,0.5)'>THIS WEEK VS</div>
          <div style='font-family:Bebas Neue,cursive;font-size:26px;letter-spacing:2px;color:#E6EDF3'>{opp_name}</div>
        </div>
        """, unsafe_allow_html=True)

        # ── Category win/loss pills ───────────────────────────────────────────
        if snp_cats is not None and opp_cats is not None:
            st.markdown('<div class="panel-title">CURRENT CATEGORY STANDING</div>', unsafe_allow_html=True)

            cats    = ["Runs", "HR", "RBI", "SB", "OBP", "Wins", "Saves", "K", "ERA", "WHIP"]
            abbrevs = ["R",    "HR", "RBI", "SB", "OBP", "W",    "SV",    "K", "ERA", "WHIP"]
            win_cats = []; loss_cats = []; tie_cats = []

            for cat, abbr in zip(cats, abbrevs):
                try:
                    sv = float(snp_cats.get(cat, None))
                    ov = float(opp_cats.get(cat, None))
                except (TypeError, ValueError):
                    tie_cats.append(abbr); continue
                if sv == ov: tie_cats.append(abbr)
                elif cat in ["ERA", "WHIP"]: (win_cats if sv < ov else loss_cats).append(abbr)
                else: (win_cats if sv > ov else loss_cats).append(abbr)

            def pill(cat, color, bg, border):
                return f"<span class='cat-pill' style='background:{bg};color:{color};border:1px solid {border}'>{cat}</span>"

            pills = ""
            if win_cats:
                pills += "<div style='margin-bottom:10px'><span style='font-family:IBM Plex Mono,monospace;font-size:9px;letter-spacing:1px;color:rgba(255,161,16,0.6)'>WINNING &nbsp;</span>"
                pills += "".join(pill(c, "#FFA110", "rgba(255,161,16,0.1)", "rgba(255,161,16,0.4)") for c in win_cats) + "</div>"
            if loss_cats:
                pills += "<div style='margin-bottom:10px'><span style='font-family:IBM Plex Mono,monospace;font-size:9px;letter-spacing:1px;color:rgba(255,80,80,0.7)'>LOSING &nbsp;</span>"
                pills += "".join(pill(c, "rgba(255,100,100,0.9)", "rgba(255,80,80,0.08)", "rgba(255,80,80,0.3)") for c in loss_cats) + "</div>"
            if tie_cats:
                pills += "<div style='margin-bottom:10px'><span style='font-family:IBM Plex Mono,monospace;font-size:9px;letter-spacing:1px;color:rgba(230,237,243,0.3)'>TIED &nbsp;</span>"
                pills += "".join(pill(c, "rgba(230,237,243,0.3)", "rgba(255,255,255,0.03)", "rgba(255,255,255,0.1)") for c in tie_cats) + "</div>"

            st.markdown(f"<div style='margin-bottom:24px'>{pills}</div>", unsafe_allow_html=True)

        # ── Opponent roster ───────────────────────────────────────────────────
        opp_roster = pd.DataFrame()
        if not sc_roster_df.empty:
            opp_roster = sc_roster_df[sc_roster_df["fantasy_team_name"].str.contains(opp_name.strip(), na=False, regex=False)]

        if not opp_roster.empty:
            st.markdown('<div class="panel-title">OPPONENT ROSTER</div>', unsafe_allow_html=True)
            disp_cols = [c for c in ["player_name", "position", "status", "ops", "era", "war_bat", "war_pitch"] if c in opp_roster.columns]
            st.dataframe(
                opp_roster[disp_cols].rename(columns={"player_name": "Player", "position": "Pos", "status": "Status", "ops": "OPS", "era": "ERA", "war_bat": "WAR(B)", "war_pitch": "WAR(P)"}),
                hide_index=True, use_container_width=True, height=300,
            )

        # ── Generate scouting report ──────────────────────────────────────────
        st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

        if st.button("🕵️ Generate Scouting Report", type="primary"):
            with st.spinner(f"Scouting {opp_name}..."):
                try:
                    import anthropic

                    def df_to_str(df, label, max_rows=100):
                        if df is None or df.empty: return f"[{label}: no data]\n"
                        return f"### {label}\n{df.head(max_rows).to_string(index=False)}\n\n"

                    snp_full = snp_roster(sc_roster_df) if not sc_roster_df.empty else pd.DataFrame()

                    opp_injuries = pd.DataFrame()
                    if not sc_injuries_df.empty and not opp_roster.empty:
                        opp_names = set(opp_roster["player_name"].tolist()) if "player_name" in opp_roster.columns else set()
                        name_col  = next((c for c in ["player_name", "Name", "name"] if c in sc_injuries_df.columns), None)
                        if name_col and opp_names:
                            opp_injuries = sc_injuries_df[sc_injuries_df[name_col].isin(opp_names)]

                    prompt = f"""Today is {date.today()}. Generate a detailed scouting report on my Week {week_label} opponent: {opp_name}.

MY TEAM (S&P):
{df_to_str(snp_full, "S&P Roster + Stats")}

CURRENT WEEK CATEGORY STATS (all teams):
{df_to_str(sc_matchup_cats, "Category Standings")}

OPPONENT ROSTER ({opp_name}):
{df_to_str(opp_roster, "Opponent Roster + Stats")}

OPPONENT INJURIES/IL:
{df_to_str(opp_injuries, "Opponent IL")}

LEAGUE STANDINGS:
{df_to_str(sc_standings_df, "Standings")}

THIS WEEK'S SCHEDULE:
{df_to_str(sc_schedule_df, "MLB Schedule", max_rows=50)}

Produce a structured scouting report with these sections:

1. OPPONENT OVERVIEW — Who are they, where do they stand, what's their roster identity (power hitters? ace pitching? speed?)

2. THEIR STRENGTHS — Which of the 10 categories (R, HR, RBI, SB, OBP, W, SV, K, ERA, WHIP) are they dominant in? Name specific players driving those categories with stats.

3. THEIR WEAKNESSES — Which categories are they vulnerable in this week? Where can S&P steal wins?

4. PLAYERS TO WATCH — Their 3 most dangerous players I need to be aware of, and why.

5. S&P GAME PLAN — Specific strategic advice for this matchup. Which categories should S&P target to win? Any lineup decisions, streaming moves, or waiver adds that would help this week specifically?

6. PREDICTED OUTCOME — Your honest category-by-category prediction and final score estimate (e.g. 6-4 S&P).

Be direct, specific, and back everything with the stats in the data. Don't hedge."""

                    api_key = get_anthropic_key()
                    client  = anthropic.Anthropic(api_key=api_key)
                    message = client.messages.create(
                        model="claude-sonnet-4-6",
                        max_tokens=2000,
                        system="You are an elite fantasy baseball scout analyzing weekly H2H matchups for team S&P in a 12-team categories league. Scoring: R, HR, RBI, SB, OBP (hitting) | W, SV, K, ERA, WHIP (pitching). Be direct, opinionated, and back every claim with stats from the data provided.",
                        messages=[{"role": "user", "content": prompt}],
                    )
                    report = message.content[0].text

                    st.markdown(f"""
                    <div style='background:#0D1117;border:1px solid rgba(255,161,16,0.25);
                                border-radius:4px;padding:28px 32px;margin-top:8px'>
                      <div style='font-family:IBM Plex Mono,monospace;font-size:10px;letter-spacing:2px;
                                  color:rgba(255,161,16,0.7);margin-bottom:20px'>
                        ● SCOUTING REPORT — {opp_name.upper()} — WEEK {week_label}
                      </div>
                      <div style='font-size:13px;line-height:1.85;color:rgba(230,237,243,0.85);font-family:DM Sans,sans-serif'>
                        {report.replace(chr(10), '<br>')}
                      </div>
                    </div>
                    """, unsafe_allow_html=True)

                except Exception as e:
                    st.error(f"Scouting report failed: {e}")
