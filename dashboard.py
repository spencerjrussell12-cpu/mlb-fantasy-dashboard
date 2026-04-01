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

# Local CSV fallback (used when Sheets is unavailable)
DATA_DIR = "/Users/spencerrussell/OneDrive - G&G Outfitters/power_bi_data"

# Google Sheets config — reads from Streamlit secrets when deployed,
# falls back to local credentials file when running locally
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
    color: rgba(230,237,243,0.5);
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
                creds = Credentials.from_service_account_info(
                    st.secrets["gcp_service_account"],
                    scopes=SCOPES,
                )
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
def load_from_sheets(tab_name: str) -> pd.DataFrame:
    """Load a tab from Google Sheets as a DataFrame."""
    sheet, status = get_sheets_client()
    if sheet is None:
        return pd.DataFrame(), "local"
    try:
        ws   = sheet.worksheet(tab_name)
        data = ws.get_all_records()
        df   = pd.DataFrame(data)
        # Replace empty strings with NaN for numeric columns
        df = df.replace("", pd.NA)
        return df, "sheets"
    except Exception:
        return pd.DataFrame(), "local"


@st.cache_data(ttl=300)
def load_from_csv(filename: str) -> pd.DataFrame:
    """Load a local CSV as a DataFrame."""
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path)


def load(tab_name: str, csv_filename: str = None) -> pd.DataFrame:
    """
    Smart loader: tries Google Sheets first, falls back to local CSV.
    tab_name    — name of the Google Sheets worksheet tab
    csv_filename — local CSV filename (defaults to tab_name + .csv)
    """
    if csv_filename is None:
        csv_filename = f"{tab_name}.csv"

    df, source = load_from_sheets(tab_name)

    if df.empty:
        df = load_from_csv(csv_filename)
        return df

    return df


@st.cache_data(ttl=300)
def load_metadata() -> dict:
    """Load the metadata tab from Sheets to show last updated time."""
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
    """Load claude insights from Google Sheets, fallback to local JSON."""
    sheet, _ = get_sheets_client()
    if sheet:
        try:
            ws   = sheet.worksheet("claude_insights")
            rows = ws.get_all_records()
            data = {r["key"]: r["value"] for r in rows}
            return {
                "generated_at":  data.get("generated_at", ""),
                "weekly_summary": data.get("weekly_summary", ""),
                "starters":      data.get("starters", ""),
                "waiver_wire":   data.get("waiver_wire", ""),
                "trade_analysis": {
                    "give":     data.get("trade_give", ""),
                    "receive":  data.get("trade_receive", ""),
                    "analysis": data.get("trade_analysis", ""),
                },
            }
        except Exception:
            pass

    # fallback to local JSON
    path = os.path.join(DATA_DIR, "claude_insights.json")
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)


def snp_roster(df):
    if "fantasy_team_name" in df.columns:
        return df[df["fantasy_team_name"].str.contains("S&P", na=False)]
    return df


# ─────────────────────────────────────────────────────────────────────────────
# DATA SOURCE STATUS
# ─────────────────────────────────────────────────────────────────────────────

_, sheets_status = get_sheets_client()
using_sheets     = sheets_status == "sheets"
meta             = load_metadata()
last_updated     = meta.get("last_updated", "Unknown")

# ─────────────────────────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────────────────────────

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
      <div style="font-family:'Bebas Neue',cursive;font-size:22px;letter-spacing:3px;color:#FFA110">
        S&P Analytics
      </div>
      <div style="font-size:10px;color:rgba(230,237,243,0.35);letter-spacing:2px;text-transform:uppercase">
        MLB Fantasy Dashboard
      </div>
    </div>
  </div>
  <div style="display:flex;align-items:center;gap:16px">
    {source_badge}
    <div style="font-family:'IBM Plex Mono',monospace;font-size:11px;
                color:rgba(230,237,243,0.35);letter-spacing:1px">
      {date.today().strftime('%b %d, %Y').upper()}
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

if last_updated != "Unknown":
    st.markdown(f"""
    <div style="font-family:'IBM Plex Mono',monospace;font-size:9px;letter-spacing:1px;
                color:rgba(230,237,243,0.2);text-align:right;padding:6px 0 0 0">
      Data refreshed: {last_updated}
    </div>
    """, unsafe_allow_html=True)

st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4 = st.tabs([
    "⚾  Fantasy Hub",
    "📊  League Overview",
    "🤖  AI Insights",
    "🔍  Player Deep Dive",
])

# ═════════════════════════════════════════════════════════════════════════════
# TAB 1 — FANTASY HUB
# ═════════════════════════════════════════════════════════════════════════════

with tab1:
    matchup_df   = load("fantasy_matchup",      "fantasy_matchup.csv")
    roster_df    = load("fantasy_roster",        "fantasy_my_roster.csv")
    roster_stats = load("fantasy_roster_stats",  "fantasy_roster_stats.csv")
    injuries_df  = load("mlb_injuries",          "mlb_injuries.csv")

    # ── Scoreboard ────────────────────────────────────────────────────────────
    week_num = matchup_df["week"].iloc[0] if not matchup_df.empty and "week" in matchup_df.columns else "?"
    my_row = None

    if not matchup_df.empty:
        for _, row in matchup_df.iterrows():
            t1 = str(row.get("team_1", "")).strip("b'").strip("'")
            t2 = str(row.get("team_2", "")).strip("b'").strip("'")
            if "S&P" in t1:
                my_row = {"name": t1, "score": row.get("team_1_score", 0),
                          "opp": t2, "opp_score": row.get("team_2_score", 0)}
            elif "S&P" in t2:
                my_row = {"name": t2, "score": row.get("team_2_score", 0),
                          "opp": t1, "opp_score": row.get("team_1_score", 0)}

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
                  <div style="font-family:'IBM Plex Mono',monospace;font-size:11px;
                              color:rgba(230,237,243,0.3);letter-spacing:2px;margin-bottom:8px">{outlook}</div>
                  <div style="font-family:'IBM Plex Mono',monospace;font-size:13px;
                              color:rgba(230,237,243,0.2);letter-spacing:3px">VS</div>
                </div>
                <div style="text-align:right">
                  <div class="team-label-opp">{my_row['opp']}</div>
                  <div class="score-opp">{int(opp_score)}</div>
                </div>
              </div>
            </div>
            """, unsafe_allow_html=True)


    # ── Category breakdown ─────────────────────────────────────────────────────
    matchup_cats = load("fantasy_matchup_cats", "fantasy_matchup_cats.csv")

    if not matchup_cats.empty and my_row:
        # Coerce numerics
        cats      = ["Runs", "HR", "RBI", "SB", "OBP", "Wins", "Saves", "K", "ERA", "WHIP"]
        abbrevs   = ["R",    "HR", "RBI", "SB", "OBP", "W",    "SV",    "K", "ERA", "WHIP"]

        for cat in cats:
            if cat in matchup_cats.columns:
                matchup_cats[cat] = pd.to_numeric(matchup_cats[cat], errors="coerce")

        # Find S&P and opponent rows
        snp_row = matchup_cats[matchup_cats["fantasy_team_name"].str.contains("S&P", na=False)]
        opp_row = matchup_cats[matchup_cats["fantasy_team_name"].str.contains(my_row["opp"].strip(), na=False, regex=False)]

        if not snp_row.empty and not opp_row.empty:
            snp = snp_row.iloc[0]
            opp = opp_row.iloc[0]
            
            # force numeric
            for cat in cats:
                try:
                    snp[cat] = float(snp[cat])
                except:
                    snp[cat] = None
                try:
                    opp[cat] = float(opp[cat])
                except:
                    opp[cat] = None

            def fmt(val, cat):
                if val is None or pd.isna(val):
                    return "—"
                return f"{val:.2f}" if cat in ["ERA", "WHIP", "OBP"] else str(int(val))

            def snp_wins(cat, sv, ov):
                if pd.isna(sv) or pd.isna(ov):
                    return None
                if sv == ov:
                    return None  # tied — no highlighting
                if cat in ["ERA", "WHIP"]:
                    return sv < ov
                return sv > ov

            # header row
            header_cols = st.columns([1] + [1]*10)
            with header_cols[0]:
                st.markdown(f"<div style='font-family:IBM Plex Mono,monospace;font-size:9px;letter-spacing:1px;color:rgba(230,237,243,0.3);padding-top:8px'>WEEK {week_num}</div>", unsafe_allow_html=True)
            for i, abbr in enumerate(abbrevs):
                with header_cols[i+1]:
                    st.markdown(f"<div style='font-family:IBM Plex Mono,monospace;font-size:9px;letter-spacing:1px;color:rgba(230,237,243,0.4);text-align:center'>{abbr}</div>", unsafe_allow_html=True)

            # S&P row
            snp_cols = st.columns([1] + [1]*10)
            with snp_cols[0]:
                st.markdown("<div style='font-family:Bebas Neue,cursive;font-size:16px;color:#FFA110;padding-top:4px'>S&P</div>", unsafe_allow_html=True)
            for i, cat in enumerate(cats):
                sv  = snp.get(cat)
                ov  = opp.get(cat)
                win = snp_wins(cat, sv, ov)
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

            # opponent row
            opp_name = my_row["opp"]
            opp_cols = st.columns([1] + [1]*10)
            with opp_cols[0]:
                st.markdown(f"<div style='font-family:Bebas Neue,cursive;font-size:16px;color:rgba(230,237,243,0.5);padding-top:4px'>{opp_name[:8]}</div>", unsafe_allow_html=True)
            for i, cat in enumerate(cats):
                sv  = snp.get(cat)
                ov  = opp.get(cat)
                win = snp_wins(cat, sv, ov)
                if win is False:
                    style = "background:rgba(255,80,80,0.08);border:1px solid rgba(255,80,80,0.3);border-radius:3px;padding:6px 4px;text-align:center"
                    color = "rgba(255,120,120,0.9)"; weight = "700"
                else:
                    style = "background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.06);border-radius:3px;padding:6px 4px;text-align:center"
                    color = "rgba(230,237,243,0.3)"; weight = "400"
                with opp_cols[i+1]:
                    st.markdown(f"<div style='{style}'><span style='font-family:Bebas Neue,cursive;font-size:18px;color:{color};font-weight:{weight};line-height:1'>{fmt(ov, cat)}</span></div>", unsafe_allow_html=True)

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

    # ── Roster + Waiver ────────────────────────────────────────────────────────
    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.markdown('<div class="panel-title">S&P ROSTER</div>', unsafe_allow_html=True)
        if not roster_df.empty:
            snp = snp_roster(roster_df)
            display_cols = [c for c in ["player_name", "position", "status", "ops", "era", "war_bat", "war_pitch"] if c in snp.columns]
            st.dataframe(
                snp[display_cols].rename(columns={
                    "player_name": "Player", "position": "Pos",
                    "status": "Status", "ops": "OPS",
                    "era": "ERA", "war_bat": "WAR(B)", "war_pitch": "WAR(P)"
                }),
                hide_index=True, use_container_width=True, height=420,
            )

    with col_right:
        st.markdown('<div class="panel-title">WAIVER WIRE</div>', unsafe_allow_html=True)
        waiver_df = load("fantasy_waiver_wire", "fantasy_waiver_wire.csv")
        if not waiver_df.empty:
            display_cols = [c for c in ["player_name", "position", "team_x", "ops", "era", "wrc_plus", "stolen_bases", "saves"] if c in waiver_df.columns]
            st.dataframe(
                waiver_df[display_cols].rename(columns={
                    "player_name": "Player", "position": "Pos",
                    "team_x": "Team", "ops": "OPS", "era": "ERA",
                    "wrc_plus": "wRC+", "stolen_bases": "SB", "saves": "SV"
                }),
                hide_index=True, use_container_width=True, height=420,
            )

    # ── Injury tracker ─────────────────────────────────────────────────────────
    st.markdown('<div class="panel-title" style="margin-top:20px">INJURY TRACKER</div>', unsafe_allow_html=True)
    if not injuries_df.empty:
        snp_names = snp_roster(roster_df)["player_name"].tolist() if not roster_df.empty else []
        il_df = injuries_df[injuries_df["player_name"].isin(snp_names)] if snp_names else injuries_df.head(20)
        if not il_df.empty:
            st.dataframe(
                il_df[["player_name", "transaction_type", "description", "date"]].rename(columns={
                    "player_name": "Player", "transaction_type": "Type",
                    "description": "Description", "date": "Date"
                }),
                hide_index=True, use_container_width=True,
            )
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

    # Coerce numeric
    for col in ["ops", "home_runs", "wrc_plus"]:
        if not batting_df.empty and col in batting_df.columns:
            batting_df[col] = pd.to_numeric(batting_df[col], errors="coerce")
    for col in ["era", "whip"]:
        if not pitching_df.empty and col in pitching_df.columns:
            pitching_df[col] = pd.to_numeric(pitching_df[col], errors="coerce")

    if not batting_df.empty:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Avg OPS", f"{batting_df['ops'].mean():.3f}" if "ops" in batting_df.columns else "—")
        with col2:
            st.metric("Total HRs", f"{int(batting_df['home_runs'].sum())}" if "home_runs" in batting_df.columns else "—")
        with col3:
            st.metric("Avg wRC+", f"{batting_df['wrc_plus'].mean():.0f}" if "wrc_plus" in batting_df.columns else "—")
        with col4:
            st.metric("Avg ERA", f"{pitching_df['era'].mean():.2f}" if not pitching_df.empty and "era" in pitching_df.columns else "—")

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown('<div class="panel-title">TOP 15 BATTERS BY OPS</div>', unsafe_allow_html=True)
        if not batting_df.empty and "ops" in batting_df.columns:
            top_bat = batting_df.nlargest(15, "ops")[["player_name", "ops", "home_runs", "wrc_plus"]].dropna()
            fig = px.bar(
                top_bat, x="ops", y="player_name", orientation="h",
                color="ops", color_continuous_scale=["#161B22", "#FFA110"],
                labels={"ops": "OPS", "player_name": ""},
            )
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#E6EDF3", family="IBM Plex Mono", size=11),
                coloraxis_showscale=False, margin=dict(l=0, r=0, t=0, b=0), height=420,
                yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
            )
            fig.update_traces(marker_line_width=0)
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.markdown('<div class="panel-title">TOP 15 PITCHERS BY ERA (min 20 IP)</div>', unsafe_allow_html=True)
        if not pitching_df.empty and "era" in pitching_df.columns:
            top_pit = pitching_df.nsmallest(15, "era")[["player_name", "era", "whip", "k_per_9"]].dropna()
            fig2 = px.bar(
                top_pit, x="era", y="player_name", orientation="h",
                color="era", color_continuous_scale=["#FFA110", "#161B22"],
                labels={"era": "ERA", "player_name": ""},
            )
            fig2.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#E6EDF3", family="IBM Plex Mono", size=11),
                coloraxis_showscale=False, margin=dict(l=0, r=0, t=0, b=0), height=420,
                yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
            )
            fig2.update_traces(marker_line_width=0)
            st.plotly_chart(fig2, use_container_width=True)

    st.markdown('<div class="panel-title" style="margin-top:8px">LEAGUE STANDINGS</div>', unsafe_allow_html=True)
    if not team_stats.empty:
        display_cols = [c for c in ["fantasy_team_name", "wins", "losses", "ties", "points", "standing"] if c in team_stats.columns]
        st.dataframe(
            team_stats[display_cols].rename(columns={
                "fantasy_team_name": "Team", "wins": "W", "losses": "L",
                "ties": "T", "points": "Pts", "standing": "Rank"
            }),
            hide_index=True, use_container_width=True,
        )


# ═════════════════════════════════════════════════════════════════════════════
# TAB 3 — AI INSIGHTS
# ═════════════════════════════════════════════════════════════════════════════

with tab3:
    insights = load_insights()

    if not insights:
        st.markdown("""
        <div style='text-align:center;padding:60px;color:rgba(230,237,243,0.3)'>
            <div style='font-family:Bebas Neue,cursive;font-size:32px;color:rgba(255,161,16,0.4);margin-bottom:12px'>
                No Insights Yet
            </div>
            <div style='font-family:IBM Plex Mono,monospace;font-size:12px;letter-spacing:1px'>
                Run claude_insights.py to generate recommendations
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        generated = insights.get("generated_at", "Unknown")
        st.markdown(f"""
        <div style='display:flex;align-items:center;gap:12px;margin-bottom:24px'>
          <div style='width:8px;height:8px;background:#FFA110;border-radius:50%'></div>
          <div style='font-family:IBM Plex Mono,monospace;font-size:10px;letter-spacing:2px;
                      color:rgba(255,161,16,0.7);text-transform:uppercase'>
            Claude AI — Last updated {generated}
          </div>
        </div>
        """, unsafe_allow_html=True)

        sections = [
            ("📋  WEEKLY SUMMARY",             "weekly_summary"),
            ("⚾  START / SIT RECOMMENDATIONS", "starters"),
            ("📡  WAIVER WIRE",                 "waiver_wire"),
        ]

        for title, key in sections:
            content = insights.get(key, "")
            if content and not content.startswith("Error"):
                with st.expander(title, expanded=True):
                    st.markdown(f"""
                    <div style='font-size:13px;line-height:1.8;color:rgba(230,237,243,0.8);
                                font-family:DM Sans,sans-serif'>
                    {content.replace(chr(10), '<br>')}
                    </div>
                    """, unsafe_allow_html=True)

        trade = insights.get("trade_analysis", {})
        if trade and "analysis" in trade:
            with st.expander("🔄  TRADE ANALYSIS", expanded=False):
                st.markdown(f"""
                <div style='margin-bottom:16px;padding:12px 16px;background:rgba(255,161,16,0.08);
                            border:1px solid rgba(255,161,16,0.2);border-radius:4px'>
                  <span style='font-family:IBM Plex Mono,monospace;font-size:10px;
                               color:rgba(255,161,16,0.7);letter-spacing:2px'>GIVE</span>
                  <span style='font-family:Bebas Neue,cursive;font-size:18px;
                               color:#FFA110;margin:0 16px'>{trade.get('give','')}</span>
                  <span style='font-family:IBM Plex Mono,monospace;font-size:10px;
                               color:rgba(255,161,16,0.7);letter-spacing:2px'>RECEIVE</span>
                  <span style='font-family:Bebas Neue,cursive;font-size:18px;
                               color:#FFA110;margin-left:16px'>{trade.get('receive','')}</span>
                </div>
                <div style='font-size:13px;line-height:1.8;color:rgba(230,237,243,0.8)'>
                {trade['analysis'].replace(chr(10), '<br>')}
                </div>
                """, unsafe_allow_html=True)

    # ── Live Trade Analyzer ────────────────────────────────────────────────────
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
                    from dotenv import load_dotenv
                    load_dotenv("/Users/spencerrussell/mlb_fantasy_dashboard/.env")

                    roster_raw   = load("fantasy_roster",       "fantasy_my_roster.csv")
                    batting_raw  = load("mlb_batting_season",   "mlb_batting_season.csv")
                    pitching_raw = load("mlb_pitching_season",  "mlb_pitching_season.csv")
                    r_stats_raw  = load("fantasy_roster_stats", "fantasy_roster_stats.csv")

                    # Coerce numerics
                    for col in ["ops", "home_runs", "wrc_plus"]:
                        if col in batting_raw.columns:
                            batting_raw[col] = pd.to_numeric(batting_raw[col], errors="coerce")
                    for col in ["era", "whip"]:
                        if col in pitching_raw.columns:
                            pitching_raw[col] = pd.to_numeric(pitching_raw[col], errors="coerce")

                    snp      = snp_roster(roster_raw)
                    snp_stats = snp_roster(r_stats_raw)

                    prompt = f"""
Today is {date.today()}. Analyze this potential trade for my fantasy team S&P:

MY TEAM GIVES: {trade_give}
MY TEAM RECEIVES: {trade_receive}

MY CURRENT ROSTER:
{snp.to_string(index=False)}

MY CURRENT WEEKLY CATEGORY STANDINGS:
{snp_stats.head(30).to_string(index=False)}

SEASON BATTING STATS (top 50):
{batting_raw.head(50).to_string(index=False)}

SEASON PITCHING STATS (top 50):
{pitching_raw.head(50).to_string(index=False)}

Remember this is a 10-category H2H league (R, HR, RBI, SB, OBP, W, SV, K, ERA, WHIP).

1. VERDICT: Accept / Decline / Counter
2. CATEGORY IMPACT — for each of the 10 categories, does this trade help, hurt, or neutral?
3. ROSTER FIT — how does this change my team construction?
4. COUNTER OFFER — if declining, what would make this trade fair?
5. LONG TERM VIEW — does this help me for the playoffs (top 6 qualify, weeks 24-26)?
"""
                    system = """You are an elite fantasy baseball analyst managing S&P in a 12-team H2H Categories league.
Scoring categories: R, HR, RBI, SB, OBP (hitting) and W, SV, K, ERA, WHIP (pitching).
Be direct, back every recommendation with stats, never make up numbers not in the data."""

                    api_key = os.getenv("ANTHROPIC_API_KEY")
                    if not api_key and "anthropic" in st.secrets:
                        api_key = st.secrets["anthropic"]["api_key"]

                    client  = anthropic.Anthropic(api_key=api_key)
                    message = client.messages.create(
                        model="claude-sonnet-4-6",
                        max_tokens=1500,
                        system=system,
                        messages=[{"role": "user", "content": prompt}],
                    )
                    result = message.content[0].text

                    st.markdown(f"""
                    <div style='background:#0D1117;border:1px solid rgba(255,161,16,0.25);
                                border-radius:4px;padding:24px;margin-top:16px'>
                      <div style='font-family:IBM Plex Mono,monospace;font-size:10px;
                                  letter-spacing:2px;color:rgba(255,161,16,0.7);margin-bottom:16px'>
                        ● TRADE ANALYSIS — {trade_give.upper()} FOR {trade_receive.upper()}
                      </div>
                      <div style='font-size:13px;line-height:1.8;color:rgba(230,237,243,0.8)'>
                        {result.replace(chr(10), '<br>')}
                      </div>
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

    # Coerce numerics
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

    selected = st.selectbox(
        "Search for any MLB player",
        options=[""] + all_players,
        index=0,
        placeholder="Type a player name..."
    )

    if selected:
        st.markdown(f"""
        <div style='margin-bottom:24px'>
          <span style='font-family:Bebas Neue,cursive;font-size:36px;
                       letter-spacing:2px;color:#FFA110'>{selected}</span>
        </div>
        """, unsafe_allow_html=True)

        col1, col2 = st.columns(2)

        with col1:
            bat_row = batting_df[batting_df["player_name"] == selected] if not batting_df.empty else pd.DataFrame()
            if not bat_row.empty:
                st.markdown('<div class="panel-title">BATTING STATS</div>', unsafe_allow_html=True)
                bat_display = bat_row.T.reset_index()
                bat_display.columns = ["Stat", "Value"]
                bat_display = bat_display[~bat_display["Stat"].isin(["player_name", "season", "stat_type"])]
                st.dataframe(bat_display, hide_index=True, use_container_width=True)

        with col2:
            pit_row = pitching_df[pitching_df["player_name"] == selected] if not pitching_df.empty else pd.DataFrame()
            if not pit_row.empty:
                st.markdown('<div class="panel-title">PITCHING STATS</div>', unsafe_allow_html=True)
                pit_display = pit_row.T.reset_index()
                pit_display.columns = ["Stat", "Value"]
                pit_display = pit_display[~pit_display["Stat"].isin(["player_name", "season", "stat_type"])]
                st.dataframe(pit_display, hide_index=True, use_container_width=True)

        if bat_row.empty and pit_row.empty:
            st.markdown("<p style='color:rgba(230,237,243,0.4)'>No stats found for this player.</p>", unsafe_allow_html=True)

        st.markdown('<div class="panel-title" style="margin-top:20px">INJURY HISTORY</div>', unsafe_allow_html=True)
        if not injuries_df.empty:
            player_il = injuries_df[injuries_df["player_name"] == selected]
            if not player_il.empty:
                st.dataframe(
                    player_il[["transaction_type", "description", "date"]].rename(columns={
                        "transaction_type": "Type", "description": "Description", "date": "Date"
                    }),
                    hide_index=True, use_container_width=True,
                )
            else:
                st.markdown("<p style='color:rgba(230,237,243,0.4);font-size:13px'>No injury records found.</p>", unsafe_allow_html=True)

        st.markdown('<div class="panel-title" style="margin-top:20px">UPCOMING SCHEDULE</div>', unsafe_allow_html=True)
        if not schedule_df.empty:
            player_team = None
            if not bat_row.empty and "team" in bat_row.columns:
                player_team = bat_row["team"].iloc[0]
            elif not pit_row.empty and "team" in pit_row.columns:
                player_team = pit_row["team"].iloc[0]

            if player_team:
                player_games = schedule_df[
                    (schedule_df["away_team"].str.contains(str(player_team), na=False, case=False)) |
                    (schedule_df["home_team"].str.contains(str(player_team), na=False, case=False))
                ]
            else:
                player_games = schedule_df[
                    (schedule_df.get("away_pitcher", pd.Series()) == selected) |
                    (schedule_df.get("home_pitcher", pd.Series()) == selected)
                ] if "away_pitcher" in schedule_df.columns else pd.DataFrame()

            if not player_games.empty:
                st.dataframe(
                    player_games[["game_date", "away_team", "home_team", "away_pitcher", "home_pitcher", "venue", "status"]].rename(columns={
                        "game_date": "Date", "away_team": "Away", "home_team": "Home",
                        "away_pitcher": "Away SP", "home_pitcher": "Home SP",
                        "venue": "Venue", "status": "Status"
                    }),
                    hide_index=True, use_container_width=True,
                )
            else:
                st.markdown("<p style='color:rgba(230,237,243,0.4);font-size:13px'>No upcoming games found.</p>", unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style='text-align:center;padding:60px;color:rgba(230,237,243,0.2)'>
          <div style='font-family:Bebas Neue,cursive;font-size:48px;color:rgba(255,161,16,0.15)'>
            Search any player
          </div>
          <div style='font-family:IBM Plex Mono,monospace;font-size:11px;letter-spacing:2px;margin-top:8px'>
            537 batters · 542 pitchers
          </div>
        </div>
        """, unsafe_allow_html=True)
