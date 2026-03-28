#!/usr/bin/env python3
"""
Build a self-contained interactive HTML dashboard for 1P O1.5 projections.

The dashboard embeds a snapshot of the dataset produced by project_1p_two_plus.py,
so it can be opened directly in a browser as a single file.
"""

import argparse
import json
import os
import re
from pathlib import Path
from urllib.parse import quote

import project_1p_two_plus as proj


HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>NHL 1P O1.5 Interactive Dashboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #f7f4ee;
      --paper: #fffdf8;
      --ink: #1f2933;
      --muted: #5f6b76;
      --line: #d7d2c8;
      --accent: #1d7f8c;
      --accent-soft: #d8f1f4;
      --good: #1f8f5f;
      --good-bg: #dff5ea;
      --warn: #b97400;
      --warn-bg: #fff1d6;
      --bad: #b23b3b;
      --bad-bg: #fde3e3;
      --radius: 16px;
      --shadow: 0 8px 20px rgba(25, 32, 36, 0.08);
    }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Space Grotesk", "Avenir Next", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 14% 10%, #f9e4c8 0%, transparent 35%),
        radial-gradient(circle at 88% 14%, #d7f0f4 0%, transparent 34%),
        linear-gradient(180deg, #fbf8f2 0%, #f3efe7 100%);
      min-height: 100vh;
    }

    .shell {
      width: min(1420px, 96vw);
      margin: 10px auto 12px;
      display: grid;
      gap: 10px;
    }

    .hero {
      background: linear-gradient(135deg, #fffef9, #f8f3e8);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 12px 14px 10px;
    }

    .hero h1 {
      margin: 0;
      font-size: clamp(1.05rem, 1.5vw, 1.35rem);
      letter-spacing: 0.01em;
    }

    .hero p {
      margin: 4px 0 0;
      color: var(--muted);
      font-size: 0.84rem;
    }

    .workspace {
      display: grid;
      grid-template-columns: minmax(340px, 430px) minmax(0, 1fr);
      gap: 10px;
      align-items: start;
    }

    .slate-panel,
    .detail-panel {
      max-height: calc(100vh - 86px);
      overflow-y: auto;
      scrollbar-width: thin;
    }

    .detail-panel {
      display: grid;
      gap: 10px;
    }

    .controls {
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 10px;
      display: grid;
      grid-template-columns: repeat(5, minmax(120px, 1fr));
      gap: 8px;
    }

    .field {
      display: grid;
      gap: 6px;
    }

    .label {
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
      font-weight: 700;
    }

    select, button {
      width: 100%;
      border: 1px solid #c9c1b3;
      border-radius: 12px;
      background: #fff;
      color: var(--ink);
      font-size: 0.88rem;
      padding: 8px 9px;
      font-family: inherit;
    }

    select:focus, button:focus {
      outline: 2px solid #6cb8c2;
      outline-offset: 1px;
    }

    button {
      background: #ebf6f8;
      border-color: #b7dce2;
      font-weight: 600;
      cursor: pointer;
      align-self: end;
    }

    .kpis {
      display: grid;
      grid-template-columns: repeat(4, minmax(130px, 1fr));
      gap: 8px;
    }

    .card {
      background: var(--paper);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 10px;
    }

    .kpi-title {
      font-size: 0.72rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
      margin-bottom: 6px;
      font-weight: 700;
    }

    .kpi-value {
      font-size: clamp(1rem, 2vw, 1.5rem);
      font-weight: 700;
      line-height: 1.05;
    }

    .mono {
      font-family: "IBM Plex Mono", monospace;
      font-size: 0.9rem;
    }

    .tag {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 2px 9px;
      font-size: 0.76rem;
      font-weight: 700;
      letter-spacing: 0.01em;
      border: 1px solid transparent;
      white-space: nowrap;
    }

    .good { color: var(--good); background: var(--good-bg); border-color: #bce7d3; }
    .warn { color: var(--warn); background: var(--warn-bg); border-color: #f4d497; }
    .bad { color: var(--bad); background: var(--bad-bg); border-color: #f2b7b7; }

    .grid-two {
      display: grid;
      grid-template-columns: repeat(2, minmax(220px, 1fr));
      gap: 8px;
    }

    .section-title {
      margin: 0 0 6px;
      font-size: 0.9rem;
      letter-spacing: 0.01em;
    }

    .rankings-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      flex-wrap: wrap;
      margin-bottom: 6px;
    }

    .rankings-wrap {
      border: 1px solid #d8d1c2;
      border-radius: 12px;
      background: #fff;
      max-height: 300px;
      overflow: auto;
    }

    .rankings-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.74rem;
    }

    .rankings-table th,
    .rankings-table td {
      padding: 6px 6px;
      border-bottom: 1px dashed #ddd4c5;
      text-align: right;
      white-space: nowrap;
      vertical-align: middle;
    }

    .rankings-table th {
      position: sticky;
      top: 0;
      z-index: 1;
      background: #f1ece1;
      border-bottom: 1px solid #d4ccbe;
      font-weight: 700;
      color: #33414c;
    }

    .rankings-table th:first-child,
    .rankings-table td:first-child {
      text-align: center;
    }

    .rankings-table th:nth-child(2),
    .rankings-table td:nth-child(2) {
      text-align: left;
    }

    .rankings-table tr:last-child td {
      border-bottom: 0;
    }

    .rankings-table tr.rank-row-selected td {
      background: #e8f4f7;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.8rem;
    }

    th, td {
      padding: 5px 4px;
      border-bottom: 1px dashed #ddd4c5;
      text-align: right;
      white-space: nowrap;
    }

    th:first-child, td:first-child { text-align: left; }
    tr:last-child td { border-bottom: 0; }
    .season-row td {
      background: #f7fbfc;
      border-bottom: 1px solid #c9dfe5;
      font-weight: 600;
    }

    .small {
      font-size: 0.79rem;
      color: var(--muted);
    }

    .team-ident {
      display: inline-flex;
      align-items: center;
      gap: 5px;
      min-width: 0;
    }

    .team-logo {
      width: 18px;
      height: 18px;
      object-fit: contain;
      display: inline-block;
      flex: 0 0 auto;
    }

    .team-logo.small {
      width: 16px;
      height: 16px;
    }

    .h2h-list {
      display: grid;
      gap: 5px;
      margin-top: 6px;
    }

    .h2h-row {
      padding: 7px 8px;
      border: 1px solid #d7d0c2;
      border-radius: 10px;
      background: #fff;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      font-size: 0.8rem;
    }

    .h2h-row.goodish { border-color: #bce7d3; background: #f2fbf6; }
    .h2h-row.badish { border-color: #efc6c6; background: #fff4f4; }

    .slate-wrap {
      display: grid;
      gap: 8px;
    }

    .slate-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 2px;
    }

    .status-pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border: 1px solid #d1caba;
      border-radius: 999px;
      padding: 2px 8px;
      font-size: 0.74rem;
      font-weight: 700;
      background: #fff;
      white-space: nowrap;
      line-height: 1;
    }

    .status-dot {
      width: 9px;
      height: 9px;
      border-radius: 999px;
      display: inline-block;
      border: 1px solid rgba(0, 0, 0, 0.18);
      box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.35);
      flex: 0 0 auto;
    }

    .status-pill.status-confirmed { background: #dcf3e5; color: #176a43; border-color: #9fd5b8; }
    .status-pill.status-likely { background: #fff1cf; color: #8a6200; border-color: #e6c57e; }
    .status-pill.status-projected { background: #e7eefb; color: #2e5b99; border-color: #b9ccef; }
    .status-pill.status-unconfirmed { background: #fbe1e1; color: #9a2d2d; border-color: #e8b1b1; }
    .status-pill.status-unknown { background: #ececec; color: #5b6470; border-color: #cfd4db; }
    .status-confirmed { background: #2ea36f; }
    .status-likely { background: #d8a21e; }
    .status-projected { background: #4d82c4; }
    .status-unconfirmed { background: #c75f5f; }
    .status-unknown { background: #9ca3af; }

    .slate-list {
      display: grid;
      gap: 8px;
    }

    .slate-item {
      width: 100%;
      border: 1px solid #d3ccbe;
      border-radius: 12px;
      background: transparent;
      padding: 7px 8px;
      display: grid;
      grid-template-columns: 1.15fr 1fr auto;
      gap: 8px;
      text-align: left;
      cursor: pointer;
      transition: transform 120ms ease, box-shadow 120ms ease, border-color 120ms ease;
    }

    .slate-item:hover {
      transform: translateY(-1px);
      box-shadow: 0 6px 16px rgba(31, 41, 51, 0.09);
      border-color: #b9d5da;
    }

    .slate-item.active {
      border-color: #7ab2bc;
      box-shadow: 0 8px 18px rgba(19, 79, 88, 0.14);
    }

    .slate-item.state-live {
      border-color: #e4a9a9;
      box-shadow: 0 7px 16px rgba(154, 35, 35, 0.1);
    }

    .slate-item.state-final {
      border-color: #b7d9c0;
    }

    .slate-item.status-row-confirmed,
    .slate-item.status-row-likely,
    .slate-item.status-row-unconfirmed,
    .slate-item.status-row-unknown {
      background: transparent;
      border-color: #d3ccbe;
    }

    .slate-item.unavailable {
      cursor: default;
      background: #fcf9f4;
      border-color: #d7c8b8;
    }

    .slate-matchup {
      display: grid;
      gap: 4px;
    }

    .slate-title {
      font-size: 0.86rem;
      font-weight: 700;
      line-height: 1.15;
      display: flex;
      align-items: center;
      gap: 6px;
      flex-wrap: wrap;
    }

    .slate-time {
      font-size: 0.7rem;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }

    .slate-state-row {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
    }

    .slate-goalies {
      display: grid;
      gap: 6px;
      align-content: start;
    }

    .slate-goalie-row {
      display: flex;
      align-items: center;
      gap: 6px;
      min-width: 0;
    }

    .slate-proj {
      display: grid;
      gap: 5px;
      align-content: start;
      justify-items: start;
    }

    .slate-trend {
      font-size: 0.68rem;
      color: var(--muted);
      line-height: 1.25;
    }

    .slate-cta {
      align-self: center;
      justify-self: end;
      font-size: 0.66rem;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: #2f6e78;
      font-weight: 700;
      border: 1px solid #b8d7dd;
      border-radius: 999px;
      padding: 4px 7px;
      background: #ebf6f8;
      white-space: nowrap;
    }

    .slate-inline-note {
      margin-top: 8px;
      color: var(--muted);
      font-size: 0.8rem;
    }

    .error {
      border: 1px solid #e7a7a7;
      background: #fff0f0;
      color: #912f2f;
      border-radius: 12px;
      padding: 10px 12px;
      font-size: 0.9rem;
      display: none;
    }

    @media (max-width: 1180px) {
      .workspace { grid-template-columns: 1fr; }
      .slate-panel, .detail-panel { max-height: none; overflow: visible; }
    }

    @media (max-width: 980px) {
      .controls { grid-template-columns: repeat(2, minmax(160px, 1fr)); }
      .kpis { grid-template-columns: repeat(2, minmax(140px, 1fr)); }
      .grid-two { grid-template-columns: 1fr; }
      button { grid-column: span 2; }
      .slate-item { grid-template-columns: 1fr; gap: 8px; }
      .slate-cta { justify-self: start; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <h1>NHL 1P O1.5 Matchup Dashboard</h1>
      <p>
        Pick teams and projected starters to view fair O1.5 probability/odds plus empirical trends.
        Data timestamp: <span id="generatedAt"></span>
      </p>
    </section>

    <section class="workspace">
      <section class="card slate-panel">
        <h3 class="section-title">Daily Slate View</h3>
        <div id="slateMeta" class="slate-meta"></div>
        <div id="slateList" class="slate-wrap"></div>
        <div id="slateNote" class="slate-inline-note"></div>
      </section>

      <section class="detail-panel">
        <section class="controls">
          <div class="field">
            <label class="label" for="awayTeam">Away Team</label>
            <select id="awayTeam"></select>
          </div>
          <div class="field">
            <label class="label" for="awayGoalie">Away Goalie</label>
            <select id="awayGoalie"></select>
          </div>
          <div class="field">
            <label class="label" for="homeTeam">Home Team</label>
            <select id="homeTeam"></select>
          </div>
          <div class="field">
            <label class="label" for="homeGoalie">Home Goalie</label>
            <select id="homeGoalie"></select>
          </div>
          <div class="field">
            <label class="label" for="swapBtn">Matchup</label>
            <button id="swapBtn" type="button">Swap Teams</button>
          </div>
        </section>

        <div id="errorBox" class="error"></div>

        <section class="kpis">
          <article class="card">
            <div class="kpi-title">Projected O1.5 Probability</div>
            <div id="kpiProb" class="kpi-value">--</div>
          </article>
          <article class="card">
            <div class="kpi-title">Fair Over 1.5 Odds</div>
            <div id="kpiOverOdds" class="kpi-value mono">--</div>
          </article>
          <article class="card">
            <div class="kpi-title">Fair Under 1.5 Odds</div>
            <div id="kpiUnderOdds" class="kpi-value mono">--</div>
          </article>
          <article class="card">
            <div class="kpi-title">H2H O1.5 Rate</div>
            <div id="kpiH2H" class="kpi-value">--</div>
          </article>
        </section>

        <section class="grid-two">
          <article class="card" id="awayTeamCard"></article>
          <article class="card" id="homeTeamCard"></article>
        </section>

        <section class="grid-two">
          <article class="card" id="awayGoalieCard"></article>
          <article class="card" id="homeGoalieCard"></article>
        </section>

        <section class="card">
          <h3 class="section-title">Recent Head-to-Head First-Period Results</h3>
          <div id="h2hSummary" class="small"></div>
          <div id="h2hList" class="h2h-list"></div>
        </section>
      </section>
    </section>

    <section class="card">
      <div class="rankings-head">
        <h3 class="section-title">League Team 2+ 1P Ranking</h3>
        <div id="teamRankingsMeta" class="small"></div>
      </div>
      <div id="teamRankingsTable" class="rankings-wrap"></div>
    </section>
  </div>

  <script>
    const DATASET = __DATASET_JSON__;
    const CONFIG = __CONFIG_JSON__;
    const DAILY_SLATE = __DAILY_SLATE_JSON__;
    const TEAM_LOGOS = __TEAM_LOGOS_JSON__;
    const WINDOWS = [5, 10, 15, 20];

    const awayTeamEl = document.getElementById("awayTeam");
    const awayGoalieEl = document.getElementById("awayGoalie");
    const homeTeamEl = document.getElementById("homeTeam");
    const homeGoalieEl = document.getElementById("homeGoalie");
    const swapBtn = document.getElementById("swapBtn");
    const errorBox = document.getElementById("errorBox");
    const slateMetaEl = document.getElementById("slateMeta");
    const slateListEl = document.getElementById("slateList");
    const slateNoteEl = document.getElementById("slateNote");
    const teamRankingsMetaEl = document.getElementById("teamRankingsMeta");
    const teamRankingsTableEl = document.getElementById("teamRankingsTable");

    document.getElementById("generatedAt").textContent = DATASET.generated_at || "n/a";

    const teams = [...DATASET.teams].sort((a, b) => a.abbrev.localeCompare(b.abbrev));
    const teamsById = new Map(teams.map(t => [t.team_id, t]));
    const teamRankById = new Map();
    const rankedTeamsByO15 = [...teams].sort((a, b) =>
      (b.games_2plus_combined - a.games_2plus_combined) ||
      (b.games_2plus_pct - a.games_2plus_pct) ||
      (b.games - a.games) ||
      a.abbrev.localeCompare(b.abbrev)
    );
    let prevHits = null;
    let prevPct = null;
    let currentRank = 0;
    rankedTeamsByO15.forEach((team, idx) => {
      if (prevHits === null || team.games_2plus_combined !== prevHits || team.games_2plus_pct !== prevPct) {
        currentRank = idx + 1;
        prevHits = team.games_2plus_combined;
        prevPct = team.games_2plus_pct;
      }
      teamRankById.set(team.team_id, currentRank);
    });
    const totalTeams = rankedTeamsByO15.length;
    const goaliesByTeam = new Map();
    for (const g of DATASET.goalies) {
      if (!goaliesByTeam.has(g.team_id)) goaliesByTeam.set(g.team_id, []);
      goaliesByTeam.get(g.team_id).push(g);
    }
    for (const [teamId, goalies] of goaliesByTeam.entries()) {
      goalies.sort((a, b) => (b.games - a.games) || a.ga_pg - b.ga_pg || a.name.localeCompare(b.name));
      goaliesByTeam.set(teamId, goalies);
    }

    const slateGames = (DAILY_SLATE && Array.isArray(DAILY_SLATE.games)) ? DAILY_SLATE.games : [];
    const starterStatusByTeamGoalie = new Map();
    let activeSlateIndex = null;
    for (const game of slateGames) {
      for (const side of ["away", "home"]) {
        const data = game && game[side] ? game[side] : null;
        if (!data) continue;
        const teamId = Number(data.team_id);
        const goalieId = Number(data.goalie_id);
        if (!Number.isFinite(teamId) || !Number.isFinite(goalieId)) continue;
        starterStatusByTeamGoalie.set(`${teamId}:${goalieId}`, {
          status: data.status || "Unconfirmed",
          updatedAt: data.status_updated_at_utc || null
        });
      }
    }

    function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }
    function stabilizedRate(rawRate, sampleGames, priorRate, priorGames) {
      const denom = sampleGames + priorGames;
      if (denom <= 0) return priorRate;
      return ((rawRate * sampleGames) + (priorRate * priorGames)) / denom;
    }
    function blendWithRecent(seasonValue, recentValue, recentGames, baseWeight, fullWeightGames) {
      const fw = fullWeightGames > 0 ? fullWeightGames : 1.0;
      const w = baseWeight * clamp(recentGames / fw, 0, 1);
      return ((1 - w) * seasonValue) + (w * recentValue);
    }
    function poissonAtLeastTwo(lambdaValue) {
      return 1 - Math.exp(-lambdaValue) * (1 + lambdaValue);
    }
    function probabilityToAmerican(prob) {
      const p = clamp(prob, 1e-6, 1 - 1e-6);
      if (p >= 0.5) return -Math.round((p / (1 - p)) * 100);
      return Math.round(((1 - p) / p) * 100);
    }
    function fmtAmerican(odds) { return odds > 0 ? `+${odds}` : `${odds}`; }
    function pairKey(a, b) {
      const low = Math.min(Number(a), Number(b));
      const high = Math.max(Number(a), Number(b));
      return `${low}_${high}`;
    }
    function pct(v) { return `${(v * 100).toFixed(1)}%`; }
    function clsForO15(v) { return v >= 0.68 ? "good" : (v >= 0.54 ? "warn" : "bad"); }
    function clsForRank(rank, total) {
      if (!rank || !total) return "warn";
      const pctile = rank / total;
      if (pctile <= 0.34) return "good";
      if (pctile <= 0.67) return "warn";
      return "bad";
    }
    function clsForCombined(v) { return v >= 2.0 ? "good" : (v >= 1.65 ? "warn" : "bad"); }
    function clsForGaPg(v) { return v <= 0.82 ? "good" : (v <= 1.08 ? "warn" : "bad"); }
    function clsForAllow2(v) { return v <= 0.18 ? "good" : (v <= 0.32 ? "warn" : "bad"); }
    function clsForSv(v) { return v >= 91 ? "good" : (v >= 88.5 ? "warn" : "bad"); }
    function normalizeStatus(status) {
      const s = (status || "").trim().toLowerCase();
      if (!s) return "Unconfirmed";
      if (s === "confirmed") return "Confirmed";
      if (s === "likely") return "Likely";
      if (s === "projected" || s === "expected" || s === "probable") return "Projected";
      if (s === "unconfirmed" || s === "not confirmed") return "Unconfirmed";
      return status;
    }
    function statusClass(status) {
      const s = normalizeStatus(status).toLowerCase();
      if (s === "confirmed") return "status-confirmed";
      if (s === "likely") return "status-likely";
      if (s === "projected") return "status-projected";
      if (s === "unconfirmed") return "status-unconfirmed";
      return "status-unknown";
    }
    function goalieLastName(name) {
      const clean = String(name || "").trim();
      if (!clean) return "TBD";
      const parts = clean.split(/\\s+/).filter(Boolean);
      if (!parts.length) return "TBD";
      return parts[parts.length - 1];
    }
    function statusPill(status, label = null) {
      const canonical = normalizeStatus(status);
      const text = label ? String(label) : canonical;
      return `<span class="status-pill ${statusClass(canonical)}" title="${canonical}"><span class="status-dot ${statusClass(canonical)}"></span>${escapeHtml(text)}</span>`;
    }
    function gameStatusTier(game) {
      const awayStatus = normalizeStatus((game.away || {}).status);
      const homeStatus = normalizeStatus((game.home || {}).status);
      const statuses = [awayStatus, homeStatus];
      if (statuses.every(s => s === "Confirmed")) return "confirmed";
      if (statuses.some(s => s === "Confirmed" || s === "Likely" || s === "Projected")) return "likely";
      if (statuses.every(s => s === "Unconfirmed")) return "unconfirmed";
      return "unknown";
    }
    function escapeHtml(value) {
      const text = String(value ?? "");
      return text
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    function teamLogoSrc(teamAbbrev) {
      const key = String(teamAbbrev || "").trim().toUpperCase();
      if (!key) return "";
      if (TEAM_LOGOS && TEAM_LOGOS[key]) return TEAM_LOGOS[key];
      return `https://assets.nhle.com/logos/nhl/svg/${encodeURIComponent(key)}_light.svg`;
    }

    function teamLogoTag(teamAbbrev, teamLabel, small = false) {
      const src = teamLogoSrc(teamAbbrev);
      if (!src) return "";
      const classes = small ? "team-logo small" : "team-logo";
      const alt = `${teamLabel || teamAbbrev || "team"} logo`;
      return `<img class="${classes}" src="${escapeHtml(src)}" alt="${escapeHtml(alt)}" loading="lazy" decoding="async" />`;
    }

    function gameStateRowClass(game) {
      const code = String((((game || {}).game_status || {}).state_code || "")).toUpperCase();
      if (code === "LIVE" || code === "CRIT") return "state-live";
      if (code === "OFF" || code === "FINAL") return "state-final";
      return "state-scheduled";
    }

    function renderGameStateChips(game) {
      const gameStatus = (game || {}).game_status || {};
      const code = String(gameStatus.state_code || "").toUpperCase();
      const label = String(gameStatus.state_label || code || "").trim();
      if (!label) return "";

      const chips = [];
      let stateText = label;
      if (code === "LIVE" || code === "CRIT") {
        const period = Number(gameStatus.period || 0);
        const clock = String(gameStatus.clock || "").trim();
        if (period > 0) stateText += ` P${period}`;
        if (clock) stateText += ` ${clock}`;
      }
      const stateClass = (code === "LIVE" || code === "CRIT") ? "bad" : ((code === "OFF" || code === "FINAL") ? "good" : "warn");
      chips.push(`<span class="tag ${stateClass}">${escapeHtml(stateText)}</span>`);

      if (gameStatus.first_period_complete && gameStatus.first_period_total_result) {
        const result = String(gameStatus.first_period_total_result || "").toUpperCase();
        const resultClass = result === "OVER" ? "good" : "bad";
        const goals = Number(gameStatus.first_period_goals || 0);
        chips.push(`<span class="tag ${resultClass}">1P ${escapeHtml(result)} (${goals})</span>`);
      } else if ((code === "LIVE" || code === "CRIT") && Number.isFinite(Number(gameStatus.first_period_goals))) {
        chips.push(`<span class="tag warn">1P ${Number(gameStatus.first_period_goals)}G</span>`);
      }

      return chips.join("");
    }

    function getWindowRow(stats, window) {
      if (!stats) return null;
      return stats[String(window)] || null;
    }

    function goalieRankText(goalie) {
      if (goalie.rank_ga_pg_qualified && goalie.rank_save_pct_qualified && goalie.rank_total_goalies_qualified) {
        return `GA #${goalie.rank_ga_pg_qualified}/${goalie.rank_total_goalies_qualified} | SV #${goalie.rank_save_pct_qualified}/${goalie.rank_total_goalies_qualified}`;
      }
      if (goalie.rank_ga_pg_all && goalie.rank_save_pct_all && goalie.rank_total_goalies_all) {
        return `GA #${goalie.rank_ga_pg_all}/${goalie.rank_total_goalies_all} | SV #${goalie.rank_save_pct_all}/${goalie.rank_total_goalies_all}`;
      }
      return "Rank n/a";
    }

    function projectMatchup(awayTeam, homeTeam, awayGoalie, homeGoalie) {
      const leagueGoalRate = DATASET.league.team_goal_rate;
      const leagueCombinedRate = DATASET.league.combined_rate > 0 ? DATASET.league.combined_rate : leagueGoalRate * 2;
      const leagueGoalieGaRate = DATASET.league.goalie_ga_rate > 0 ? DATASET.league.goalie_ga_rate : leagueGoalRate;

      const awayOffSeason = stabilizedRate(awayTeam.gf_pg, awayTeam.games, leagueGoalRate, CONFIG.team_prior_games);
      const homeOffSeason = stabilizedRate(homeTeam.gf_pg, homeTeam.games, leagueGoalRate, CONFIG.team_prior_games);
      const awayDefSeason = stabilizedRate(awayTeam.ga_pg, awayTeam.games, leagueGoalRate, CONFIG.team_prior_games);
      const homeDefSeason = stabilizedRate(homeTeam.ga_pg, homeTeam.games, leagueGoalRate, CONFIG.team_prior_games);

      const awayOff = blendWithRecent(
        awayOffSeason, awayTeam.recent_gf_pg ?? awayOffSeason, awayTeam.recent_games ?? 0,
        CONFIG.team_form_base_weight, CONFIG.team_form_full_weight_games
      );
      const homeOff = blendWithRecent(
        homeOffSeason, homeTeam.recent_gf_pg ?? homeOffSeason, homeTeam.recent_games ?? 0,
        CONFIG.team_form_base_weight, CONFIG.team_form_full_weight_games
      );
      const awayDef = blendWithRecent(
        awayDefSeason, awayTeam.recent_ga_pg ?? awayDefSeason, awayTeam.recent_games ?? 0,
        CONFIG.team_form_base_weight, CONFIG.team_form_full_weight_games
      );
      const homeDef = blendWithRecent(
        homeDefSeason, homeTeam.recent_ga_pg ?? homeDefSeason, homeTeam.recent_games ?? 0,
        CONFIG.team_form_base_weight, CONFIG.team_form_full_weight_games
      );

      const awayGoalieSeason = stabilizedRate(awayGoalie.ga_pg, awayGoalie.games, leagueGoalieGaRate, CONFIG.goalie_prior_games);
      const homeGoalieSeason = stabilizedRate(homeGoalie.ga_pg, homeGoalie.games, leagueGoalieGaRate, CONFIG.goalie_prior_games);
      const awayGoalieBlend = blendWithRecent(
        awayGoalieSeason, awayGoalie.recent_ga_pg ?? awayGoalieSeason, awayGoalie.recent_games ?? 0,
        CONFIG.goalie_form_base_weight, CONFIG.goalie_form_full_weight_games
      );
      const homeGoalieBlend = blendWithRecent(
        homeGoalieSeason, homeGoalie.recent_ga_pg ?? homeGoalieSeason, homeGoalie.recent_games ?? 0,
        CONFIG.goalie_form_base_weight, CONFIG.goalie_form_full_weight_games
      );

      const awayGoalieFactor = clamp(awayGoalieBlend / Math.max(awayDef, 1e-6), CONFIG.goalie_factor_min, CONFIG.goalie_factor_max);
      const homeGoalieFactor = clamp(homeGoalieBlend / Math.max(homeDef, 1e-6), CONFIG.goalie_factor_min, CONFIG.goalie_factor_max);

      const lambdaAway = 0.5 * (awayOff + homeDef) * homeGoalieFactor;
      const lambdaHome = 0.5 * (homeOff + awayDef) * awayGoalieFactor;

      const awayTempo = awayTeam.combined_pg / Math.max(leagueCombinedRate, 1e-6);
      const homeTempo = homeTeam.combined_pg / Math.max(leagueCombinedRate, 1e-6);
      const tempoFactor = clamp(Math.sqrt(Math.max(awayTempo, 1e-6) * Math.max(homeTempo, 1e-6)), CONFIG.tempo_factor_min, CONFIG.tempo_factor_max);

      const lambdaTotal = (lambdaAway + lambdaHome) * tempoFactor;
      const poissonProb = poissonAtLeastTwo(lambdaTotal);

      const awayEmp = blendWithRecent(
        awayTeam.games_2plus_pct,
        awayTeam.recent_2plus_pct ?? awayTeam.games_2plus_pct,
        awayTeam.recent_games ?? 0,
        CONFIG.team_form_base_weight,
        CONFIG.team_form_full_weight_games
      );
      const homeEmp = blendWithRecent(
        homeTeam.games_2plus_pct,
        homeTeam.recent_2plus_pct ?? homeTeam.games_2plus_pct,
        homeTeam.recent_games ?? 0,
        CONFIG.team_form_base_weight,
        CONFIG.team_form_full_weight_games
      );
      const empiricalProb = 0.5 * (awayEmp + homeEmp);
      const finalProb = clamp(
        (CONFIG.poisson_weight * poissonProb) + ((1 - CONFIG.poisson_weight) * empiricalProb),
        CONFIG.prob_min,
        CONFIG.prob_max
      );

      return {
        prob: finalProb,
        overOdds: probabilityToAmerican(finalProb),
        underOdds: probabilityToAmerican(1 - finalProb),
        h2hGames: DATASET.h2h_games.filter(g => g.pair_key === pairKey(awayTeam.team_id, homeTeam.team_id))
      };
    }

    function populateTeams() {
      awayTeamEl.innerHTML = "";
      homeTeamEl.innerHTML = "";
      for (const team of teams) {
        const label = `${team.abbrev} - ${team.name}`;
        const aOpt = new Option(label, String(team.team_id));
        const hOpt = new Option(label, String(team.team_id));
        awayTeamEl.add(aOpt);
        homeTeamEl.add(hOpt);
      }
      awayTeamEl.value = String(teams[0].team_id);
      homeTeamEl.value = String(teams[1].team_id);
    }

    function populateGoalies(teamSelect, goalieSelect, keepValue = null) {
      const teamId = Number(teamSelect.value);
      const goalies = goaliesByTeam.get(teamId) || [];
      goalieSelect.innerHTML = "";
      for (const g of goalies) {
        const statusMeta = starterStatusByTeamGoalie.get(`${teamId}:${g.goalie_id}`);
        const statusText = statusMeta ? `, ${normalizeStatus(statusMeta.status)}` : "";
        const text = `${g.name} (GP ${g.games}, GA/GP ${g.ga_pg.toFixed(2)}${statusText})`;
        const opt = new Option(text, String(g.goalie_id));
        goalieSelect.add(opt);
      }
      if (keepValue && [...goalieSelect.options].some(o => o.value === keepValue)) {
        goalieSelect.value = keepValue;
      } else if (goalieSelect.options.length > 0) {
        goalieSelect.selectedIndex = 0;
      }
    }

    function selectedEntities() {
      const awayTeam = teamsById.get(Number(awayTeamEl.value));
      const homeTeam = teamsById.get(Number(homeTeamEl.value));
      const awayGoalies = goaliesByTeam.get(awayTeam.team_id) || [];
      const homeGoalies = goaliesByTeam.get(homeTeam.team_id) || [];
      const awayGoalie = awayGoalies.find(g => g.goalie_id === Number(awayGoalieEl.value)) || awayGoalies[0];
      const homeGoalie = homeGoalies.find(g => g.goalie_id === Number(homeGoalieEl.value)) || homeGoalies[0];
      return { awayTeam, homeTeam, awayGoalie, homeGoalie };
    }

    function renderTeamCard(elId, team) {
      const rank = teamRankById.get(team.team_id);
      const rankClass = clsForRank(rank, totalTeams);
      const seasonRow = `
        <tr class="season-row">
          <td>Season (${team.games}g)</td>
          <td class="${clsForGaPg(team.gf_pg)}">${team.gf_pg.toFixed(2)}</td>
          <td class="${clsForGaPg(team.ga_pg)}">${team.ga_pg.toFixed(2)}</td>
          <td>${team.games_2plus_combined}/${team.games}</td>
          <td><span class="tag ${clsForO15(team.games_2plus_pct)}">${pct(team.games_2plus_pct)}</span></td>
        </tr>`;

      const rows = WINDOWS.map(w => {
        const r = getWindowRow(team.window_stats, w);
        if (!r) return "";
        return `
          <tr>
            <td>L${w}</td>
            <td class="${clsForGaPg(r.gf_pg)}">${r.gf_pg.toFixed(2)}</td>
            <td class="${clsForGaPg(r.ga_pg)}">${r.ga_pg.toFixed(2)}</td>
            <td>${r.ge2_count}/${r.games}</td>
            <td><span class="tag ${clsForO15(r.ge2_pct)}">${pct(r.ge2_pct)}</span></td>
          </tr>`;
      }).join("");

      document.getElementById(elId).innerHTML = `
        <h3 class="section-title"><span class="team-ident">${teamLogoTag(team.abbrev, team.name, true)}<span>${team.abbrev} Team Form</span></span></h3>
        <div class="small">
          <span class="tag ${rankClass}">2+ Goal-Game Rank #${rank}/${totalTeams}</span>
          &nbsp;Season O1.5: ${team.games_2plus_combined}/${team.games} (${pct(team.games_2plus_pct)})
        </div>
        <table>
          <thead>
            <tr>
              <th>Window</th><th>GF/GP</th><th>GA/GP</th><th>O1.5 Games (2+)</th><th>O1.5%</th>
            </tr>
          </thead>
          <tbody>${seasonRow}${rows}</tbody>
        </table>`;
    }

    function trendTag(row) {
      if (!row || !Number(row.games)) return `<span class="small">n/a</span>`;
      return `<span class="tag ${clsForO15(Number(row.ge2_pct || 0))}">${Number(row.ge2_count || 0)}/${Number(row.games || 0)} ${pct(Number(row.ge2_pct || 0))}</span>`;
    }

    function renderTeamRankings(selectedAwayTeamId = null, selectedHomeTeamId = null) {
      if (!teamRankingsTableEl || !teamRankingsMetaEl) return;

      const selectedIds = new Set(
        [Number(selectedAwayTeamId), Number(selectedHomeTeamId)].filter(Number.isFinite)
      );
      const trendHeaders = WINDOWS.map(w => `<th>L${w}</th>`).join("");
      const rows = rankedTeamsByO15.map(team => {
        const rank = teamRankById.get(team.team_id) || 0;
        const rankClass = clsForRank(rank, totalTeams);
        const selectedClass = selectedIds.has(Number(team.team_id)) ? "rank-row-selected" : "";
        const trendCells = WINDOWS.map(w => `<td>${trendTag(getWindowRow(team.window_stats, w))}</td>`).join("");

        return `
          <tr class="${selectedClass}">
            <td><span class="tag ${rankClass}">#${rank}</span></td>
            <td><span class="team-ident">${teamLogoTag(team.abbrev, team.name, true)}<strong>${escapeHtml(team.abbrev)}</strong></span> <span class="small">${escapeHtml(team.name)}</span></td>
            <td>${Number(team.games_2plus_combined || 0)}/${Number(team.games || 0)}</td>
            <td><span class="tag ${clsForO15(Number(team.games_2plus_pct || 0))}">${pct(Number(team.games_2plus_pct || 0))}</span></td>
            <td><span class="tag ${clsForCombined(Number(team.combined_pg || 0))}">${Number(team.combined_pg || 0).toFixed(2)}</span></td>
            ${trendCells}
          </tr>`;
      }).join("");

      teamRankingsMetaEl.textContent = `${totalTeams} teams sorted by season 2+ first-period goal-game count`;
      teamRankingsTableEl.innerHTML = `
        <table class="rankings-table">
          <thead>
            <tr>
              <th>Rank</th>
              <th>Team</th>
              <th>2+ Goal Games</th>
              <th>2+%</th>
              <th>Comb/GP</th>
              ${trendHeaders}
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      `;
    }

    function renderGoalieCard(elId, team, goalie) {
      const rows = WINDOWS.map(w => {
        const r = getWindowRow(goalie.window_stats, w);
        if (!r) return "";
        return `
          <tr>
            <td>L${w}</td>
            <td>${r.ga_total.toFixed(0)}</td>
            <td class="${clsForGaPg(r.ga_pg)}">${r.ga_pg.toFixed(2)}</td>
            <td class="${clsForSv(r.sv_pct)}">${r.sv_pct.toFixed(1)}%</td>
            <td>${r.allow2_count}/${r.games}</td>
            <td><span class="tag ${clsForAllow2(r.allow2_pct)}">${pct(r.allow2_pct)}</span></td>
          </tr>`;
      }).join("");

      const statusMeta = starterStatusByTeamGoalie.get(`${team.team_id}:${goalie.goalie_id}`);
      const statusValue = statusMeta ? statusMeta.status : "Unconfirmed";
      const statusMarkup = statusPill(statusValue, goalieLastName(goalie.name));
      const statusUpdated = statusMeta && statusMeta.updatedAt
        ? `<span class="small">${normalizeStatus(statusValue)} | Updated: ${statusMeta.updatedAt}</span>`
        : `<span class="small">${normalizeStatus(statusValue)} | No same-day confirmation timestamp.</span>`;

      document.getElementById(elId).innerHTML = `
        <h3 class="section-title"><span class="team-ident">${teamLogoTag(team.abbrev, team.name, true)}<span>${team.abbrev} - ${goalie.name}</span></span></h3>
        <div class="small">${goalieRankText(goalie)} | Season 1P GA/GP ${goalie.ga_pg.toFixed(2)} | SV% ${goalie.save_pct.toFixed(1)}%</div>
        <div style="margin:6px 0 8px; display:flex; gap:8px; align-items:center; flex-wrap:wrap;">
          ${statusMarkup}
          ${statusUpdated}
        </div>
        <table>
          <thead>
            <tr>
              <th>Window</th><th>1P GA</th><th>GA/GP</th><th>SV%</th><th>Allow 2+</th><th>Allow 2+%</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>`;
    }

    function renderH2H(awayTeam, homeTeam, h2hGames) {
      const listEl = document.getElementById("h2hList");
      const summaryEl = document.getElementById("h2hSummary");
      if (!h2hGames.length) {
        summaryEl.textContent = "No meetings in this dataset window.";
        listEl.innerHTML = "";
        document.getElementById("kpiH2H").innerHTML = `<span class="tag bad">No data</span>`;
        return;
      }

      h2hGames.sort((a, b) => (b.game_date || "").localeCompare(a.game_date || "") || (b.game_id - a.game_id));
      const ge2Count = h2hGames.filter(g => g.combined_1p >= 2).length;
      const hitRate = ge2Count / h2hGames.length;
      const avgCombined = h2hGames.reduce((acc, g) => acc + g.combined_1p, 0) / h2hGames.length;
      summaryEl.innerHTML = `
        ${h2hGames.length} meeting(s) | Avg 1P combined goals <b>${avgCombined.toFixed(2)}</b> |
        O1.5 rate <b>${ge2Count}/${h2hGames.length} (${pct(hitRate)})</b>`;
      document.getElementById("kpiH2H").innerHTML = `<span class="tag ${clsForO15(hitRate)}">${pct(hitRate)}</span>`;

      listEl.innerHTML = h2hGames.slice(0, 5).map(g => {
        const awayGF = g.team_a_id === awayTeam.team_id ? g.team_a_gf : g.team_b_gf;
        const homeGF = g.team_a_id === homeTeam.team_id ? g.team_a_gf : g.team_b_gf;
        const scoreText = `${awayTeam.abbrev} ${awayGF} - ${homeGF} ${homeTeam.abbrev}`;
        const cls = g.combined_1p >= 2 ? "goodish" : "badish";
        return `
          <div class="h2h-row ${cls}">
            <div>${g.game_date || g.game_id} <span class="small">${scoreText}</span></div>
            <div><span class="tag ${clsForO15(g.combined_1p >= 2 ? 1 : 0)}">1P total ${g.combined_1p}</span></div>
          </div>`;
      }).join("");
    }

    function selectSlateGame(index) {
      const game = slateGames[index];
      if (!game || !game.projection) return;

      const away = game.away || {};
      const home = game.home || {};
      const awayTeamId = Number(away.team_id);
      const homeTeamId = Number(home.team_id);
      if (!Number.isFinite(awayTeamId) || !Number.isFinite(homeTeamId)) return;

      awayTeamEl.value = String(awayTeamId);
      homeTeamEl.value = String(homeTeamId);
      populateGoalies(awayTeamEl, awayGoalieEl);
      populateGoalies(homeTeamEl, homeGoalieEl);

      const awayGoalieId = String(away.goalie_id || "");
      const homeGoalieId = String(home.goalie_id || "");
      if ([...awayGoalieEl.options].some(o => o.value === awayGoalieId)) awayGoalieEl.value = awayGoalieId;
      if ([...homeGoalieEl.options].some(o => o.value === homeGoalieId)) homeGoalieEl.value = homeGoalieId;

      activeSlateIndex = index;
      render();
      renderSlate();
    }

    function renderSlate() {
      if (!slateGames.length) {
        slateMetaEl.innerHTML = `<span class="tag warn">Daily starter feed unavailable</span>`;
        slateListEl.innerHTML = `<div class="small">No daily games were loaded for this build.</div>`;
        slateNoteEl.textContent = "You can still use manual team/goalie selection below.";
        return;
      }

      const meta = DAILY_SLATE.meta || {};
      const statusCounts = meta.status_counts || {};
      const statusChips = Object.keys(statusCounts).sort().map(status => {
        return `<span class="status-pill ${statusClass(status)}"><span class="status-dot ${statusClass(status)}"></span>${escapeHtml(status)}: ${statusCounts[status]}</span>`;
      }).join("");
      const liveTag = `<span class="tag ${(meta.live_games || 0) > 0 ? "bad" : "warn"}">Live ${meta.live_games || 0}</span>`;
      const gradedTag = `<span class="tag good">1P Graded ${meta.first_period_graded_games || 0}</span>`;
      const overTag = `<span class="tag good">OVER ${meta.first_period_over_games || 0}</span>`;
      const underTag = `<span class="tag bad">UNDER ${meta.first_period_under_games || 0}</span>`;

      slateMetaEl.innerHTML = `
        <span class="tag good">Date ${escapeHtml(DAILY_SLATE.target_date || "n/a")}</span>
        <span class="tag ${meta.failed_games > 0 ? "warn" : "good"}">Projected ${meta.projectable_games || 0}/${meta.total_games || slateGames.length}</span>
        ${liveTag}
        ${gradedTag}
        ${overTag}
        ${underTag}
        ${statusChips}
      `;

      slateListEl.innerHTML = `
        <div class="slate-list">
          ${slateGames.map((game, idx) => {
            const away = game.away || {};
            const home = game.home || {};
            const available = !!game.projection;
            const projection = game.projection || {};
            const trend = game.trends || {};
            const prob = available ? Number(projection.prob_over_1p_1_5 || 0) : null;
            const overOdds = available ? fmtAmerican(Number(projection.over_american_odds || 0)) : "--";
            const underOdds = available ? fmtAmerican(Number(projection.under_american_odds || 0)) : "--";
            const h2hText = (trend.h2h_games && trend.h2h_o15_pct !== null && trend.h2h_o15_pct !== undefined)
              ? `H2H O1.5 ${pct(Number(trend.h2h_o15_pct))} (${trend.h2h_o15_hits}/${trend.h2h_games})`
              : "H2H O1.5 n/a";
            const gameTime = game.game_time_et || game.game_time_utc || "Time TBD";
            const gameStateChips = renderGameStateChips(game);
            const rowStatusTier = gameStatusTier(game);
            const awayGoalieName = away.goalie_name || away.goalie_name_feed || "TBD";
            const homeGoalieName = home.goalie_name || home.goalie_name_feed || "TBD";
            const awayLabel = away.team_abbrev || away.team_name_feed || "AWY";
            const homeLabel = home.team_abbrev || home.team_name_feed || "HME";
            const rowClasses = [
              "slate-item",
              `status-row-${rowStatusTier}`,
              gameStateRowClass(game),
              available ? "" : "unavailable",
              activeSlateIndex === idx ? "active" : ""
            ].join(" ").trim();
            const cta = available ? `<span class="slate-cta">View Details</span>` : `<span class="slate-cta">Unavailable</span>`;

            return `
              <button type="button" class="${rowClasses}" data-slate-index="${idx}" ${available ? "" : "disabled"}>
                <div class="slate-matchup">
                  <div class="slate-title">
                    <span class="team-ident">${teamLogoTag(away.team_abbrev, away.team_name || away.team_name_feed, true)}<span>${escapeHtml(awayLabel)}</span></span>
                    <span class="small">at</span>
                    <span class="team-ident">${teamLogoTag(home.team_abbrev, home.team_name || home.team_name_feed, true)}<span>${escapeHtml(homeLabel)}</span></span>
                  </div>
                  <div class="slate-time">${escapeHtml(gameTime)}</div>
                  ${gameStateChips ? `<div class="slate-state-row">${gameStateChips}</div>` : ""}
                </div>
                <div class="slate-goalies">
                  <div class="slate-goalie-row">${statusPill(away.status || "Unconfirmed", goalieLastName(awayGoalieName))}</div>
                  <div class="slate-goalie-row">${statusPill(home.status || "Unconfirmed", goalieLastName(homeGoalieName))}</div>
                  <div class="slate-trend">${escapeHtml(h2hText)}</div>
                </div>
                <div class="slate-proj">
                  ${available ? `<span class="tag ${clsForO15(prob)}">O1.5 ${pct(prob)}</span>` : `<span class="tag bad">No projection</span>`}
                  <div class="mono">O ${overOdds} / U ${underOdds}</div>
                </div>
                ${cta}
              </button>`;
          }).join("")}
        </div>
      `;

      slateListEl.querySelectorAll("[data-slate-index]").forEach(btn => {
        btn.addEventListener("click", () => {
          const idx = Number(btn.getAttribute("data-slate-index"));
          selectSlateGame(idx);
        });
      });

      const warnings = Array.isArray(DAILY_SLATE.warnings) ? DAILY_SLATE.warnings : [];
      if (warnings.length) {
        const preview = warnings.slice(0, 2).join(" | ");
        slateNoteEl.textContent = `Notes: ${preview}${warnings.length > 2 ? " ..." : ""}`;
      } else {
        slateNoteEl.textContent = "Click any game row to load full lower-level team/goalie and H2H detail below.";
      }
    }

    function render() {
      const { awayTeam, homeTeam, awayGoalie, homeGoalie } = selectedEntities();
      renderTeamRankings(awayTeam ? awayTeam.team_id : null, homeTeam ? homeTeam.team_id : null);
      if (!awayTeam || !homeTeam || !awayGoalie || !homeGoalie) return;

      if (awayTeam.team_id === homeTeam.team_id) {
        errorBox.style.display = "block";
        errorBox.textContent = "Away and home teams must be different.";
        return;
      }
      errorBox.style.display = "none";

      const projection = projectMatchup(awayTeam, homeTeam, awayGoalie, homeGoalie);
      document.getElementById("kpiProb").innerHTML = `<span class="tag ${clsForO15(projection.prob)}">${pct(projection.prob)}</span>`;
      document.getElementById("kpiOverOdds").textContent = fmtAmerican(projection.overOdds);
      document.getElementById("kpiUnderOdds").textContent = fmtAmerican(projection.underOdds);

      renderTeamCard("awayTeamCard", awayTeam);
      renderTeamCard("homeTeamCard", homeTeam);
      renderGoalieCard("awayGoalieCard", awayTeam, awayGoalie);
      renderGoalieCard("homeGoalieCard", homeTeam, homeGoalie);
      renderH2H(awayTeam, homeTeam, projection.h2hGames);
    }

    function onTeamChange(teamEl, goalieEl) {
      const keepGoalie = goalieEl.value;
      populateGoalies(teamEl, goalieEl, keepGoalie);
      activeSlateIndex = null;
      render();
      renderSlate();
    }

    function init() {
      populateTeams();
      populateGoalies(awayTeamEl, awayGoalieEl);
      populateGoalies(homeTeamEl, homeGoalieEl);

      awayTeamEl.addEventListener("change", () => onTeamChange(awayTeamEl, awayGoalieEl));
      homeTeamEl.addEventListener("change", () => onTeamChange(homeTeamEl, homeGoalieEl));
      awayGoalieEl.addEventListener("change", () => {
        activeSlateIndex = null;
        render();
        renderSlate();
      });
      homeGoalieEl.addEventListener("change", () => {
        activeSlateIndex = null;
        render();
        renderSlate();
      });

      swapBtn.addEventListener("click", () => {
        const awayTeam = awayTeamEl.value;
        const awayGoalie = awayGoalieEl.value;
        awayTeamEl.value = homeTeamEl.value;
        homeTeamEl.value = awayTeam;
        populateGoalies(awayTeamEl, awayGoalieEl);
        populateGoalies(homeTeamEl, homeGoalieEl);
        if ([...awayGoalieEl.options].some(o => o.value === awayGoalie)) {
          awayGoalieEl.value = awayGoalie;
        }
        activeSlateIndex = null;
        render();
        renderSlate();
      });

      renderSlate();
      const firstPlayableIndex = slateGames.findIndex(g => !!g.projection);
      if (firstPlayableIndex >= 0) {
        selectSlateGame(firstPlayableIndex);
        return;
      }
      render();
    }

    init();
  </script>
</body>
</html>
"""


TEAM_LOGO_CANDIDATES_BY_ABBREV = {
    "ANA": ["Anaheim"],
    "ARI": ["Arizona", "Phoenix"],
    "BOS": ["Boston"],
    "BUF": ["Buffalo"],
    "CGY": ["Calgary"],
    "CAR": ["Carolina"],
    "CHI": ["Chicago"],
    "COL": ["Colorado"],
    "CBJ": ["Columbus"],
    "DAL": ["Dallas"],
    "DET": ["Detroit"],
    "EDM": ["Edmonton"],
    "FLA": ["Florida"],
    "LAK": ["Los Angeles"],
    "MIN": ["Minnesota"],
    "MTL": ["Montreal"],
    "NSH": ["Nashville"],
    "NJD": ["New Jersey"],
    "NYI": ["NY Islanders", "New York Islanders", "NYI"],
    "NYR": ["NY Rangers", "New York Rangers"],
    "OTT": ["Ottawa"],
    "PHI": ["Philadelphia"],
    "PIT": ["Pittsburgh"],
    "SEA": ["Seattle"],
    "SJS": ["San Jose"],
    "STL": ["St. Louis", "St Louis"],
    "TBL": ["Tampa Bay"],
    "TOR": ["Toronto"],
    "UTA": ["Utah", "Arizona", "Phoenix"],
    "VAN": ["Vancouver"],
    "VGK": ["Vegas"],
    "WSH": ["Washington"],
    "WPG": ["Winnipeg"],
}


def _normalize_logo_key(value):
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _build_team_logo_map(dataset, script_dir):
    logos_dir_candidates = [
        script_dir / "NHL Logos",
        script_dir.parent / "NHL Logos",
    ]
    logos_dir = next((p for p in logos_dir_candidates if p.exists() and p.is_dir()), None)
    if logos_dir is None:
        return {}

    best_by_name = {}
    for logo_path in sorted(logos_dir.glob("*.png"), key=lambda p: p.name.lower()):
        stem = logo_path.stem.strip()
        base = re.sub(r"\s+\d+$", "", stem).strip()
        key = _normalize_logo_key(base)
        if not key:
            continue
        priority = 0 if stem == base else 1
        current = best_by_name.get(key)
        candidate_meta = (priority, len(logo_path.name), logo_path.name.lower())
        if current is None or candidate_meta < current[0]:
            best_by_name[key] = (candidate_meta, logo_path)

    team_logo_map = {}
    for team in dataset.get("teams", []):
        abbrev = str(team.get("abbrev") or "").upper()
        if not abbrev:
            continue

        candidates = list(TEAM_LOGO_CANDIDATES_BY_ABBREV.get(abbrev, []))
        if team.get("name"):
            candidates.append(str(team["name"]))
        candidates.append(abbrev)

        chosen_path = None
        for candidate in candidates:
            key = _normalize_logo_key(candidate)
            if key in best_by_name:
                chosen_path = best_by_name[key][1]
                break

        if not chosen_path:
            continue

        rel_path = Path(os.path.relpath(chosen_path, start=script_dir)).as_posix()
        team_logo_map[abbrev] = "/".join(quote(part) for part in rel_path.split("/"))

    return team_logo_map


def build_dashboard_html(force_refresh=False, output_path=None, slate_date=None):
    script_dir = Path(__file__).resolve().parent
    dataset = proj.build_projection_dataset(force_refresh=force_refresh, verbose=True)
    team_logos = _build_team_logo_map(dataset, script_dir)
    try:
        daily_slate = proj.build_daily_projection_slate(dataset, date_str=slate_date, verbose=True)
    except Exception as exc:
        daily_slate = {
            "source_url": proj.dfo_build_url(slate_date),
            "pulled_at_utc": None,
            "target_date": slate_date,
            "games": [],
            "warnings": [f"Daily slate unavailable: {exc}"],
            "meta": {
                "total_games": 0,
                "projectable_games": 0,
                "failed_games": 0,
                "status_counts": {},
                "live_games": 0,
                "first_period_graded_games": 0,
                "first_period_over_games": 0,
                "first_period_under_games": 0,
            },
        }

    config = {
        "team_prior_games": proj.TEAM_PRIOR_GAMES,
        "goalie_prior_games": proj.GOALIE_PRIOR_GAMES,
        "team_form_base_weight": proj.TEAM_FORM_BASE_WEIGHT,
        "goalie_form_base_weight": proj.GOALIE_FORM_BASE_WEIGHT,
        "team_form_full_weight_games": proj.TEAM_FORM_FULL_WEIGHT_GAMES,
        "goalie_form_full_weight_games": proj.GOALIE_FORM_FULL_WEIGHT_GAMES,
        "poisson_weight": proj.POISSON_WEIGHT,
        "goalie_factor_min": proj.GOALIE_FACTOR_MIN,
        "goalie_factor_max": proj.GOALIE_FACTOR_MAX,
        "tempo_factor_min": proj.TEMPO_FACTOR_MIN,
        "tempo_factor_max": proj.TEMPO_FACTOR_MAX,
        "prob_min": proj.PROB_MIN,
        "prob_max": proj.PROB_MAX,
    }

    html = HTML_TEMPLATE.replace(
        "__DATASET_JSON__", json.dumps(dataset, ensure_ascii=False, separators=(",", ":"))
    ).replace(
        "__CONFIG_JSON__", json.dumps(config, ensure_ascii=False, separators=(",", ":"))
    ).replace(
        "__DAILY_SLATE_JSON__", json.dumps(daily_slate, ensure_ascii=False, separators=(",", ":"))
    ).replace(
        "__TEAM_LOGOS_JSON__", json.dumps(team_logos, ensure_ascii=False, separators=(",", ":"))
    )

    out_path = Path(output_path) if output_path else script_dir / "index.html"
    out_path.write_text(html, encoding="utf-8")
    return out_path


def parse_args():
    parser = argparse.ArgumentParser(description="Build interactive 1P O1.5 HTML dashboard.")
    parser.add_argument("--refresh", action="store_true", help="Refresh dataset from APIs before writing dashboard")
    parser.add_argument("--output", help="Output HTML file path")
    parser.add_argument("--date", help="Daily slate date override (YYYY-MM-DD)")
    return parser.parse_args()


def main():
    args = parse_args()
    out_path = build_dashboard_html(force_refresh=args.refresh, output_path=args.output, slate_date=args.date)
    print(f"Dashboard written to: {out_path}")


if __name__ == "__main__":
    main()
