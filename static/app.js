const state = {
  agentId: "A003",
  dashboard: null,
};

const nodes = {
  agentSelect: document.querySelector("#agentSelect"),
  streakStrip: document.querySelector("#streakStrip"),
  currentStreak: document.querySelector("#currentStreak"),
  longestStreak: document.querySelector("#longestStreak"),
  shieldCount: document.querySelector("#shieldCount"),
  lifetimePoints: document.querySelector("#lifetimePoints"),
  studyForm: document.querySelector("#studyForm"),
  moduleId: document.querySelector("#moduleId"),
  quizScore: document.querySelector("#quizScore"),
  bioRhythm: document.querySelector("#bioRhythm"),
  statusLine: document.querySelector("#statusLine"),
  leaderboardList: document.querySelector("#leaderboardList"),
  branchGrid: document.querySelector("#branchGrid"),
  ledgerRows: document.querySelector("#ledgerRows"),
  epochLabel: document.querySelector("#epochLabel"),
  runDailyCheck: document.querySelector("#runDailyCheck"),
  rebuildCache: document.querySelector("#rebuildCache"),
};

async function requestJson(url, options = {}) {
  const response = await fetch(url, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || "Request failed");
  }
  return payload;
}

async function loadAgents() {
  const payload = await requestJson("/api/agents");
  nodes.agentSelect.innerHTML = payload.agents
    .map(
      (agent) =>
        `<option value="${escapeHtml(agent.agent_id)}">${escapeHtml(agent.agent_name)} · ${escapeHtml(agent.branch_name)}</option>`
    )
    .join("");
  nodes.agentSelect.value = state.agentId;
}

async function loadDashboard(message = "") {
  state.dashboard = await requestJson(`/api/dashboard?agent_id=${state.agentId}`);
  renderDashboard();
  nodes.statusLine.textContent = message;
}

function renderDashboard() {
  const dashboard = state.dashboard;
  nodes.epochLabel.textContent = `Weekly epoch ${dashboard.epoch_week_number}`;
  renderStreak(dashboard.streak, dashboard.lifetime_points);
  renderLeaderboard(dashboard.relative_leaderboard);
  renderBranches(dashboard.branch_arena);
  renderLedger(dashboard.recent_ledger);
}

function renderStreak(streak, lifetimePoints) {
  const visibleDays = Math.max(7, Math.min(14, streak.current_streak_days + 2));
  const activeDays = Math.min(streak.current_streak_days, visibleDays);
  const fires = Array.from({ length: visibleDays }, (_, index) => {
    const active = index < activeDays;
    return `<span class="fire-icon ${active ? "active" : ""}" aria-hidden="true">🔥</span>`;
  }).join("");
  const shields = Array.from(
    { length: Math.max(1, streak.active_shields_count) },
    (_, index) => {
      const active = index < streak.active_shields_count;
      return `<span class="shield-icon ${active ? "active" : ""}" aria-hidden="true">🛡️</span>`;
    }
  ).join("");

  nodes.streakStrip.innerHTML = `${fires}<span class="shield-stack">${shields}</span>`;
  nodes.currentStreak.textContent = streak.current_streak_days;
  nodes.longestStreak.textContent = streak.longest_historical_streak;
  nodes.shieldCount.textContent = streak.active_shields_count;
  nodes.lifetimePoints.textContent = lifetimePoints;
}

function renderLeaderboard(rows) {
  nodes.leaderboardList.innerHTML = rows
    .map(
      (row) => `
        <article class="leaderboard-row ${row.is_current_agent ? "current" : ""}">
          <span class="rank">#${row.rank}</span>
          <div class="agent-copy">
            <strong>${escapeHtml(row.agent_name)}</strong>
            <span>${escapeHtml(row.branch_name)}</span>
          </div>
          <span class="points">${row.weekly_points_total}</span>
        </article>
      `
    )
    .join("");
}

function renderBranches(rows) {
  nodes.branchGrid.innerHTML = rows
    .map(
      (row) => `
        <article class="branch-tile">
          <span class="rank">#${row.rank}</span>
          <h3>${escapeHtml(row.branch_name)}</h3>
          <p>${escapeHtml(row.city)}</p>
          <div class="branch-metrics">
            <strong>${row.branch_points_total}</strong>
            <span>${row.agent_count} agents · ${row.average_agent_points} avg</span>
          </div>
        </article>
      `
    )
    .join("");
}

function renderLedger(rows) {
  nodes.ledgerRows.innerHTML = rows
    .map(
      (row) => `
        <tr>
          <td>${formatDateTime(row.occurred_at)}</td>
          <td>${escapeHtml(row.event_type.replaceAll("_", " "))}</td>
          <td class="points-cell">+${row.points_awarded}</td>
        </tr>
      `
    )
    .join("");
}

function formatDateTime(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

nodes.agentSelect.addEventListener("change", async (event) => {
  state.agentId = event.target.value;
  await loadDashboard("");
});

nodes.studyForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  nodes.statusLine.textContent = "Saving sprint...";
  const payload = {
    agent_id: state.agentId,
    module_id: nodes.moduleId.value,
    quiz_score: Number(nodes.quizScore.value),
    bio_rhythm_respected: nodes.bioRhythm.checked,
    sprint_minutes: 7,
  };
  const result = await requestJson("/api/complete-study", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  state.dashboard = result.dashboard;
  renderDashboard();
  nodes.statusLine.textContent = `+${result.awarded_total} points posted to ledger`;
});

nodes.runDailyCheck.addEventListener("click", async () => {
  nodes.statusLine.textContent = "Checking streak...";
  const result = await requestJson("/api/run-daily-streak-check", {
    method: "POST",
    body: JSON.stringify({ agent_id: state.agentId }),
  });
  state.dashboard = result.dashboard;
  renderDashboard();
  nodes.statusLine.textContent = result.already_evaluated
    ? `Already checked: ${result.status.replaceAll("_", " ")}`
    : `Daily check: ${result.status.replaceAll("_", " ")}`;
});

nodes.rebuildCache.addEventListener("click", async () => {
  nodes.statusLine.textContent = "Rebuilding weekly standings...";
  state.dashboard = await requestJson("/api/rebuild-weekly-cache", {
    method: "POST",
    body: JSON.stringify({ agent_id: state.agentId }),
  });
  renderDashboard();
  nodes.statusLine.textContent = "Weekly cache rebuilt from PointLedger";
});

document.querySelectorAll(".tab-button").forEach((button) => {
  button.addEventListener("click", () => {
    document.querySelectorAll(".tab-button").forEach((item) => item.classList.remove("active"));
    document.querySelectorAll(".tab-pane").forEach((item) => item.classList.remove("active"));
    button.classList.add("active");
    document.querySelector(`#${button.dataset.tab}Tab`).classList.add("active");
  });
});

async function boot() {
  try {
    await loadAgents();
    await loadDashboard("");
  } catch (error) {
    nodes.statusLine.textContent = error.message;
  }
}

boot();
