const resultViewer = document.getElementById("resultViewer");
const resultStatus = document.getElementById("resultStatus");
const metricSampleSize = document.getElementById("metricSampleSize");
const metricR2 = document.getElementById("metricR2");
const metricPermutationP = document.getElementById("metricPermutationP");
const metricRefreshAt = document.getElementById("metricRefreshAt");

function nowText() {
  return new Date().toLocaleString();
}

function setStatus(type, text) {
  resultStatus.className = `status ${type}`;
  resultStatus.textContent = text;
}

function renderJson(payload) {
  resultViewer.textContent = JSON.stringify(payload, null, 2);
  metricRefreshAt.textContent = nowText();
}

function updateMetricsFromCase(payload) {
  const sampleSize = payload && Number.isFinite(payload.sample_size) ? String(payload.sample_size) : "-";
  const r2 = payload && payload.ols && Number.isFinite(payload.ols.r2) ? payload.ols.r2.toFixed(4) : "-";
  const p =
    payload &&
    payload.momentum_group_mean_test &&
    Number.isFinite(payload.momentum_group_mean_test.p_value_permutation)
      ? payload.momentum_group_mean_test.p_value_permutation.toFixed(4)
      : "-";
  metricSampleSize.textContent = sampleSize;
  metricR2.textContent = r2;
  metricPermutationP.textContent = p;
}

function updateMetricsFallback(payload) {
  if (payload && Number.isFinite(payload.n)) {
    metricSampleSize.textContent = String(payload.n);
  } else if (payload && Number.isFinite(payload.row_count)) {
    metricSampleSize.textContent = String(payload.row_count);
  }
}

function buildRetestDemoRows() {
  const rows = [];
  for (let i = 0; i < 60; i += 1) {
    const math = 95 + ((i * 7) % 41) + ((i % 5) - 2);
    const english = 55 + ((i * 5) % 31) + ((i % 7) - 3);
    const stat = Math.round(0.52 * math + 0.58 * english + 18 + ((i % 6) - 3) * 1.2);
    rows.push({ math, english, stat });
  }
  return rows;
}

function buildTwoSampleDemoData() {
  const sampleA = [];
  const sampleB = [];
  for (let i = 0; i < 24; i += 1) {
    const high = 0.004 + i * 0.00035 + ((i % 4) - 1.5) * 0.00025;
    const low = -0.001 + i * 0.00015 + ((i % 5) - 2) * 0.0002;
    sampleA.push(Number(high.toFixed(6)));
    sampleB.push(Number(low.toFixed(6)));
  }
  return { sampleA, sampleB };
}

function buildOlsDemoData() {
  const studyHours = [];
  const attendanceRate = [];
  const simulationScore = [];
  const target = [];
  for (let i = 0; i < 36; i += 1) {
    const study = 2 + (i % 8) + Math.floor(i / 12);
    const attend = Math.min(0.98, 0.62 + (i % 10) * 0.025 + ((i % 3) - 1) * 0.01);
    const sim = 52 + ((i * 3) % 38) + ((i % 5) - 2);
    const y = 25 + 4.2 * study + 36 * attend + 0.45 * sim + ((i % 6) - 3) * 1.8;
    studyHours.push(Number(study.toFixed(3)));
    attendanceRate.push(Number(attend.toFixed(4)));
    simulationScore.push(Number(sim.toFixed(3)));
    target.push(Number(y.toFixed(3)));
  }
  return { target, studyHours, attendanceRate, simulationScore };
}

async function postJson(url, body) {
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify(body),
  });
  let payload;
  try {
    payload = await resp.json();
  } catch {
    payload = { detail: "Invalid JSON response" };
  }
  if (!resp.ok) {
    const detail = payload && payload.detail ? payload.detail : `HTTP ${resp.status}`;
    throw new Error(String(detail));
  }
  return payload;
}

async function runDescriptiveDemo() {
  setStatus("running", "运行中");
  const rows = buildRetestDemoRows();
  try {
    const payload = await postJson("/applied-stats/descriptive", {
      dataset_name: "retest_demo_n60",
      rows,
      columns: ["math", "english", "stat"],
    });
    renderJson(payload);
    updateMetricsFallback(payload);
    setStatus("success", `描述统计已完成（样本量 ${rows.length}）`);
  } catch (error) {
    renderJson({ error: String(error) });
    setStatus("error", "执行失败");
  }
}

async function runTwoSampleDemo() {
  setStatus("running", "运行中");
  const { sampleA, sampleB } = buildTwoSampleDemoData();
  try {
    const payload = await postJson("/applied-stats/tests/two-sample-mean", {
      sample_a: sampleA,
      sample_b: sampleB,
      group_a_name: "high_group",
      group_b_name: "low_group",
      equal_var: false,
      permutations: 2000,
      random_seed: 42,
    });
    renderJson(payload);
    metricPermutationP.textContent = Number.isFinite(payload.p_value_permutation)
      ? payload.p_value_permutation.toFixed(4)
      : "-";
    setStatus("success", `双样本检验已完成（n=${sampleA.length} vs ${sampleB.length}）`);
  } catch (error) {
    renderJson({ error: String(error) });
    setStatus("error", "执行失败");
  }
}

async function runOlsDemo() {
  setStatus("running", "运行中");
  const olsDemo = buildOlsDemoData();
  try {
    const payload = await postJson("/applied-stats/model/ols", {
      target: olsDemo.target,
      features: {
        study_hours: olsDemo.studyHours,
        attendance_rate: olsDemo.attendanceRate,
        simulation_score: olsDemo.simulationScore,
      },
    });
    renderJson(payload);
    updateMetricsFallback(payload);
    metricR2.textContent = Number.isFinite(payload.r2) ? payload.r2.toFixed(4) : "-";
    setStatus("success", `回归分析已完成（样本量 ${olsDemo.target.length}）`);
  } catch (error) {
    renderJson({ error: String(error) });
    setStatus("error", "执行失败");
  }
}

async function runMarketFactorStudy(event) {
  if (event) event.preventDefault();
  setStatus("running", "运行中");
  const symbol = document.getElementById("symbolInput").value.trim();
  const startDate = document.getElementById("startDateInput").value;
  const endDate = document.getElementById("endDateInput").value;
  const includeFundamentals = document.getElementById("includeFundamentalsInput").checked;
  const exportMarkdown = document.getElementById("exportMarkdownInput").checked;
  try {
    const payload = await postJson("/applied-stats/cases/market-factor-study", {
      symbol,
      start_date: startDate,
      end_date: endDate,
      include_fundamentals: includeFundamentals,
      permutations: 2000,
      bootstrap_samples: 2000,
      random_seed: 42,
      export_markdown: exportMarkdown,
    });
    renderJson(payload);
    updateMetricsFromCase(payload);
    setStatus("success", "完整案例已完成");
  } catch (error) {
    renderJson({ error: String(error) });
    setStatus("error", "执行失败");
  }
}

document.getElementById("runDescriptiveDemoBtn").addEventListener("click", runDescriptiveDemo);
document.getElementById("runTwoSampleDemoBtn").addEventListener("click", runTwoSampleDemo);
document.getElementById("runOlsDemoBtn").addEventListener("click", runOlsDemo);
document.getElementById("marketFactorStudyForm").addEventListener("submit", runMarketFactorStudy);

metricRefreshAt.textContent = nowText();
renderJson({
  message:
    "请选择一个演示动作开始。提示：描述统计演示默认使用 60 个样本，市场数据案例样本量由时间区间决定。",
});
