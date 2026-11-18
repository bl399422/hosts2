const $ = (id) => document.getElementById(id);

function render(enabled) {
  $("status").textContent = enabled ? "running" : "stopped";
  $("status").parentElement.dataset.enabled = String(enabled);
  $("toggle").textContent = enabled ? "Stop" : "Start";
  $("toggle").title = enabled
    ? "Stop recording network activity."
    : "Start recording network activity.";
}

async function init() {
  const s = await chrome.storage.local.get({ enabled: false, autoStart: false });
  render(s.enabled);
  $("auto").checked = s.autoStart;
}

$("toggle").addEventListener("click", async () => {
  const { enabled } = await chrome.storage.local.get({ enabled: false });
  await chrome.storage.local.set({ enabled: !enabled });
  render(!enabled);
});

$("auto").addEventListener("change", (e) =>
  chrome.storage.local.set({ autoStart: e.target.checked })
);

let pendingClear;

function requestClear(scope, label) {
  pendingClear = { scope, label };
  $("confirmation-text").textContent = `Clear ${label}? This cannot be undone.`;
  $("confirmation").hidden = false;
  $("message").textContent = "";
}

async function clearData() {
  const request = pendingClear;
  if (!request) return;
  pendingClear = null;
  $("confirmation").hidden = true;
  $("message").textContent = "";
  try {
    const result = await chrome.runtime.sendMessage({
      target: "background",
      cmd: "clearData",
      scope: request.scope,
    });
    if (!result?.ok) throw new Error(result?.error || "Could not clear data.");
    $("message").textContent = `${request.label} cleared.`;
  } catch (error) {
    $("message").textContent = `Could not clear ${request.label}: ${error.message}`;
  }
}

$("clear-hosts").addEventListener("click", () => requestClear("hosts", "hosts2 data"));
$("clear-peers").addEventListener("click", () => requestClear("peers", "peers2 data"));
$("clear-all").addEventListener("click", () => requestClear("all", "all data"));
$("confirm-clear").addEventListener("click", clearData);
$("cancel-clear").addEventListener("click", () => {
  pendingClear = null;
  $("confirmation").hidden = true;
});

$("export").addEventListener("click", async () => {
  const { records } = await chrome.storage.local.get({ records: {} });
  const rows = Object.entries(records).map(([id, ts]) => {
    const i = id.lastIndexOf(",");
    return `${id.slice(0, i)},${id.slice(i + 1)},${ts}`;
  });
  rows.sort((a, b) => a.split(",")[2].localeCompare(b.split(",")[2]));
  const csv = ["domain,ip,timestamp", ...rows].join("\n") + "\n";
  const a = document.createElement("a");
  a.href = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
  a.download = "hosts2.csv";
  a.click();
});

$("export-peers").addEventListener("click", async () => {
  const { peers } = await chrome.storage.local.get({ peers: {} });
  const rows = Object.entries(peers)
    .sort((a, b) => a[1].firstSeen.localeCompare(b[1].firstSeen))
    .map(([, p]) => `${p.ip},${p.firstSeen},${p.lastSeen},${!!p.preferred}`);
  const csv = ["ip,first_seen,last_seen,preferred", ...rows].join("\n") + "\n";
  const a = document.createElement("a");
  a.href = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
  a.download = "peers2.csv";
  a.click();
});

init();
