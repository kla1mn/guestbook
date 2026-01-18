const FRONT_VERSION = "0.1.0";

const API_ORIGIN = "https://d5dke8vmsrs8dvkd8qg3.cmxivbes.apigw.yandexcloud.net";
const API = `${API_ORIGIN}/api`;

const el = (id) => document.getElementById(id);

el("frontVer").textContent = FRONT_VERSION;

function setBackendInfo(data, gwReleaseHeader) {
    el("backInfo").textContent = `${data.backend_version} (replica: ${data.replica})`;
    el("gwRelease").textContent = gwReleaseHeader || "stable";
}

function renderMessages(msgs) {
    const ul = el("list");
    ul.innerHTML = "";
    for (const m of msgs) {
        const li = document.createElement("li");
        const dt = new Date(m.created_at);
        li.innerHTML = `<div class="row">
      <div class="who">${escapeHtml(m.name)}</div>
      <div class="when">${dt.toLocaleString()}</div>
    </div>
    <div class="txt">${escapeHtml(m.text)}</div>`;
        ul.appendChild(li);
    }
}

function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, (c) => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;", "'": "&#039;"
    }[c]));
}

async function apiFetch(path, opts) {
    const res = await fetch(`${API}${path}`, {
        ...opts,
        headers: {"Content-Type": "application/json", ...(opts?.headers || {})}
    });

    const gwRelease = res.headers.get("x-yc-apigateway-release");
    const data = await res.json().catch(() => ({}));
    return {res, data, gwRelease};
}

async function load() {
    el("status").textContent = "Загрузка...";
    const {res, data, gwRelease} = await apiFetch("/messages");
    if (!res.ok) {
        el("status").textContent = `Ошибка: ${res.status}`;
        return;
    }
    setBackendInfo(data, gwRelease);
    renderMessages(data.messages || []);
    el("status").textContent = "";
}

el("reload").addEventListener("click", load);

el("form").addEventListener("submit", async (e) => {
    e.preventDefault();
    el("status").textContent = "Отправка...";

    const payload = {
        name: el("name").value,
        text: el("text").value
    };

    const {res, data, gwRelease} = await apiFetch("/messages", {
        method: "POST",
        body: JSON.stringify(payload),
    });

    if (!res.ok) {
        el("status").textContent = data.error ? `Ошибка: ${data.error}` : `Ошибка: ${res.status}`;
        return;
    }

    setBackendInfo(data, gwRelease);
    el("text").value = "";
    await load();
});

load();
