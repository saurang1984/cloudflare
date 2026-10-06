"use strict";

const SESSION_KEY = "atlas-session-id";
const SPEAK_KEY = "atlas-speak-replies";
const SESSION_ID_PATTERN = /^[A-Za-z0-9-]{8,64}$/;
const MEMORY_POLL_MS = 2500;

const els = {
  messages: document.getElementById("messages"),
  emptyState: document.getElementById("empty-state"),
  form: document.getElementById("composer"),
  input: document.getElementById("input"),
  sendBtn: document.getElementById("send-btn"),
  micBtn: document.getElementById("mic-btn"),
  resetBtn: document.getElementById("reset-btn"),
  speakToggle: document.getElementById("speak-toggle"),
  speakToggleLabel: document.getElementById("speak-toggle-label"),
  memoryBadge: document.getElementById("memory-badge"),
  memoryStatus: document.getElementById("memory-status"),
  memorySummary: document.getElementById("memory-summary"),
  memoryFacts: document.getElementById("memory-facts"),
  refreshBtn: document.getElementById("refresh-memory-btn"),
  toast: document.getElementById("toast"),
};

const sessionId = getSessionId();
let busy = false;
let memoryPollTimer = null;
let toastTimer = null;

function getSessionId() {
  let id = localStorage.getItem(SESSION_KEY);
  if (!id || !SESSION_ID_PATTERN.test(id)) {
    id = crypto.randomUUID();
    localStorage.setItem(SESSION_KEY, id);
  }
  return id;
}

function api(path = "") {
  return `/api/sessions/${encodeURIComponent(sessionId)}${path}`;
}

async function requestJson(path, options = {}) {
  const res = await fetch(api(path), options);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.error || `Request failed (${res.status})`);
  }
  return data;
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

function scrollToBottom() {
  els.messages.scrollTop = els.messages.scrollHeight;
}

function updateEmptyState() {
  els.emptyState.hidden = els.messages.querySelector(".message") !== null;
}

function renderMessage(role, content) {
  const row = document.createElement("div");
  row.className = `message ${role}`;
  const bubble = document.createElement("div");
  bubble.className = "bubble";
  bubble.textContent = content;
  row.appendChild(bubble);
  els.messages.appendChild(row);
  updateEmptyState();
  scrollToBottom();
  return bubble;
}

function clearMessages() {
  for (const node of els.messages.querySelectorAll(".message")) {
    node.remove();
  }
  updateEmptyState();
}

function renderMemory(memory) {
  els.memorySummary.textContent =
    memory.summary || "No summary yet. Keep chatting and Atlas will summarize the conversation.";
  els.memorySummary.classList.toggle("muted", !memory.summary);

  els.memoryFacts.replaceChildren();
  if (memory.facts.length === 0) {
    const li = document.createElement("li");
    li.className = "muted";
    li.textContent = "Nothing yet. Tell Atlas about yourself!";
    els.memoryFacts.appendChild(li);
  } else {
    for (const fact of memory.facts) {
      const li = document.createElement("li");
      li.textContent = fact;
      els.memoryFacts.appendChild(li);
    }
  }

  const pending = memory.pendingMessages;
  const pendingText = `${pending} message${pending === 1 ? "" : "s"} not yet in memory`;
  if (memory.job) {
    els.memoryStatus.textContent = `Workflow ${memory.job.status}... ${pendingText}.`;
  } else if (memory.updatedAt) {
    els.memoryStatus.textContent = `Last updated ${new Date(memory.updatedAt).toLocaleString()}. ${pendingText}.`;
  } else {
    els.memoryStatus.textContent = `${pendingText}.`;
  }

  els.memoryBadge.hidden = !memory.job;
  els.refreshBtn.disabled = Boolean(memory.job) || pending === 0;

  if (memory.job) {
    scheduleMemoryPoll();
  }
}

function scheduleMemoryPoll() {
  if (memoryPollTimer) return;
  memoryPollTimer = setTimeout(async () => {
    memoryPollTimer = null;
    try {
      renderMemory(await requestJson("/memory"));
    } catch (err) {
      console.error(err);
      scheduleMemoryPoll();
    }
  }, MEMORY_POLL_MS);
}

function showToast(message) {
  els.toast.textContent = message;
  els.toast.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => {
    els.toast.hidden = true;
  }, 4000);
}

function setBusy(value) {
  busy = value;
  els.sendBtn.disabled = value;
  els.input.disabled = value;
  els.resetBtn.disabled = value;
  if (!els.micBtn.dataset.unsupported) {
    els.micBtn.disabled = value;
  }
}

function autoResize() {
  els.input.style.height = "auto";
  els.input.style.height = `${Math.min(els.input.scrollHeight, 200)}px`;
}

// ---------------------------------------------------------------------------
// Chat
// ---------------------------------------------------------------------------

async function* readServerSentEvents(body) {
  const reader = body.pipeThrough(new TextDecoderStream()).getReader();
  let buffer = "";
  while (true) {
    const { value, done } = await reader.read();
    if (done) return;
    buffer += value;
    let boundary;
    while ((boundary = buffer.indexOf("\n\n")) !== -1) {
      const block = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);
      let event = "message";
      const data = [];
      for (const line of block.split("\n")) {
        if (line.startsWith("event:")) event = line.slice(6).trim();
        else if (line.startsWith("data:")) data.push(line.slice(5).trimStart());
      }
      if (data.length > 0) {
        yield { event, data: JSON.parse(data.join("\n")) };
      }
    }
  }
}

async function sendMessage(text) {
  const message = text.trim();
  if (!message || busy) return;

  setBusy(true);
  els.input.value = "";
  autoResize();
  renderMessage("user", message);
  const bubble = renderMessage("assistant", "");
  bubble.classList.add("streaming");
  let reply = "";

  try {
    const res = await fetch(api("/chat"), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ message }),
    });
    if (!res.ok || !res.body) {
      const data = await res.json().catch(() => ({}));
      throw new Error(data.error || `Request failed (${res.status})`);
    }

    for await (const { event, data } of readServerSentEvents(res.body)) {
      if (event === "token") {
        reply += data.token;
        bubble.textContent = reply;
        scrollToBottom();
      } else if (event === "done") {
        reply = data.assistantMessage.content;
        bubble.textContent = reply;
        renderMemory(data.memory);
      } else if (event === "error") {
        throw new Error(data.message);
      }
    }

    if (els.speakToggle.checked) {
      speak(reply);
    }
  } catch (err) {
    console.error(err);
    bubble.parentElement.classList.add("error");
    bubble.textContent = reply ? `${reply}\n\n[${err.message}]` : err.message;
  } finally {
    bubble.classList.remove("streaming");
    setBusy(false);
    els.input.focus();
  }
}

async function loadSession() {
  try {
    const state = await requestJson("");
    clearMessages();
    for (const message of state.messages) {
      renderMessage(message.role, message.content);
    }
    renderMemory(state.memory);
  } catch (err) {
    console.error(err);
    showToast(`Could not load session: ${err.message}`);
  }
}

async function resetSession() {
  if (!confirm("Delete this conversation and everything Atlas remembers about you?")) return;
  setBusy(true);
  try {
    await requestJson("", { method: "DELETE" });
    window.speechSynthesis?.cancel();
    await loadSession();
    showToast("Session reset.");
  } catch (err) {
    showToast(`Reset failed: ${err.message}`);
  } finally {
    setBusy(false);
  }
}

async function refreshMemory() {
  els.refreshBtn.disabled = true;
  try {
    const result = await requestJson("/memory/refresh", { method: "POST" });
    renderMemory(result.memory);
    showToast(result.reason);
  } catch (err) {
    showToast(`Memory update failed: ${err.message}`);
    els.refreshBtn.disabled = false;
  }
}

// ---------------------------------------------------------------------------
// Voice (Web Speech API)
// ---------------------------------------------------------------------------

function speak(text) {
  if (!("speechSynthesis" in window) || !text) return;
  window.speechSynthesis.cancel();
  const utterance = new SpeechSynthesisUtterance(text);
  utterance.lang = navigator.language || "en-US";
  window.speechSynthesis.speak(utterance);
}

function setupVoiceInput() {
  const Recognition = window.SpeechRecognition || window.webkitSpeechRecognition;
  if (!Recognition) {
    els.micBtn.disabled = true;
    els.micBtn.dataset.unsupported = "true";
    els.micBtn.title = "Voice input is not supported in this browser";
    return;
  }

  const recognition = new Recognition();
  recognition.lang = navigator.language || "en-US";
  recognition.interimResults = true;
  recognition.continuous = false;

  let listening = false;
  let finalTranscript = "";

  recognition.onstart = () => {
    listening = true;
    finalTranscript = "";
    els.micBtn.classList.add("listening");
    els.input.placeholder = "Listening...";
  };

  recognition.onresult = (event) => {
    let interim = "";
    for (let i = event.resultIndex; i < event.results.length; i++) {
      const result = event.results[i];
      if (result.isFinal) finalTranscript += result[0].transcript;
      else interim += result[0].transcript;
    }
    els.input.value = `${finalTranscript}${interim}`.trim();
    autoResize();
  };

  recognition.onerror = (event) => {
    if (event.error !== "no-speech" && event.error !== "aborted") {
      showToast(`Voice input error: ${event.error}`);
    }
  };

  recognition.onend = () => {
    listening = false;
    els.micBtn.classList.remove("listening");
    els.input.placeholder = "Message Atlas... (Enter to send, Shift+Enter for a new line)";
    if (finalTranscript.trim()) {
      sendMessage(finalTranscript);
    }
  };

  els.micBtn.addEventListener("click", () => {
    if (listening) {
      recognition.stop();
    } else if (!busy) {
      window.speechSynthesis?.cancel();
      recognition.start();
    }
  });
}

function setupSpeechOutput() {
  if (!("speechSynthesis" in window)) {
    els.speakToggleLabel.hidden = true;
    return;
  }
  els.speakToggle.checked = localStorage.getItem(SPEAK_KEY) === "true";
  els.speakToggle.addEventListener("change", () => {
    localStorage.setItem(SPEAK_KEY, String(els.speakToggle.checked));
    if (!els.speakToggle.checked) window.speechSynthesis.cancel();
  });
}

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

els.form.addEventListener("submit", (event) => {
  event.preventDefault();
  sendMessage(els.input.value);
});

els.input.addEventListener("keydown", (event) => {
  if (event.key === "Enter" && !event.shiftKey && !event.isComposing) {
    event.preventDefault();
    sendMessage(els.input.value);
  }
});

els.input.addEventListener("input", autoResize);
els.resetBtn.addEventListener("click", resetSession);
els.refreshBtn.addEventListener("click", refreshMemory);

for (const button of document.querySelectorAll(".suggestion")) {
  button.addEventListener("click", () => sendMessage(button.textContent));
}

setupVoiceInput();
setupSpeechOutput();
loadSession();
