(() => {
  "use strict";

  const $ = (id) => document.getElementById(id);

  const elements = {
    gatewayConnection: $("gatewayConnection"),
    stateConnection: $("stateConnection"),
    imageConnection: $("imageConnection"),
    recordOverview: document.querySelector(".record-overview"),
    recordState: $("recordState"),
    readinessBadge: $("readinessBadge"),
    recordHint: $("recordHint"),
    recordDuration: $("recordDuration"),
    recordFrames: $("recordFrames"),
    episodeCount: $("episodeCount"),
    recordPathLabel: $("recordPathLabel"),
    recordPath: $("recordPath"),
    cameraGrid: $("cameraGrid"),
    cameraEmpty: $("cameraEmpty"),
    cameraCount: $("cameraCount"),
    collectionForm: $("collectionForm"),
    taskName: $("taskName"),
    episodeId: $("episodeId"),
    operatorName: $("operatorName"),
    outputPath: $("outputPath"),
    notes: $("notes"),
    formError: $("formError"),
    regeneratePathButton: $("regeneratePathButton"),
    startButton: $("startButton"),
    stopButton: $("stopButton"),
    discardButton: $("discardButton"),
    resetButton: $("resetButton"),
    commandStatus: $("commandStatus"),
    readinessSummary: $("readinessSummary"),
    readinessList: $("readinessList"),
    jointCount: $("jointCount"),
    jointTableBody: $("jointTableBody"),
    skippedFrames: $("skippedFrames"),
    coalescedInputs: $("coalescedInputs"),
    gatewayFrames: $("gatewayFrames"),
    gatewayUptime: $("gatewayUptime"),
    simState: $("simState"),
    diagnosticMessage: $("diagnosticMessage"),
    confirmDialog: $("confirmDialog"),
    confirmTitle: $("confirmTitle"),
    confirmMessage: $("confirmMessage"),
    confirmActionButton: $("confirmActionButton"),
    toastRegion: $("toastRegion"),
  };

  const storageKeys = {
    taskName: "forge.collector.taskName",
    operatorName: "forge.collector.operatorName",
  };

  const app = {
    closing: false,
    lastStateMessageAt: 0,
    snapshot: null,
    readiness: null,
    recordStatus: null,
    recordState: "UNKNOWN",
    recordObservedAt: null,
    activePath: "",
    lastPath: "",
    lastPathOutcome: "",
    lastRecorderError: "",
    pendingCommand: null,
    commandFeedback: null,
    resetBusy: false,
    pathIsAutomatic: true,
    episodeWasEdited: false,
    initialEpisodeSynced: false,
    renderFrame: 0,
    queuedSnapshot: null,
    cameras: new Map(),
    jointSignature: "",
    sockets: {
      state: createSocketState("/ws/state", handleStateMessage),
      images: createSocketState("/ws/images", handleImageMessage),
    },
  };

  function createSocketState(path, messageHandler) {
    return {
      path,
      messageHandler,
      socket: null,
      connected: false,
      connecting: false,
      attempts: 0,
      retryTimer: 0,
    };
  }

  function readStorage(key, fallback = "") {
    try {
      return localStorage.getItem(key) || fallback;
    } catch (_error) {
      return fallback;
    }
  }

  function writeStorage(key, value) {
    try {
      localStorage.setItem(key, value);
    } catch (_error) {
      // Collection must keep working when storage is disabled.
    }
  }

  function initializeForm() {
    elements.taskName.value = readStorage(storageKeys.taskName, elements.taskName.value);
    elements.operatorName.value = readStorage(storageKeys.operatorName, "");
    regenerateOutputPath();

    elements.taskName.addEventListener("input", () => {
      writeStorage(storageKeys.taskName, elements.taskName.value.trim());
      if (app.pathIsAutomatic) regenerateOutputPath();
      clearFormError();
    });
    elements.episodeId.addEventListener("input", () => {
      app.episodeWasEdited = true;
      if (app.pathIsAutomatic) regenerateOutputPath();
      clearFormError();
    });
    elements.operatorName.addEventListener("input", () => {
      writeStorage(storageKeys.operatorName, elements.operatorName.value.trim());
    });
    elements.outputPath.addEventListener("input", () => {
      app.pathIsAutomatic = false;
      clearFormError();
    });
    elements.regeneratePathButton.addEventListener("click", () => {
      app.pathIsAutomatic = true;
      regenerateOutputPath();
      clearFormError();
    });
  }

  function slugify(value, fallback) {
    const normalized = String(value || "")
      .normalize("NFKC")
      .trim()
      .toLowerCase()
      .replace(/[^\p{Letter}\p{Number}]+/gu, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 64);
    return normalized || fallback;
  }

  function localTimestamp(date = new Date()) {
    const pad = (value) => String(value).padStart(2, "0");
    return [
      date.getFullYear(),
      pad(date.getMonth() + 1),
      pad(date.getDate()),
      "-",
      pad(date.getHours()),
      pad(date.getMinutes()),
      pad(date.getSeconds()),
      "-",
      String(date.getMilliseconds()).padStart(3, "0"),
    ].join("");
  }

  function regenerateOutputPath() {
    const task = slugify(elements.taskName.value, "collection");
    const episode = slugify(elements.episodeId.value, "episode");
    elements.outputPath.value = `./recordings/${task}_${episode}_${localTimestamp()}.mcap`;
    app.pathIsAutomatic = true;
  }

  function incrementEpisodeId(value) {
    const current = String(value || "episode-001").trim();
    const match = current.match(/^(.*?)(\d+)$/);
    if (!match) return `${current || "episode"}-002`;
    const next = String(Number(match[2]) + 1).padStart(match[2].length, "0");
    return `${match[1]}${next}`;
  }

  function prepareNextCollection() {
    elements.episodeId.value = incrementEpisodeId(elements.episodeId.value);
    app.episodeWasEdited = false;
    app.pathIsAutomatic = true;
    regenerateOutputPath();
  }

  function setConnectionPill(element, status, text) {
    element.classList.remove("is-online", "is-offline", "is-connecting");
    element.classList.add(`is-${status}`);
    const dot = document.createElement("span");
    dot.className = "status-dot";
    dot.setAttribute("aria-hidden", "true");
    element.replaceChildren(dot, document.createTextNode(text));
  }

  function stateStreamIsFresh() {
    return (
      app.sockets.state.connected &&
      app.lastStateMessageAt > 0 &&
      Date.now() - app.lastStateMessageAt < 3000
    );
  }

  function refreshConnectionPills() {
    const stateSocket = app.sockets.state;
    const imageSocket = app.sockets.images;
    const stateFresh = stateStreamIsFresh();

    if (stateFresh) {
      setConnectionPill(elements.stateConnection, "online", "状态流正常");
    } else if (stateSocket.connected) {
      setConnectionPill(elements.stateConnection, "connecting", "状态流停滞");
    } else if (stateSocket.connecting) {
      setConnectionPill(elements.stateConnection, "connecting", "连接状态流");
    } else {
      setConnectionPill(elements.stateConnection, "offline", "状态流断开");
    }

    if (imageSocket.connected) {
      setConnectionPill(elements.imageConnection, "online", "图像流正常");
    } else if (imageSocket.connecting) {
      setConnectionPill(elements.imageConnection, "connecting", "连接图像流");
    } else {
      setConnectionPill(elements.imageConnection, "offline", "图像流断开");
    }

    if (stateFresh) {
      setConnectionPill(elements.gatewayConnection, "online", "Gateway 在线");
    } else if (stateSocket.connecting || imageSocket.connecting || stateSocket.connected) {
      setConnectionPill(elements.gatewayConnection, "connecting", "Gateway 连接中");
    } else {
      setConnectionPill(elements.gatewayConnection, "offline", "Gateway 离线");
    }

    renderControls();
  }

  function socketUrl(path) {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    return `${protocol}//${window.location.host}${path}`;
  }

  function connectSocket(name) {
    const channel = app.sockets[name];
    if (
      app.closing ||
      !navigator.onLine ||
      channel.connecting ||
      (channel.socket && channel.socket.readyState <= WebSocket.OPEN)
    ) {
      return;
    }

    window.clearTimeout(channel.retryTimer);
    channel.retryTimer = 0;
    channel.connecting = true;
    refreshConnectionPills();

    let socket;
    try {
      socket = new WebSocket(socketUrl(channel.path));
    } catch (_error) {
      channel.connecting = false;
      scheduleReconnect(name);
      return;
    }
    channel.socket = socket;

    socket.addEventListener("open", () => {
      if (channel.socket !== socket) return;
      channel.connected = true;
      channel.connecting = false;
      channel.attempts = 0;
      refreshConnectionPills();
    });

    socket.addEventListener("message", (event) => {
      if (channel.socket !== socket) return;
      try {
        channel.messageHandler(JSON.parse(event.data));
      } catch (error) {
        console.warn(`Invalid ${name} websocket payload`, error);
      }
    });

    socket.addEventListener("error", () => {
      socket.close();
    });

    socket.addEventListener("close", () => {
      if (channel.socket !== socket) return;
      channel.socket = null;
      channel.connected = false;
      channel.connecting = false;
      refreshConnectionPills();
      scheduleReconnect(name);
    });
  }

  function scheduleReconnect(name, immediate = false) {
    const channel = app.sockets[name];
    if (app.closing || !navigator.onLine || channel.retryTimer) return;
    const baseDelay = immediate ? 0 : Math.min(15000, 700 * 2 ** channel.attempts);
    const jitter = immediate ? 0 : Math.round(Math.random() * 300);
    channel.attempts = Math.min(channel.attempts + 1, 6);
    channel.retryTimer = window.setTimeout(() => {
      channel.retryTimer = 0;
      connectSocket(name);
    }, baseDelay + jitter);
  }

  function reconnectNow(name) {
    const channel = app.sockets[name];
    window.clearTimeout(channel.retryTimer);
    channel.retryTimer = 0;
    if (!channel.socket || channel.socket.readyState >= WebSocket.CLOSING) {
      channel.socket = null;
      channel.connected = false;
      channel.connecting = false;
      scheduleReconnect(name, true);
    }
  }

  function handleStateMessage(snapshot) {
    if (!snapshot || typeof snapshot !== "object") return;
    app.lastStateMessageAt = Date.now();
    app.queuedSnapshot = snapshot;
    if (!app.renderFrame) {
      app.renderFrame = window.requestAnimationFrame(() => {
        app.renderFrame = 0;
        const next = app.queuedSnapshot;
        app.queuedSnapshot = null;
        if (next) renderSnapshot(next);
      });
    }
  }

  function renderSnapshot(snapshot) {
    app.snapshot = snapshot;
    const runtime = objectValue(snapshot.runtime);
    const readiness = objectValue(runtime.readiness);
    const recordStatus = objectValue(runtime.record_status);
    const sensors = objectValue(snapshot.sensors);

    app.readiness = readiness;
    Object.keys(objectValue(readiness.images)).forEach(ensureCamera);
    renderReadiness(readiness);
    observeRecordStatus(recordStatus);
    renderJoints(objectValue(sensors.joints), objectValue(sensors.command));
    renderDiagnostics(snapshot, runtime, recordStatus);
    refreshConnectionPills();
  }

  function objectValue(value) {
    return value && typeof value === "object" && !Array.isArray(value) ? value : {};
  }

  function normalizeRecordState(value) {
    const normalized = String(value || "UNKNOWN").toUpperCase();
    return normalized === "RECORDING" || normalized === "IDLE" ? normalized : "UNKNOWN";
  }

  function observeRecordStatus(status) {
    const previous = app.recordState;
    const next = normalizeRecordState(status.record_state);
    const recorderError = typeof status.error === "string" ? status.error.trim() : "";
    const pendingAction = app.pendingCommand ? app.pendingCommand.action : "";

    app.recordStatus = status;
    app.recordState = next;

    if (!app.initialEpisodeSynced && Number.isFinite(Number(status.episode_count))) {
      app.initialEpisodeSynced = true;
      const completed = Number(status.episode_count);
      if (!app.episodeWasEdited && completed > 0 && elements.episodeId.value === "episode-001") {
        elements.episodeId.value = `episode-${String(completed + 1).padStart(3, "0")}`;
        if (app.pathIsAutomatic) regenerateOutputPath();
      }
    }

    if (next === "RECORDING") {
      if (previous !== "RECORDING") {
        app.recordObservedAt = Date.now();
        app.activePath = String(
          status.current_output_path ||
          (app.pendingCommand && app.pendingCommand.requestedPath) ||
          app.activePath ||
          elements.outputPath.value,
        );
        if (previous === "UNKNOWN") {
          notify("检测到录制进行中", app.activePath || "Recorder 已进入 RECORDING", "warning");
        } else {
          notify("录制已开始", app.activePath, "success");
        }
      } else if (status.current_output_path) {
        app.activePath = String(status.current_output_path);
      }
    }

    if (next === "IDLE" && previous === "RECORDING") {
      const completedPath = app.activePath || String(status.current_output_path || elements.outputPath.value);
      app.lastPath = completedPath;
      app.lastPathOutcome = recorderError
        ? "error"
        : pendingAction === "DISCARD"
          ? "discarded"
          : "saved";
      app.activePath = "";
      app.recordObservedAt = null;

      if (recorderError) {
        notify("录制结束但 recorder 报错", recorderError, "error", 9000);
      } else if (pendingAction === "DISCARD") {
        notify("本次采集已丢弃", completedPath, "warning");
      } else {
        notify("MCAP 已保留", completedPath, "success");
      }
      if (recorderError) {
        app.pathIsAutomatic = true;
        regenerateOutputPath();
      } else {
        prepareNextCollection();
      }
    }

    if (app.pendingCommand && next === app.pendingCommand.expectedState) {
      const action = app.pendingCommand.action;
      settlePendingCommand();
      if (action === "START") {
        setCommandFeedback("Recorder 已确认开始写入", "success", 3500);
      } else if (action === "STOP") {
        setCommandFeedback(
          recorderError ? `结束录制时发生错误：${recorderError}` : "录制已结束，文件已保留",
          recorderError ? "error" : "success",
          6500,
        );
      } else {
        setCommandFeedback(
          recorderError ? `丢弃录制失败：${recorderError}` : "当前 session 已丢弃",
          recorderError ? "error" : "success",
          recorderError ? 9000 : 5000,
        );
      }
    }

    if (recorderError && recorderError !== app.lastRecorderError) {
      app.lastRecorderError = recorderError;
      if (app.pendingCommand && app.pendingCommand.action === "START" && next !== "RECORDING") {
        settlePendingCommand();
        setCommandFeedback(`Recorder 拒绝开始：${recorderError}`, "error", 9000);
      }
    } else if (!recorderError) {
      app.lastRecorderError = "";
    }

    renderRecordOverview();
    renderControls();
  }

  function renderRecordOverview() {
    const status = app.recordStatus || {};
    const recording = app.recordState === "RECORDING";
    elements.recordOverview.classList.toggle("is-recording", recording);
    elements.recordState.textContent = recording
      ? "正在录制"
      : app.recordState === "IDLE"
        ? "待机"
        : "等待 Recorder";
    elements.recordFrames.textContent = formatInteger(status.current_frame_count);
    elements.episodeCount.textContent = formatInteger(status.episode_count);

    const displayPath = recording
      ? String(status.current_output_path || app.activePath || "")
      : app.lastPath;
    elements.recordPath.textContent = displayPath || "—";
    elements.recordPath.title = displayPath;
    elements.recordPathLabel.textContent = recording
      ? "当前输出路径"
      : app.lastPathOutcome === "saved"
        ? "最近保留路径"
        : app.lastPathOutcome === "discarded"
          ? "最近丢弃路径"
          : app.lastPathOutcome === "error"
            ? "异常结束路径"
            : "输出路径";

    const readinessReady = Boolean(app.readiness && app.readiness.ready);
    elements.readinessBadge.classList.remove("is-ready", "is-blocked", "is-waiting");
    if (!stateStreamIsFresh()) {
      elements.readinessBadge.classList.add("is-waiting");
      elements.readinessBadge.textContent = "状态未知";
    } else if (readinessReady) {
      elements.readinessBadge.classList.add("is-ready");
      elements.readinessBadge.textContent = "数据已就绪";
    } else {
      elements.readinessBadge.classList.add("is-blocked");
      elements.readinessBadge.textContent = "数据未就绪";
    }

    if (recording) {
      elements.recordHint.textContent = "请完成当前操作后选择保留或丢弃";
    } else if (!stateStreamIsFresh()) {
      elements.recordHint.textContent = "正在等待 Gateway 状态流";
    } else if (!readinessReady) {
      elements.recordHint.textContent = missingReadinessText(app.readiness);
    } else {
      elements.recordHint.textContent = "相机与机器人状态正常，可以开始采集";
    }
    updateDuration();
  }

  function missingReadinessText(readiness) {
    const missing = Array.isArray(readiness && readiness.missing) ? readiness.missing : [];
    if (!missing.length) return "等待必要数据源";
    return `缺少：${missing.map(readinessLabel).join("、")}`;
  }

  function readinessLabel(value) {
    const raw = String(value);
    if (raw === "proprio_state") return "本体状态";
    if (raw === "ws:state") return "状态客户端";
    if (raw === "ws:images") return "图像客户端";
    if (raw.startsWith("image:")) return raw.slice(6);
    return raw;
  }

  function renderReadiness(readiness) {
    const rows = [];
    rows.push({
      label: "本体状态 proprio_state",
      ready: Boolean(readiness.proprio_state_ready),
    });

    const images = objectValue(readiness.images);
    Object.keys(images).forEach((id) => {
      rows.push({ label: `相机 ${id}`, ready: Boolean(images[id]) });
      const camera = ensureCamera(id);
      camera.ready = Boolean(images[id]);
      camera.card.classList.toggle("is-ready", camera.ready);
    });

    rows.push({ label: "Gateway 状态 WebSocket", ready: stateStreamIsFresh() });
    if (Object.keys(images).length > 0) {
      rows.push({ label: "Gateway 图像 WebSocket", ready: app.sockets.images.connected });
    }

    const missing = new Set(
      Array.isArray(readiness.missing) ? readiness.missing.map((item) => String(item)) : [],
    );
    if (missing.has("ws:state")) rows.push({ label: "配置要求状态客户端", ready: false });
    if (missing.has("ws:images")) rows.push({ label: "配置要求图像客户端", ready: false });

    elements.readinessList.replaceChildren(
      ...rows.map((row) => {
        const wrapper = document.createElement("div");
        wrapper.className = `check-row ${row.ready ? "is-ready" : "is-missing"}`;
        const mark = document.createElement("span");
        mark.className = "check-mark";
        mark.setAttribute("aria-hidden", "true");
        const text = document.createElement("span");
        text.textContent = row.label;
        wrapper.append(mark, text);
        return wrapper;
      }),
    );

    const ready = Boolean(readiness.ready);
    elements.readinessSummary.classList.remove("is-ready", "is-blocked");
    elements.readinessSummary.classList.add(ready ? "is-ready" : "is-blocked");
    elements.readinessSummary.textContent = ready ? "全部通过" : `${missing.size || "部分"} 项待处理`;
  }

  function ensureCamera(id) {
    const cameraId = String(id);
    const existing = app.cameras.get(cameraId);
    if (existing) return existing;

    const card = document.createElement("article");
    card.className = "camera-card";

    const image = document.createElement("img");
    image.alt = `${cameraId} 实时画面`;
    image.decoding = "async";

    const placeholder = document.createElement("div");
    placeholder.className = "camera-placeholder";
    const placeholderSymbol = document.createElement("span");
    placeholderSymbol.className = "camera-placeholder-symbol";
    placeholderSymbol.setAttribute("aria-hidden", "true");
    const placeholderText = document.createElement("span");
    placeholderText.textContent = "等待首帧";
    placeholder.append(placeholderSymbol, placeholderText);

    const readyDot = document.createElement("span");
    readyDot.className = "camera-ready-dot";
    readyDot.setAttribute("aria-hidden", "true");

    const overlay = document.createElement("div");
    overlay.className = "camera-overlay";
    const name = document.createElement("span");
    name.className = "camera-name";
    name.textContent = cameraId;
    name.title = cameraId;
    const meta = document.createElement("span");
    meta.className = "camera-meta";
    meta.textContent = "NO SIGNAL";
    overlay.append(name, meta);

    card.append(image, placeholder, readyDot, overlay);
    const camera = {
      id: cameraId,
      card,
      image,
      meta,
      ready: false,
      receivedAt: 0,
      sourceTimestamp: 0,
      sequence: 0,
      objectUrl: "",
      decodeErrorShown: false,
    };
    app.cameras.set(cameraId, camera);
    sortCameraCards();
    return camera;
  }

  function sortCameraCards() {
    const cameras = Array.from(app.cameras.values()).sort((a, b) => a.id.localeCompare(b.id));
    elements.cameraEmpty.hidden = cameras.length > 0;
    cameras.forEach((camera) => elements.cameraGrid.append(camera.card));
    elements.cameraGrid.classList.toggle("single-camera", cameras.length === 1);
    elements.cameraCount.textContent = `${cameras.length} 路`;
  }

  function handleImageMessage(message) {
    if (!message || message.type !== "image" || !message.id || !message.data) return;
    const camera = ensureCamera(message.id);
    let blob;
    try {
      blob = base64ImageBlob(
        String(message.data),
        String(message.content_type || "image/jpeg"),
      );
    } catch (error) {
      if (!camera.decodeErrorShown) {
        camera.decodeErrorShown = true;
        notify(`无法解码 ${camera.id}`, String(error.message || error), "error");
      }
      return;
    }

    if (camera.objectUrl) URL.revokeObjectURL(camera.objectUrl);
    camera.objectUrl = URL.createObjectURL(blob);
    camera.image.src = camera.objectUrl;
    camera.card.classList.add("has-image");
    camera.receivedAt = Date.now();
    camera.sourceTimestamp = Number(message.timestamp) || 0;
    camera.sequence = Number(message.seq) || camera.sequence + 1;
    camera.decodeErrorShown = false;
    updateCameraAge(camera);
  }

  function base64ImageBlob(base64, contentType) {
    const safeContentType = contentType.startsWith("image/") ? contentType : "image/jpeg";
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index += 1) {
      bytes[index] = binary.charCodeAt(index);
    }
    return new Blob([bytes], { type: safeContentType });
  }

  function updateCameraAge(camera) {
    if (!camera.receivedAt) {
      camera.meta.textContent = "NO SIGNAL";
      camera.card.classList.add("is-stale");
      return;
    }
    const ageMs = Math.max(0, Date.now() - camera.receivedAt);
    const sourceLatencyMs = camera.sourceTimestamp
      ? Math.max(0, Date.now() - camera.sourceTimestamp * 1000)
      : ageMs;
    const ageText = ageMs < 1000 ? `${Math.round(sourceLatencyMs)} ms` : `${(ageMs / 1000).toFixed(1)} s 前`;
    camera.meta.textContent = `#${camera.sequence} · ${ageText}`;
    camera.card.classList.toggle("is-stale", ageMs > 2500 || !camera.ready);
  }

  function renderJoints(joints, commands) {
    const names = [...Object.keys(joints), ...Object.keys(commands).filter((name) => !(name in joints))];
    const signature = names.join("\u0000");
    if (signature !== app.jointSignature) {
      app.jointSignature = signature;
      elements.jointTableBody.replaceChildren(
        ...names.map((name) => {
          const row = document.createElement("tr");
          row.dataset.joint = name;
          const nameCell = document.createElement("td");
          nameCell.textContent = name;
          nameCell.title = name;
          const measuredCell = document.createElement("td");
          measuredCell.className = "measured-value";
          const commandCell = document.createElement("td");
          commandCell.className = "command-value";
          const deltaCell = document.createElement("td");
          deltaCell.className = "delta-value";
          row.append(nameCell, measuredCell, commandCell, deltaCell);
          return row;
        }),
      );
      if (!names.length) {
        const row = document.createElement("tr");
        row.className = "placeholder-row";
        const cell = document.createElement("td");
        cell.colSpan = 4;
        cell.textContent = "等待 proprio_state / action";
        row.append(cell);
        elements.jointTableBody.append(row);
      }
      elements.jointCount.textContent = `${names.length} joints`;
    }

    names.forEach((name, index) => {
      const row = elements.jointTableBody.rows[index];
      if (!row) return;
      const measured = finiteNumber(joints[name]);
      const command = finiteNumber(commands[name]);
      const delta = measured !== null && command !== null ? command - measured : null;
      row.querySelector(".measured-value").textContent = formatValue(measured);
      row.querySelector(".command-value").textContent = formatValue(command);
      const deltaCell = row.querySelector(".delta-value");
      deltaCell.textContent = formatSignedValue(delta);
      deltaCell.classList.toggle("delta-high", delta !== null && Math.abs(delta) > 0.1);
    });
  }

  function finiteNumber(value) {
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
  }

  function formatValue(value) {
    if (value === null) return "—";
    if (Math.abs(value) >= 1000) return value.toFixed(1);
    if (Math.abs(value) >= 100) return value.toFixed(2);
    return value.toFixed(4);
  }

  function formatSignedValue(value) {
    if (value === null) return "—";
    const prefix = value > 0 ? "+" : "";
    return `${prefix}${formatValue(value)}`;
  }

  function renderDiagnostics(snapshot, runtime, recordStatus) {
    elements.skippedFrames.textContent = formatInteger(recordStatus.skipped_frame_count);
    elements.coalescedInputs.textContent = formatInteger(recordStatus.coalesced_input_count);
    elements.gatewayFrames.textContent = formatInteger(snapshot.current_frame_count);
    elements.gatewayUptime.textContent = formatCompactDuration(Number(snapshot.running_time));

    const simStatus = objectValue(runtime.sim_status);
    const simName = String(simStatus.status_name || simStatus.status || "UNINITIALIZED");
    elements.simState.textContent = simName;
    elements.simState.title = simName;

    const errors = [];
    if (recordStatus.error) errors.push(`Recorder: ${recordStatus.error}`);
    if (runtime.last_error) errors.push(`Gateway: ${runtime.last_error}`);
    elements.diagnosticMessage.classList.toggle("has-error", errors.length > 0);
    elements.diagnosticMessage.textContent = errors.length ? errors.join(" · ") : "暂无运行错误";
  }

  function formatInteger(value) {
    const number = Number(value);
    return Number.isFinite(number) ? Math.max(0, Math.trunc(number)).toLocaleString("zh-CN") : "0";
  }

  function formatCompactDuration(seconds) {
    if (!Number.isFinite(seconds) || seconds < 0) return "—";
    if (seconds < 60) return `${Math.floor(seconds)}s`;
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes}m ${Math.floor(seconds % 60)}s`;
    return `${Math.floor(minutes / 60)}h ${minutes % 60}m`;
  }

  function updateDuration() {
    if (app.recordState !== "RECORDING" || !app.recordObservedAt) {
      elements.recordDuration.textContent = "00:00:00";
      return;
    }
    const elapsed = Math.max(0, Math.floor((Date.now() - app.recordObservedAt) / 1000));
    const hours = String(Math.floor(elapsed / 3600)).padStart(2, "0");
    const minutes = String(Math.floor((elapsed % 3600) / 60)).padStart(2, "0");
    const seconds = String(elapsed % 60).padStart(2, "0");
    elements.recordDuration.textContent = `${hours}:${minutes}:${seconds}`;
  }

  function cameraStreamRequired() {
    return app.cameras.size > 0;
  }

  function renderControls() {
    const stateFresh = stateStreamIsFresh();
    const readinessReady = Boolean(app.readiness && app.readiness.ready);
    const imageViewReady = !cameraStreamRequired() || app.sockets.images.connected;
    const recording = app.recordState === "RECORDING";
    const pending = Boolean(app.pendingCommand);
    const busy = pending || app.resetBusy;

    elements.startButton.disabled =
      !stateFresh ||
      !readinessReady ||
      !imageViewReady ||
      app.recordState !== "IDLE" ||
      busy;
    // Never trap an operator in a recording when only the status stream is stale.
    // The HTTP request can still succeed, and a failed request remains actionable.
    elements.stopButton.disabled = !recording || busy;
    elements.discardButton.disabled = !recording || busy;
    elements.resetButton.disabled = !stateFresh || recording || busy;
    elements.regeneratePathButton.disabled = recording || busy;

    [
      elements.taskName,
      elements.episodeId,
      elements.operatorName,
      elements.outputPath,
      elements.notes,
    ].forEach((field) => {
      field.disabled = recording || busy;
    });

    if (app.pendingCommand) {
      const actionLabels = {
        START: "正在等待 Recorder 确认开始…",
        STOP: "正在结束并写入 MCAP 索引…",
        DISCARD: "正在结束并删除当前 MCAP…",
      };
      setCommandStatus(actionLabels[app.pendingCommand.action], "pending");
      return;
    }

    if (app.resetBusy) {
      setCommandStatus("场景复位命令发送中…", "pending");
      return;
    }

    if (app.commandFeedback && app.commandFeedback.expiresAt > Date.now()) {
      setCommandStatus(app.commandFeedback.message, app.commandFeedback.type);
      return;
    }
    app.commandFeedback = null;

    if (recording) {
      setCommandStatus(
        stateFresh
          ? "录制进行中；结束后请选择保留或丢弃"
          : "状态流已中断；仍可发送停止或丢弃命令",
        stateFresh ? "success" : "error",
      );
    } else if (!stateFresh) {
      setCommandStatus("等待 Gateway 状态流，开始与复位按钮已锁定", "error");
    } else if (!readinessReady) {
      setCommandStatus(missingReadinessText(app.readiness), "pending");
    } else if (!imageViewReady) {
      setCommandStatus("图像 WebSocket 未连接；为避免盲采，开始按钮已锁定", "pending");
    } else if (app.recordState === "UNKNOWN") {
      setCommandStatus("等待 Recorder 状态", "pending");
    } else {
      setCommandStatus("数据源已就绪，可以开始采集", "success");
    }
  }

  function setCommandStatus(message, type = "") {
    elements.commandStatus.textContent = message;
    elements.commandStatus.classList.remove("is-pending", "is-error", "is-success");
    if (type) elements.commandStatus.classList.add(`is-${type}`);
  }

  function setCommandFeedback(message, type, durationMs = 6000) {
    app.commandFeedback = {
      message,
      type,
      expiresAt: Date.now() + durationMs,
    };
    renderControls();
  }

  function beginPendingCommand(action, expectedState, requestedPath = "") {
    settlePendingCommand();
    const pending = {
      action,
      expectedState,
      requestedPath,
      timeout: 0,
    };
    pending.timeout = window.setTimeout(() => {
      if (app.pendingCommand !== pending) return;
      app.pendingCommand = null;
      setCommandFeedback(
        "命令已发送，但 Recorder 状态在 8 秒内没有变化。请检查 policy_command / record_status 连线和 recorder 日志。",
        "error",
        12000,
      );
      notify("Recorder 未确认命令", "请检查 dataflow 控制与状态连线", "error", 9000);
      renderControls();
    }, 8000);
    app.pendingCommand = pending;
    renderControls();
    return pending;
  }

  function settlePendingCommand() {
    if (!app.pendingCommand) return;
    window.clearTimeout(app.pendingCommand.timeout);
    app.pendingCommand = null;
  }

  async function postJson(path, body) {
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), 6000);
    try {
      const response = await fetch(path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
      const text = await response.text();
      let payload = {};
      if (text) {
        try {
          payload = JSON.parse(text);
        } catch (_error) {
          throw new Error(`Gateway 返回了无法解析的响应（HTTP ${response.status}）`);
        }
      }
      if (!response.ok || payload.ok === false) {
        throw new Error(payload.msg || `Gateway 请求失败（HTTP ${response.status}）`);
      }
      return payload;
    } catch (error) {
      if (error.name === "AbortError") {
        throw new Error("Gateway 请求超时");
      }
      throw error;
    } finally {
      window.clearTimeout(timeout);
    }
  }

  function validateCollectionForm() {
    clearFormError();
    const taskName = elements.taskName.value.trim();
    const episodeId = elements.episodeId.value.trim();
    const outputPath = elements.outputPath.value.trim();

    if (!taskName) return showFormError("请填写任务名", elements.taskName);
    if (!episodeId) return showFormError("请填写 Episode ID", elements.episodeId);
    if (!outputPath) return showFormError("请填写 MCAP 输出路径", elements.outputPath);
    if (/[\0\r\n]/.test(outputPath)) {
      return showFormError("输出路径不能包含换行或空字符", elements.outputPath);
    }
    if (!outputPath.toLowerCase().endsWith(".mcap")) {
      return showFormError("输出路径必须以 .mcap 结尾", elements.outputPath);
    }

    return {
      taskName,
      episodeId,
      outputPath,
      operatorName: elements.operatorName.value.trim(),
      notes: elements.notes.value.trim(),
    };
  }

  function showFormError(message, field) {
    elements.formError.hidden = false;
    elements.formError.textContent = message;
    if (field) {
      field.setAttribute("aria-invalid", "true");
      field.focus();
    }
    return null;
  }

  function clearFormError() {
    elements.formError.hidden = true;
    elements.formError.textContent = "";
    [elements.taskName, elements.episodeId, elements.outputPath].forEach((field) => {
      field.removeAttribute("aria-invalid");
    });
  }

  async function startRecording(event) {
    event.preventDefault();
    if (elements.startButton.disabled) return;
    const values = validateCollectionForm();
    if (!values) return;

    const startedAt = new Date().toISOString();
    const metadata = {
      workflow: {
        episode_id: values.episodeId,
        task_name: values.taskName,
      },
      collection: {
        source: "gateway-collector-ui",
        started_at: startedAt,
      },
    };
    if (values.operatorName) metadata.collection.operator = values.operatorName;
    if (values.notes) metadata.collection.notes = values.notes;

    app.activePath = values.outputPath;
    const pending = beginPendingCommand("START", "RECORDING", values.outputPath);
    try {
      await postJson("/record/control", {
        action: "START",
        output_path: values.outputPath,
        metadata,
      });
      if (app.pendingCommand === pending) {
        setCommandStatus("开始命令已进入 Dora 队列，等待 Recorder 确认…", "pending");
      }
    } catch (error) {
      if (app.pendingCommand === pending) settlePendingCommand();
      app.activePath = "";
      const message = String(error.message || error);
      setCommandFeedback(`开始录制失败：${message}`, "error", 9000);
      notify("开始录制失败", message, "error", 9000);
      renderControls();
    }
  }

  async function stopRecording() {
    if (elements.stopButton.disabled) return;
    const pending = beginPendingCommand("STOP", "IDLE");
    try {
      await postJson("/record/control", { action: "STOP" });
      if (app.pendingCommand === pending) {
        setCommandStatus("停止命令已发送，Recorder 正在完成最后一帧和索引…", "pending");
      }
    } catch (error) {
      if (app.pendingCommand === pending) settlePendingCommand();
      const message = String(error.message || error);
      setCommandFeedback(`停止录制失败：${message}`, "error", 9000);
      notify("停止命令失败", message, "error", 9000);
      renderControls();
    }
  }

  async function discardRecording() {
    if (elements.discardButton.disabled) return;
    const confirmed = await requestConfirmation(
      "丢弃当前采集？",
      "Recorder 将结束当前 session 并删除对应 MCAP，此操作无法撤销。",
      "确认丢弃",
    );
    if (!confirmed) return;

    const pending = beginPendingCommand("DISCARD", "IDLE");
    try {
      await postJson("/record/control", { action: "DISCARD" });
      if (app.pendingCommand === pending) {
        setCommandStatus("丢弃命令已发送，等待 Recorder 确认删除…", "pending");
      }
    } catch (error) {
      if (app.pendingCommand === pending) settlePendingCommand();
      const message = String(error.message || error);
      setCommandFeedback(`丢弃失败：${message}`, "error", 9000);
      notify("丢弃命令失败", message, "error", 9000);
      renderControls();
    }
  }

  async function resetScene() {
    if (elements.resetButton.disabled) return;
    const confirmed = await requestConfirmation(
      "复位当前场景？",
      "Gateway 将发送 reset_scene。请确认机械臂周围安全，且下游 task_robot 已连接该命令。",
      "发送复位",
    );
    if (!confirmed) return;

    app.resetBusy = true;
    renderControls();
    try {
      await postJson("/runtime/reset_scene", {
        inputs: { reason: "collector-ui" },
      });
      setCommandFeedback("场景复位命令已发送", "success", 5000);
      notify("复位命令已发送", "请观察相机与机器人状态确认复位完成", "success");
    } catch (error) {
      const message = String(error.message || error);
      setCommandFeedback(`场景复位失败：${message}`, "error", 9000);
      notify("场景复位失败", message, "error", 9000);
    } finally {
      app.resetBusy = false;
      renderControls();
    }
  }

  function requestConfirmation(title, message, confirmLabel) {
    if (typeof elements.confirmDialog.showModal !== "function") {
      return Promise.resolve(window.confirm(`${title}\n\n${message}`));
    }

    elements.confirmTitle.textContent = title;
    elements.confirmMessage.textContent = message;
    elements.confirmActionButton.textContent = confirmLabel;
    elements.confirmDialog.returnValue = "";
    elements.confirmDialog.showModal();

    return new Promise((resolve) => {
      elements.confirmDialog.addEventListener(
        "close",
        () => resolve(elements.confirmDialog.returnValue === "confirm"),
        { once: true },
      );
    });
  }

  function notify(title, message, type = "info", durationMs = 6000) {
    const toast = document.createElement("div");
    toast.className = `toast is-${type}`;
    toast.setAttribute("role", type === "error" ? "alert" : "status");
    const content = document.createElement("div");
    const heading = document.createElement("strong");
    heading.textContent = title;
    const detail = document.createElement("span");
    detail.textContent = message || "";
    content.append(heading, detail);
    toast.append(content);
    elements.toastRegion.append(toast);
    window.setTimeout(() => toast.remove(), durationMs);
  }

  function bindActions() {
    elements.collectionForm.addEventListener("submit", startRecording);
    elements.stopButton.addEventListener("click", stopRecording);
    elements.discardButton.addEventListener("click", discardRecording);
    elements.resetButton.addEventListener("click", resetScene);

    window.addEventListener("online", () => {
      reconnectNow("state");
      reconnectNow("images");
    });
    window.addEventListener("offline", refreshConnectionPills);
    document.addEventListener("visibilitychange", () => {
      if (!document.hidden) {
        reconnectNow("state");
        reconnectNow("images");
      }
    });
    window.addEventListener("beforeunload", () => {
      app.closing = true;
      Object.values(app.sockets).forEach((channel) => {
        window.clearTimeout(channel.retryTimer);
        if (channel.socket) channel.socket.close();
      });
      app.cameras.forEach((camera) => {
        if (camera.objectUrl) URL.revokeObjectURL(camera.objectUrl);
      });
    });
  }

  function updateTransientUi() {
    refreshConnectionPills();
    updateDuration();
    app.cameras.forEach(updateCameraAge);
    if (app.commandFeedback && app.commandFeedback.expiresAt <= Date.now()) {
      app.commandFeedback = null;
      renderControls();
    }
  }

  initializeForm();
  bindActions();
  refreshConnectionPills();
  renderRecordOverview();
  connectSocket("state");
  connectSocket("images");
  window.setInterval(updateTransientUi, 750);
})();
