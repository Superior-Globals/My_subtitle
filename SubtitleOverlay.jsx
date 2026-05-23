// SubtitleOverlay.jsx
// ─────────────────────────────────────────────────────────────────────────────
// Drop this into your Lovable IPTV app.
// Connects to your local subtitle-server.py via WebSocket.
// Renders Netflix-style subtitle overlay + settings panel.
//
// USAGE:
//   <div style={{ position: "relative" }}>
//     <video ref={videoRef} ... />          ← your existing player
//     <SubtitleOverlay
//       channelId={activeChannel.id}
//       streamUrl={activeChannel.url}
//     />
//   </div>
// ─────────────────────────────────────────────────────────────────────────────

import { useState, useEffect, useRef, useCallback } from "react";

// ── constants ─────────────────────────────────────────────────────────────────

const WS_URL = import.meta.env.VITE_SUBTITLE_WS_URL ?? "ws://localhost:8765/subtitles/ws";

const LANGUAGES = [
  { code: "en", label: "English"    },
  { code: "es", label: "Spanish"    },
  { code: "fr", label: "French"     },
  { code: "de", label: "German"     },
  { code: "ar", label: "Arabic"     },
  { code: "hi", label: "Hindi"      },
  { code: "pt", label: "Portuguese" },
  { code: "zh", label: "Mandarin"   },
  { code: "tr", label: "Turkish"    },
  { code: "ru", label: "Russian"    },
];

const FONT_SIZES  = { S: 14, M: 18, L: 23, XL: 30 };
const SUB_TTL_MS  = 5500;   // how long each line stays visible
const STORAGE_KEY = "iptv_sub_prefs";

function loadPrefs() {
  try { return { ...defaults(), ...JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "{}") }; }
  catch { return defaults(); }
}
function defaults() {
  return { enabled: true, targetLang: "en", showOriginal: false, syncOffset: 0, fontSize: "M" };
}
function savePrefs(p) {
  try { localStorage.setItem(STORAGE_KEY, JSON.stringify(p)); } catch (_) {}
}

// ── main component ────────────────────────────────────────────────────────────

export default function SubtitleOverlay({ channelId, streamUrl }) {
  const [prefs, setPrefs]         = useState(loadPrefs);
  const [lines, setLines]         = useState([]);   // [{ id, text, entering }]
  const [panelOpen, setPanelOpen] = useState(false);
  const [wsState, setWsState]     = useState("disconnected"); // connected|connecting|disconnected|error
  const [toasts, setToasts]       = useState([]);

  const wsRef          = useRef(null);
  const reconnectTimer = useRef(null);
  const panelHideTimer = useRef(null);
  const lineTimers     = useRef({});
  const pendingQueue   = useRef([]);   // { chunkIdx, text } — reordered before display
  const nextExpected   = useRef(0);
  const panelRef       = useRef(null);
  const gearRef        = useRef(null);

  // ── prefs helpers ─────────────────────────────────────────────────────────

  const update = useCallback((patch) => {
    setPrefs(p => { const n = { ...p, ...patch }; savePrefs(n); return n; });
  }, []);

  // ── toast helper ─────────────────────────────────────────────────────────

  const toast = useCallback((msg, kind = "error") => {
    const id = Date.now() + Math.random();
    setToasts(t => [...t, { id, msg, kind }]);
    setTimeout(() => setToasts(t => t.filter(x => x.id !== id)), 4500);
  }, []);

  // ── WebSocket lifecycle ───────────────────────────────────────────────────

  const connect = useCallback(() => {
    if (!prefs.enabled || !channelId || !streamUrl) return;
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    setWsState("connecting");
    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => {
      setWsState("connected");
      ws.send(JSON.stringify({
        action:     "start",
        channelId,
        streamUrl,
        targetLang: prefs.targetLang,
      }));
    };

    ws.onmessage = (e) => {
      let msg;
      try { msg = JSON.parse(e.data); } catch { return; }

      if (msg.type === "subtitle") {
        const text = prefs.showOriginal ? msg.original : msg.translated;
        if (!text?.trim()) return;
        enqueue({ chunkIdx: msg.chunkIdx, text });
      }

      if (msg.type === "error") {
        if (msg.code === "rate_limit")  toast("Rate limit hit — subtitles may lag", "warn");
        if (msg.code === "no_credits")  toast("API quota exceeded — subtitles stopped", "error");
        if (msg.code === "ffmpeg_missing") toast("ffmpeg not found on server", "error");
        if (msg.code === "stream_error")   toast(`Stream error: ${msg.message}`, "error");
      }
    };

    ws.onerror = () => setWsState("error");

    ws.onclose = () => {
      setWsState("disconnected");
      // auto-reconnect after 3s if still supposed to be active
      clearTimeout(reconnectTimer.current);
      reconnectTimer.current = setTimeout(() => {
        if (prefs.enabled) connect();
      }, 3000);
    };
  }, [channelId, streamUrl, prefs.enabled, prefs.targetLang, prefs.showOriginal, toast]);

  const disconnect = useCallback(() => {
    clearTimeout(reconnectTimer.current);
    const ws = wsRef.current;
    if (ws) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ action: "stop", channelId }));
      }
      ws.close();
      wsRef.current = null;
    }
    setWsState("disconnected");
    setLines([]);
    pendingQueue.current = [];
    nextExpected.current = 0;
  }, [channelId]);

  // reconnect when channel or language changes
  useEffect(() => {
    if (!prefs.enabled) { disconnect(); return; }
    disconnect();
    const t = setTimeout(connect, 200); // brief pause lets old session stop
    return () => clearTimeout(t);
  }, [channelId, streamUrl, prefs.enabled, prefs.targetLang]); // eslint-disable-line

  useEffect(() => () => disconnect(), []); // eslint-disable-line

  // ── subtitle queue (reorder by chunkIdx) ─────────────────────────────────

  const enqueue = useCallback(({ chunkIdx, text }) => {
    pendingQueue.current.push({ chunkIdx, text });
    pendingQueue.current.sort((a, b) => a.chunkIdx - b.chunkIdx);
    drainQueue();
  }, []); // eslint-disable-line

  const drainQueue = useCallback(() => {
    while (
      pendingQueue.current.length > 0 &&
      pendingQueue.current[0].chunkIdx <= nextExpected.current + 2 // allow max 2 gap
    ) {
      const { chunkIdx, text } = pendingQueue.current.shift();
      nextExpected.current = chunkIdx + 1;
      showLine(text);
    }
  }, []); // eslint-disable-line

  const showLine = useCallback((text) => {
    const id       = `sub_${Date.now()}_${Math.random()}`;
    const offsetMs = (prefs.syncOffset ?? 0) * 1000;
    const delay    = Math.max(0, offsetMs);
    const ttl      = SUB_TTL_MS + Math.max(0, offsetMs);

    setTimeout(() => {
      setLines(l => [...l.slice(-3), { id, text, entering: true }]);
      // remove entering class after animation
      setTimeout(() => {
        setLines(l => l.map(x => x.id === id ? { ...x, entering: false } : x));
      }, 250);
      // remove line after TTL
      lineTimers.current[id] = setTimeout(() => {
        setLines(l => l.filter(x => x.id !== id));
        delete lineTimers.current[id];
      }, ttl);
    }, delay);
  }, [prefs.syncOffset]);

  // cleanup line timers on unmount
  useEffect(() => () => Object.values(lineTimers.current).forEach(clearTimeout), []);

  // ── panel auto-hide ───────────────────────────────────────────────────────

  const resetPanelHide = useCallback(() => {
    clearTimeout(panelHideTimer.current);
    panelHideTimer.current = setTimeout(() => setPanelOpen(false), 4000);
  }, []);

  useEffect(() => {
    if (panelOpen) resetPanelHide();
    return () => clearTimeout(panelHideTimer.current);
  }, [panelOpen]); // eslint-disable-line

  // close on outside click
  useEffect(() => {
    const h = (e) => {
      if (panelRef.current?.contains(e.target) || gearRef.current?.contains(e.target)) return;
      setPanelOpen(false);
    };
    document.addEventListener("mousedown", h);
    return () => document.removeEventListener("mousedown", h);
  }, []);

  // ── render ────────────────────────────────────────────────────────────────

  const fs = FONT_SIZES[prefs.fontSize] ?? 18;

  return (
    <>
      <style>{CSS}</style>

      {/* ── Subtitle lines ─────────────────────────────────────────────── */}
      {prefs.enabled && (
        <div className="sub-container" aria-live="polite" aria-label="Live subtitles">
          {lines.map(line => (
            <div
              key={line.id}
              className={`sub-line ${line.entering ? "sub-entering" : ""}`}
              style={{ fontSize: fs }}
            >
              {line.text}
            </div>
          ))}
        </div>
      )}

      {/* ── Gear / status button ───────────────────────────────────────── */}
      <button
        ref={gearRef}
        className="sub-gear"
        onClick={() => { setPanelOpen(p => !p); resetPanelHide(); }}
        title="Subtitle settings"
        aria-label="Subtitle settings"
        data-state={wsState}
        data-enabled={prefs.enabled}
      >
        <GearSVG />
        <span className="sub-gear-badge">
          {prefs.enabled
            ? wsState === "connected"    ? "CC●"
            : wsState === "connecting"   ? "CC…"
            : wsState === "error"        ? "CC✕"
            :                             "CC○"
            : "CC"
          }
        </span>
      </button>

      {/* ── Settings panel ─────────────────────────────────────────────── */}
      {panelOpen && (
        <div
          ref={panelRef}
          className="sub-panel"
          onMouseMove={resetPanelHide}
          role="dialog"
          aria-label="Subtitle settings"
        >
          {/* Header + master toggle */}
          <div className="sub-panel-header">
            <span className="sub-panel-title">SUBTITLES</span>
            <button
              className={`sub-master-toggle ${prefs.enabled ? "on" : "off"}`}
              onClick={() => update({ enabled: !prefs.enabled })}
              aria-pressed={prefs.enabled}
            >
              {prefs.enabled ? "ON" : "OFF"}
            </button>
          </div>

          {/* Language */}
          <PanelSection label="LANGUAGE">
            <div className="sub-lang-grid">
              {LANGUAGES.map(l => (
                <button
                  key={l.code}
                  className={`sub-lang-btn ${prefs.targetLang === l.code ? "active" : ""}`}
                  onClick={() => update({ targetLang: l.code })}
                >
                  {l.label}
                </button>
              ))}
            </div>
          </PanelSection>

          {/* Display mode */}
          <PanelSection label="DISPLAY">
            <div className="sub-row">
              {["Translated", "Original"].map(m => {
                const isOrig = m === "Original";
                return (
                  <button
                    key={m}
                    className={`sub-mode-btn ${prefs.showOriginal === isOrig ? "active" : ""}`}
                    onClick={() => update({ showOriginal: isOrig })}
                  >
                    {m}
                  </button>
                );
              })}
            </div>
          </PanelSection>

          {/* Font size */}
          <PanelSection label="FONT SIZE">
            <div className="sub-row">
              {Object.keys(FONT_SIZES).map(sz => (
                <button
                  key={sz}
                  className={`sub-mode-btn ${prefs.fontSize === sz ? "active" : ""}`}
                  style={{ fontSize: FONT_SIZES[sz] * 0.75 }}
                  onClick={() => update({ fontSize: sz })}
                >
                  {sz}
                </button>
              ))}
            </div>
          </PanelSection>

          {/* Sync offset */}
          <PanelSection label={`SYNC  ${prefs.syncOffset > 0 ? "+" : ""}${prefs.syncOffset}s`}>
            <input
              type="range" min={-10} max={10} step={0.5}
              value={prefs.syncOffset}
              onChange={e => update({ syncOffset: parseFloat(e.target.value) })}
              className="sub-slider"
              aria-label="Subtitle sync offset"
            />
            <div className="sub-slider-labels">
              <span>−10s</span><span>0</span><span>+10s</span>
            </div>
          </PanelSection>

          {/* Server status */}
          <div className="sub-server-status" data-state={wsState}>
            <span className="sub-status-dot" />
            <span className="sub-status-label">
              { wsState === "connected"  ? "Server connected"
              : wsState === "connecting" ? "Connecting…"
              : wsState === "error"      ? "Connection error"
              :                           "Disconnected" }
            </span>
            <span className="sub-status-url">{WS_URL}</span>
          </div>
        </div>
      )}

      {/* ── Toasts ─────────────────────────────────────────────────────── */}
      <div className="sub-toasts" aria-live="assertive">
        {toasts.map(t => (
          <div key={t.id} className={`sub-toast sub-toast-${t.kind}`}>
            {t.msg}
          </div>
        ))}
      </div>
    </>
  );
}

// ── sub-components ────────────────────────────────────────────────────────────

function PanelSection({ label, children }) {
  return (
    <div className="sub-section">
      <div className="sub-section-label">{label}</div>
      {children}
    </div>
  );
}

function GearSVG() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none"
      stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="3"/>
      <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06
        a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09
        A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83
        l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09
        A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83
        l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09
        a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83
        l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09
        a1.65 1.65 0 0 0-1.51 1z"/>
    </svg>
  );
}

// ── injected CSS ──────────────────────────────────────────────────────────────

const CSS = `
  /* ── subtitle lines ── */
  .sub-container {
    position: absolute;
    bottom: 60px;
    left: 50%;
    transform: translateX(-50%);
    width: 90%;
    max-width: 920px;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 5px;
    pointer-events: none;
    z-index: 50;
  }
  .sub-line {
    background: rgba(8, 8, 12, 0.78);
    color: #ffffff;
    padding: 5px 16px 6px;
    border-radius: 4px;
    font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
    font-weight: 600;
    line-height: 1.45;
    letter-spacing: 0.01em;
    text-shadow: 0 1px 4px rgba(0,0,0,0.9);
    box-shadow: 0 2px 12px rgba(0,0,0,0.5);
    max-width: 100%;
    text-align: center;
    transition: opacity 0.25s ease, transform 0.25s ease;
  }
  .sub-entering {
    animation: subIn 0.22s ease forwards;
  }
  @keyframes subIn {
    from { opacity: 0; transform: translateY(8px); }
    to   { opacity: 1; transform: translateY(0); }
  }

  /* ── gear button ── */
  .sub-gear {
    position: absolute;
    top: 12px;
    right: 14px;
    z-index: 60;
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 6px 11px;
    background: rgba(255,255,255,0.1);
    border: 1px solid rgba(255,255,255,0.18);
    border-radius: 8px;
    color: #e5e7eb;
    cursor: pointer;
    backdrop-filter: blur(10px);
    transition: background 0.15s, opacity 0.15s;
    font-family: monospace;
  }
  .sub-gear:hover { background: rgba(255,255,255,0.18); }
  .sub-gear[data-enabled="false"] { color: #f87171; }
  .sub-gear-badge {
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.5px;
  }
  .sub-gear[data-state="connected"]   .sub-gear-badge { color: #4ade80; }
  .sub-gear[data-state="connecting"]  .sub-gear-badge { color: #facc15; }
  .sub-gear[data-state="error"]       .sub-gear-badge { color: #f87171; }
  .sub-gear[data-state="disconnected"].sub-gear-badge { color: #9ca3af; }

  /* ── settings panel ── */
  .sub-panel {
    position: absolute;
    top: 50px;
    right: 14px;
    z-index: 60;
    width: 268px;
    background: rgba(10, 10, 16, 0.94);
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 14px;
    padding: 15px 16px 13px;
    backdrop-filter: blur(24px);
    box-shadow: 0 12px 48px rgba(0,0,0,0.7);
    animation: panelIn 0.18s ease;
  }
  @keyframes panelIn {
    from { opacity: 0; transform: translateY(-10px) scale(0.97); }
    to   { opacity: 1; transform: translateY(0) scale(1); }
  }
  .sub-panel-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 13px;
    padding-bottom: 11px;
    border-bottom: 1px solid rgba(255,255,255,0.08);
  }
  .sub-panel-title {
    color: #d1d5db;
    font-family: monospace;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 2.5px;
  }
  .sub-master-toggle {
    padding: 4px 12px;
    border-radius: 20px;
    border: none;
    font-size: 11px;
    font-weight: 700;
    font-family: monospace;
    cursor: pointer;
    letter-spacing: 1px;
    transition: background 0.2s;
  }
  .sub-master-toggle.on  { background: #22c55e; color: #fff; }
  .sub-master-toggle.off { background: #ef4444; color: #fff; }

  /* ── panel sections ── */
  .sub-section { margin-bottom: 13px; }
  .sub-section-label {
    color: #4b5563;
    font-family: monospace;
    font-size: 9px;
    letter-spacing: 2px;
    margin-bottom: 7px;
  }

  /* ── language grid ── */
  .sub-lang-grid {
    display: flex;
    flex-wrap: wrap;
    gap: 5px;
  }
  .sub-lang-btn {
    background: rgba(255,255,255,0.05);
    border: 1px solid rgba(255,255,255,0.09);
    border-radius: 6px;
    color: #6b7280;
    font-size: 12px;
    padding: 3px 9px;
    cursor: pointer;
    font-family: 'Helvetica Neue', Arial, sans-serif;
    transition: all 0.13s;
  }
  .sub-lang-btn:hover { border-color: rgba(255,255,255,0.2); color: #d1d5db; }
  .sub-lang-btn.active {
    background: rgba(34,197,94,0.18);
    border-color: #22c55e;
    color: #86efac;
  }

  /* ── mode / size row ── */
  .sub-row { display: flex; gap: 6px; }
  .sub-mode-btn {
    flex: 1;
    background: rgba(255,255,255,0.05);
    border: 1px solid rgba(255,255,255,0.09);
    border-radius: 6px;
    color: #6b7280;
    font-size: 12px;
    padding: 5px 0;
    cursor: pointer;
    font-family: 'Helvetica Neue', Arial, sans-serif;
    transition: all 0.13s;
  }
  .sub-mode-btn:hover  { color: #d1d5db; border-color: rgba(255,255,255,0.2); }
  .sub-mode-btn.active {
    background: rgba(99,102,241,0.25);
    border-color: #818cf8;
    color: #c7d2fe;
  }

  /* ── sync slider ── */
  .sub-slider {
    width: 100%;
    cursor: pointer;
    accent-color: #818cf8;
    margin-bottom: 3px;
  }
  .sub-slider-labels {
    display: flex;
    justify-content: space-between;
    color: #374151;
    font-family: monospace;
    font-size: 9px;
  }

  /* ── server status ── */
  .sub-server-status {
    display: flex;
    align-items: center;
    gap: 6px;
    margin-top: 4px;
    padding-top: 10px;
    border-top: 1px solid rgba(255,255,255,0.06);
  }
  .sub-status-dot {
    width: 7px; height: 7px;
    border-radius: 50%;
    flex-shrink: 0;
    background: #374151;
  }
  .sub-server-status[data-state="connected"]   .sub-status-dot { background: #22c55e; }
  .sub-server-status[data-state="connecting"]  .sub-status-dot { background: #facc15; animation: blink 1s infinite; }
  .sub-server-status[data-state="error"]       .sub-status-dot { background: #ef4444; }
  @keyframes blink { 50% { opacity: 0.3; } }

  .sub-status-label {
    color: #9ca3af;
    font-size: 11px;
    font-family: monospace;
    flex-shrink: 0;
  }
  .sub-status-url {
    color: #374151;
    font-size: 9px;
    font-family: monospace;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  /* ── toasts ── */
  .sub-toasts {
    position: absolute;
    top: 52px;
    left: 50%;
    transform: translateX(-50%);
    z-index: 70;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 6px;
    pointer-events: none;
  }
  .sub-toast {
    padding: 8px 20px;
    border-radius: 8px;
    font-size: 13px;
    font-weight: 600;
    font-family: 'Helvetica Neue', Arial, sans-serif;
    box-shadow: 0 4px 20px rgba(0,0,0,0.5);
    animation: subIn 0.2s ease;
    color: white;
  }
  .sub-toast-error { background: rgba(220,38,38,0.92); }
  .sub-toast-warn  { background: rgba(217,119,6,0.92); }
`;
