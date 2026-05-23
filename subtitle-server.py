#!/usr/bin/env python3
"""
subtitle-server.py
──────────────────
Local/server-side subtitle engine for IPTV streams.

What it does:
  1. Receives a stream URL + target language from the Lovable frontend
  2. ffmpeg taps the audio (bypasses all browser CORS issues)
  3. Cuts audio into 7s chunks with 1s overlap
  4. Sends each chunk to OpenAI Whisper (auto-detects source language)
  5. Translates via Google Gemini (or OpenAI if you prefer)
  6. Pushes results to all connected WebSocket clients in real time

Requirements:
  pip install fastapi uvicorn websockets openai httpx python-dotenv
  apt install ffmpeg   (or brew install ffmpeg)

Usage:
  python subtitle-server.py
  # then point your Lovable app at ws://localhost:8765
"""

import asyncio
import io
import json
import logging
import os
import subprocess
import tempfile
import time
import wave
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

import httpx
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from openai import AsyncOpenAI

load_dotenv()

# ── config ────────────────────────────────────────────────────────────────────

OPENAI_API_KEY        = os.environ["OPENAI_API_KEY"]
GEMINI_API_KEY        = os.getenv("GEMINI_API_KEY", "")          # optional — falls back to OpenAI translate
ALLOWED_ORIGINS       = os.getenv("ALLOWED_ORIGINS", "*").split(",")  # e.g. "https://myapp.lovable.app"

CHUNK_DURATION_S      = 7       # seconds of audio per Whisper call
OVERLAP_S             = 1       # seconds of overlap between chunks
SAMPLE_RATE           = 16000   # Hz — Whisper's native rate
CHANNELS              = 1       # mono
BYTES_PER_SAMPLE      = 2       # 16-bit PCM

CHUNK_BYTES           = SAMPLE_RATE * CHANNELS * BYTES_PER_SAMPLE * CHUNK_DURATION_S
OVERLAP_BYTES         = SAMPLE_RATE * CHANNELS * BYTES_PER_SAMPLE * OVERLAP_S

LANG_NAMES = {
    "en": "English", "es": "Spanish", "fr": "French", "de": "German",
    "ar": "Arabic",  "hi": "Hindi",   "pt": "Portuguese", "zh": "Mandarin Chinese",
    "tr": "Turkish", "ru": "Russian",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("subtitles")

openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

# ── data structures ───────────────────────────────────────────────────────────

@dataclass
class SubtitleEvent:
    chunk_idx:     int
    original:      str
    translated:    str
    source_lang:   str
    target_lang:   str
    timestamp_ms:  int = field(default_factory=lambda: int(time.time() * 1000))

@dataclass
class StreamSession:
    stream_url:    str
    target_lang:   str
    channel_id:    str
    active:        bool = True
    ffmpeg_proc:   Optional[subprocess.Popen] = None

# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(title="IPTV Subtitle Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type"],
)

# connected WebSocket clients: channel_id → set of websockets
ws_clients: dict[str, set[WebSocket]] = {}

# active stream sessions: channel_id → StreamSession
sessions: dict[str, StreamSession] = {}


# ── WebSocket endpoint ────────────────────────────────────────────────────────

@app.websocket("/subtitles/ws")
async def subtitle_ws(websocket: WebSocket):
    """
    WebSocket endpoint.

    Client sends one JSON message to start:
        { "action": "start", "channelId": "...", "streamUrl": "...", "targetLang": "en" }

    Client sends to stop (or just disconnect):
        { "action": "stop", "channelId": "..." }

    Server pushes:
        { "type": "subtitle", "chunkIdx": N, "original": "...", "translated": "...",
          "sourceLang": "xx", "targetLang": "en", "timestampMs": 1234567890 }
        { "type": "error",   "code": "rate_limit"|"no_credits"|"ffmpeg_fail", "message": "..." }
        { "type": "status",  "status": "started"|"stopped"|"switching" }
    """
    await websocket.accept()
    channel_id = None

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await _send(websocket, {"type": "error", "code": "bad_json", "message": "Invalid JSON"})
                continue

            action = msg.get("action")

            if action == "start":
                channel_id = msg.get("channelId", "default")
                stream_url = msg.get("streamUrl", "")
                target_lang = msg.get("targetLang", "en")

                if not stream_url:
                    await _send(websocket, {"type": "error", "code": "no_url", "message": "streamUrl required"})
                    continue

                # register this client
                ws_clients.setdefault(channel_id, set()).add(websocket)

                # stop any existing session for this channel
                await _stop_session(channel_id)

                session = StreamSession(
                    stream_url=stream_url,
                    target_lang=target_lang,
                    channel_id=channel_id,
                )
                sessions[channel_id] = session

                await _send(websocket, {"type": "status", "status": "started", "channelId": channel_id})
                log.info(f"[{channel_id}] Starting — {stream_url[:60]}... → {target_lang}")

                # fire and forget — runs until session.active = False
                asyncio.create_task(_run_session(session))

            elif action == "stop":
                cid = msg.get("channelId", channel_id)
                if cid:
                    await _stop_session(cid)
                    await _send(websocket, {"type": "status", "status": "stopped", "channelId": cid})

            elif action == "ping":
                await _send(websocket, {"type": "pong"})

    except WebSocketDisconnect:
        pass
    finally:
        if channel_id and channel_id in ws_clients:
            ws_clients[channel_id].discard(websocket)


# ── REST: start/stop (alternative to WebSocket control messages) ──────────────

@app.post("/session/start")
async def start_session(body: dict):
    channel_id  = body.get("channelId", "default")
    stream_url  = body.get("streamUrl", "")
    target_lang = body.get("targetLang", "en")

    if not stream_url:
        raise HTTPException(400, "streamUrl required")

    await _stop_session(channel_id)
    session = StreamSession(stream_url=stream_url, target_lang=target_lang, channel_id=channel_id)
    sessions[channel_id] = session
    asyncio.create_task(_run_session(session))
    return {"status": "started", "channelId": channel_id}


@app.post("/session/stop")
async def stop_session(body: dict):
    channel_id = body.get("channelId", "default")
    await _stop_session(channel_id)
    return {"status": "stopped", "channelId": channel_id}


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "active_sessions": list(sessions.keys()),
        "connected_clients": {k: len(v) for k, v in ws_clients.items()},
    }


# ── core pipeline ─────────────────────────────────────────────────────────────

async def _run_session(session: StreamSession):
    """
    Main loop for one stream session:
      ffmpeg → raw PCM → chunk → Whisper → translate → broadcast
    """
    loop = asyncio.get_event_loop()

    # ffmpeg command: read stream, output raw 16kHz mono PCM to stdout
    # -err_detect ignore_err: tolerates brief stream hiccups
    # -reconnect 1: auto-reconnects HLS/HTTP streams
    ffmpeg_cmd = [
        "ffmpeg",
        "-loglevel", "error",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-i", session.stream_url,
        "-vn",                          # no video
        "-acodec", "pcm_s16le",         # raw 16-bit PCM
        "-ar", str(SAMPLE_RATE),
        "-ac", str(CHANNELS),
        "-f", "s16le",
        "pipe:1",                       # stdout
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *ffmpeg_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError:
        await _broadcast(session.channel_id, {
            "type": "error", "code": "ffmpeg_missing",
            "message": "ffmpeg not found — install with: apt install ffmpeg"
        })
        return

    session.ffmpeg_proc = proc
    log.info(f"[{session.channel_id}] ffmpeg PID {proc.pid}")

    buffer      = bytearray()
    overlap_buf = bytearray()  # carries last OVERLAP_BYTES into next chunk
    chunk_idx   = 0
    pending     = deque()      # asyncio.Tasks ordered by chunk_idx

    try:
        while session.active:
            # read a block of PCM data
            try:
                block = await asyncio.wait_for(proc.stdout.read(4096), timeout=10.0)
            except asyncio.TimeoutError:
                log.warning(f"[{session.channel_id}] ffmpeg read timeout")
                continue

            if not block:
                # stream ended
                break

            buffer.extend(block)

            while len(buffer) + len(overlap_buf) >= CHUNK_BYTES:
                # assemble chunk = overlap from last window + fresh data
                chunk_data = bytes(overlap_buf) + bytes(buffer[: CHUNK_BYTES - len(overlap_buf)])
                buffer     = buffer[CHUNK_BYTES - len(overlap_buf):]
                overlap_buf = bytearray(chunk_data[-OVERLAP_BYTES:])

                idx = chunk_idx
                chunk_idx += 1

                # process in background — don't block the reader
                task = asyncio.create_task(
                    _process_chunk(session, chunk_data, idx)
                )
                pending.append((idx, task))

                # drain completed tasks in order (so subtitles appear sequentially)
                while pending and pending[0][1].done():
                    _, t = pending.popleft()
                    exc = t.exception()
                    if exc:
                        log.error(f"[{session.channel_id}] chunk error: {exc}")

    except Exception as e:
        log.error(f"[{session.channel_id}] session error: {e}")
        await _broadcast(session.channel_id, {
            "type": "error", "code": "stream_error", "message": str(e)
        })
    finally:
        session.active = False
        try:
            proc.kill()
            await proc.wait()
        except Exception:
            pass
        log.info(f"[{session.channel_id}] session ended")


async def _process_chunk(session: StreamSession, pcm_data: bytes, chunk_idx: int):
    """Convert one PCM chunk → Whisper → translate → broadcast."""
    if not session.active:
        return

    # wrap raw PCM in a WAV container so Whisper accepts it
    wav_buf = io.BytesIO()
    with wave.open(wav_buf, "wb") as wf:
        wf.setnchannels(CHANNELS)
        wf.setsampwidth(BYTES_PER_SAMPLE)
        wf.setframerate(SAMPLE_RATE)
        wf.writeframes(pcm_data)
    wav_buf.seek(0)

    # ── Whisper STT ───────────────────────────────────────────────────────────
    try:
        response = await openai_client.audio.transcriptions.create(
            model="whisper-1",
            file=("chunk.wav", wav_buf, "audio/wav"),
            response_format="verbose_json",   # includes detected language
        )
    except Exception as e:
        err_str = str(e)
        if "429" in err_str:
            await _broadcast(session.channel_id, {
                "type": "error", "code": "rate_limit", "message": "Whisper rate limit — slowing down"
            })
        elif "402" in err_str or "insufficient_quota" in err_str:
            await _broadcast(session.channel_id, {
                "type": "error", "code": "no_credits", "message": "OpenAI quota exceeded"
            })
            session.active = False
        log.warning(f"[{session.channel_id}] Whisper error: {e}")
        return

    original_text = (response.text or "").strip()
    source_lang   = getattr(response, "language", "?") or "?"

    if not original_text:
        return  # silent chunk

    # ── Translation ───────────────────────────────────────────────────────────
    translated_text = original_text  # default: pass-through

    target_lang = session.target_lang
    needs_translation = not (
        source_lang == target_lang or
        source_lang.startswith(target_lang) or
        (target_lang == "en" and source_lang in ("english", "en"))
    )

    if needs_translation:
        translated_text = await _translate(original_text, target_lang, session.channel_id)

    # ── Broadcast ─────────────────────────────────────────────────────────────
    event = SubtitleEvent(
        chunk_idx=chunk_idx,
        original=original_text,
        translated=translated_text,
        source_lang=source_lang,
        target_lang=target_lang,
    )
    await _broadcast(session.channel_id, {
        "type":        "subtitle",
        "chunkIdx":    event.chunk_idx,
        "original":    event.original,
        "translated":  event.translated,
        "sourceLang":  event.source_lang,
        "targetLang":  event.target_lang,
        "timestampMs": event.timestamp_ms,
    })
    log.info(f"[{session.channel_id}] #{chunk_idx} [{source_lang}→{target_lang}] {original_text[:60]}")


async def _translate(text: str, target_lang: str, channel_id: str) -> str:
    """Translate text using Gemini if available, otherwise GPT-4o-mini."""
    lang_name = LANG_NAMES.get(target_lang, target_lang)
    prompt    = (
        f"Translate the following transcript to {lang_name}. "
        f"Return ONLY the translation, no explanation, no quotes.\n\n{text}"
    )

    # ── try Gemini first (cheaper) ────────────────────────────────────────────
    if GEMINI_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                r = await client.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/"
                    f"gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}",
                    json={"contents": [{"parts": [{"text": prompt}]}]},
                )
            if r.status_code == 429:
                await _broadcast(channel_id, {
                    "type": "error", "code": "rate_limit", "message": "Gemini rate limit"
                })
            elif r.status_code == 200:
                data = r.json()
                return data["candidates"][0]["content"]["parts"][0]["text"].strip()
        except Exception as e:
            log.warning(f"Gemini error: {e} — falling back to OpenAI")

    # ── fallback: GPT-4o-mini ─────────────────────────────────────────────────
    try:
        resp = await openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300,
            temperature=0.2,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        log.warning(f"OpenAI translate error: {e}")
        return text  # return original on failure


# ── helpers ───────────────────────────────────────────────────────────────────

async def _stop_session(channel_id: str):
    session = sessions.pop(channel_id, None)
    if session:
        session.active = False
        if session.ffmpeg_proc:
            try:
                session.ffmpeg_proc.kill()
            except Exception:
                pass
        await _broadcast(channel_id, {"type": "status", "status": "stopped", "channelId": channel_id})
        log.info(f"[{channel_id}] stopped")


async def _broadcast(channel_id: str, payload: dict):
    clients = ws_clients.get(channel_id, set()).copy()
    if not clients:
        return
    msg = json.dumps(payload)
    dead = set()
    for ws in clients:
        try:
            await ws.send_text(msg)
        except Exception:
            dead.add(ws)
    ws_clients[channel_id] -= dead


async def _send(ws: WebSocket, payload: dict):
    try:
        await ws.send_text(json.dumps(payload))
    except Exception:
        pass


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8765))
    log.info(f"Starting subtitle server on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
