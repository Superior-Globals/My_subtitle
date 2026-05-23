# IPTV Subtitle Server

## Deploy in 2 clicks

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/new/template?template=https://github.com/YOUR_USERNAME/YOUR_REPO)

> **Replace** `YOUR_USERNAME/YOUR_REPO` in the URL above with your actual GitHub repo after you push these files.

---

## After deploy — 3 things to do

### 1. Set env vars in Railway dashboard
Go to your service → **Variables** → add these:

| Key | Value |
|-----|-------|
| `OPENAI_API_KEY` | `sk-...` |
| `GEMINI_API_KEY` | `AIza...` *(optional)* |
| `ALLOWED_ORIGINS` | `https://your-app.lovable.app` |

### 2. Get your domain
Railway dashboard → your service → **Settings → Networking → Generate Domain**

Your URL will look like:
```
subtitle-server-production-xxxx.up.railway.app
```

### 3. Paste into Lovable
In Lovable → Project Settings → Environment Variables:
```
VITE_SUBTITLE_WS_URL=wss://subtitle-server-production-xxxx.up.railway.app/subtitles/ws
```

---

That's your subtitle API URL. Done.
