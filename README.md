# Telegram Auto Join Request Bot

Channel + group join requests auto-approve (human-like delay), old requests approve, full button panel.

## Local run
pip install -r requirements.txt
export API_ID=... API_HASH=... BOT_TOKEN=... MONGO_URI=... OWNER_IDS=...
python main.py

## Render (Web Service)
- Build: `pip install -r requirements.txt`
- Start: `python main.py`
- Env vars: .env.example ke hisaab se (+ `PYTHON_VERSION=3.11.9`)
- PORT aur RENDER_EXTERNAL_URL Render khud deta hai (self-ping har 5 min)

## Use
1. Bot ko channel/group me admin banao (Invite Users permission).
2. Bot me /settings dabao -> sab kuch buttons se.
3. Old requests: bot me Settings -> "Old-Req Session" -> Login (phone+OTP). Session MongoDB me save hota hai (Render me kuch nahi badalna). Wo account chat me admin ho. Phir Chat -> "Approve Old Requests".
