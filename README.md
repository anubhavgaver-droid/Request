# Telegram Auto Join Request Bot

Channel + group join requests auto-approve (human-like delay), purani requests approve (helper account se),
failed requests ka reason-wise report, aur Public/Private mode.

## Render (Web Service)
- Build: `pip install -r requirements.txt`
- Start: `python main.py`
- Env vars: .env.example ke hisaab se (+ `PYTHON_VERSION=3.11.9` recommended)
- PORT aur RENDER_EXTERNAL_URL Render khud deta hai (self-ping har 5 min)
- MongoDB Atlas: Network Access me 0.0.0.0/0 allow karo

## Admin (OWNER_IDS)
- /settings -> poora panel (Active, Auto-all, Delay, Welcome, Old-Req Session, Public/Private)
- /public aur /private -> bot sabke liye khule ya sirf admin ke liye
- /users -> bot start karne wale sabhi users ki list (10 per page, Next/Back): naam, @username, ID, start/last time, chats, approved
- Old-Req Session -> helper account login (phone + OTP). Sirf admin ko dikhta hai.

## Users (Public mode)
1. Bot ko apne chat me admin banao (Invite Users permission) -> chat auto-add
2. Ya /start -> Add Chat
3. Chat ki settings: approve ON/OFF, delay, welcome DM, failed report
4. Purani requests: bot helper account ki details dikhata hai, use chat me admin banao,
   phir chat -> Approve Old Requests

## Failed Report
Chat -> Failed Report: reason-wise ginti (500 limit, deleted account, already member, banned, ...),
recent users, aur kisi ek user ki ID se reason check.
