# Telegram Auto Join Request Bot

Auto-approve (human-like delay), purani requests (helper account), Failed Report, Public/Private mode,
Free/Premium plans, Hindi/English, Captcha, Force Subscribe, Log channel + alerts, User ban.

## Render (Web Service)
- Build: `pip install -r requirements.txt`   Start: `python main.py`
- Env vars: .env.example ke hisaab se (+ `PYTHON_VERSION=3.11.9` recommended)
- MongoDB Atlas: Network Access me 0.0.0.0/0 allow karo

## Admin commands (OWNER_IDS)
/settings  panel | /public /private  mode | /users  users list (10/page)
/premium <id> <days>   Premium do (days jodte jate hain) | /free <id>  Premium hatao | /premiums  list
/ban <id>  /unban <id>  /banned  list
Panel: Log Channel (set/test/remove), Old-Req Session (helper login), Users -> Ban/Premium buttons

## Plans
Free: FREE_CHATS chats, auto approve, delay, welcome DM, failed report
Premium: PREMIUM_CHATS chats + Captcha (human button / math) + Force Subscribe + old requests approve
Premium khatam hote hi captcha/force-sub apne-aap band ho jate hain, user ko DM jata hai.

## Captcha / Force Subscribe (Premium)
Chat -> Captcha button cycles Off -> Human -> Math. Force Subscribe: max 3 channels (bot unme admin ho).
User ko request ke baad bot DM karta hai; GATE_MINUTES me verify/join na kare to request decline.
Failed ones Failed Report me dikhte hain.

## Log channel / alerts
Naye users, naye chats, premium/ban changes, errors, bot hataye jaane par, helper session mar jaane par.
Log channel set nahi to important alerts owners ko DM aate hain.

## Helper account (old requests)
Settings -> Old-Req Session -> Login (phone + OTP). Secondary account use karo, chat me admin banao.
