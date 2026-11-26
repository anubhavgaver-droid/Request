# Telegram Auto Join Request Bot (+ Clone Bots, Hindi/English)

Auto-approve (human-like delay), purani requests (helper account), Failed Report, Public/Private mode,
Users list, aur **Clone Bot** feature. Har user /start ke baad apni bhasha (हिंदी / English) chun sakta hai.

## Render (Web Service)
- Build: `pip install -r requirements.txt`   Start: `python main.py`
- Env vars: .env.example ke hisaab se (+ `PYTHON_VERSION=3.11.9` recommended)
- MongoDB Atlas: Network Access me 0.0.0.0/0 allow karo
- **SECRET_KEY** zaroor set karo (kuch bhi lamba random text) aur kabhi mat badlo.

## Clone Bot kaise kaam karta hai
1. User main bot me **🤖 Clone Bot** dabata hai, @BotFather se naya bot banata hai aur uska token bhejta hai.
2. Bot token ko turant chat se delete kar deta hai aur encrypt karke save karta hai.
3. Clone usi process me chalta hai, par **uska data alag** hai (chats, settings, users, helper session).
4. Clone ka owner = wahi user. Clone me wahi commands/panel milte hain jo main bot me admin ke paas hain.
5. Old requests ke liye clone owner ko **apna khud ka helper account** clone ke andar login karna hota hai
   (Settings -> Old-Req Session) aur use apne chats me admin banana hota hai.
6. Ek account se **ek hi clone**. User jab chahe 🗑 se delete kar sakta hai (data bhi hat jata hai).
7. Main admin: `/settings` -> **🤖 Clones (Admin)**: list, stop/start, delete, aur clone feature ON/OFF.
- Clone feature tabhi users ko milta hai jab main bot **Public** ho (`/public`).

## Admin commands (main aur clone dono me, apne owner ke liye)
/settings panel | /public /private mode | /users users list (10/page) | /approveold <id> | /stopold <id> | /ping | /id

## Clone header
Clone me har user ko (panel aur /start par) dikhta hai: "Ye bot @MainBot ka clone hai, owner: ..." -
clone owner ise hata nahi sakta.

## Daily report
Har raat REPORT_HOUR (IST) par bot chat owners ko DM karta hai: aaj kitni requests approve/fail hui (top chats ke saath).
Admin ko poore bot ka summary milta hai. Sirf unko jaata hai jinke chats par aaj activity hui. Band karne ke liye
/settings -> 🔔 Daily report. Clone me ye clone ke apne users/chats ke liye alag chalta hai.

## ⚡ Speed (v2)
- **Delay 0 = Instant**: default ab `0-0` hai, approve bina ruke hota hai. Panel me **⚡ स्पीड / डिले** → `⚡ तुरंत (0s)`.
  (Purane deploy me DB me pehle wala delay saved hota hai, to ek baar ye button dabao.)
- **Naye requests**: `WORKERS` (default 30) parallel approvers + settings cache + DB writes background me → 5-10 requests/second aaram se.
- **Purani requests**: `OLD_BATCH` (default 12) requests **ek saath** approve hoti hain (10-12 parallel).
- **FloodWait safe**: Telegram rok de to us bot ke saare approvers ek saath ruk kar phir chalte hain, koi request chhootti nahi.
- Env (optional): `WORKERS=30`, `CLONE_WORKERS=8`, `OLD_BATCH=12`, `TG_WORKERS=32`.
- Note: Telegram ki apni limit hoti hai; zyada load par FloodWait aa sakta hai, bot khud wait karke aage badhta hai.

## 🤝 Helper account auto-admin
Bot ko chat me admin banao (**Invite Users + Add New Admins** permission ke saath). Bot khud:
1. helper account ko ek-baar-wale invite link se chat me join karata hai (link turant revoke),
2. use **Invite Users** permission ke saath admin bana deta hai.

Ye apne-aap hota hai: (a) bot ko admin banate hi, (b) jab 🕰 purani requests dabate ho aur helper admin nahi hai,
(c) chat panel ke **🤝 Helper को Admin बनाओ (Auto)** button se. Bot sirf helper account ko hi promote karta hai.
Agar "Add New Admins" permission nahi hai to bot saaf bata deta hai ki kya karna hai.
