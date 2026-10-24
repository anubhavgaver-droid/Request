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
