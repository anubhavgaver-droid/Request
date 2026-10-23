import os, re, asyncio, random, logging, functools, time
from collections import Counter
from aiohttp import web, ClientSession
from motor.motor_asyncio import AsyncIOMotorClient

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)  # Pyrogram/Motor isi loop par bandhenge

from pyrogram import Client, filters
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import (FloodWait, RPCError, MessageNotModified, SessionPasswordNeeded,
                             PhoneCodeInvalid, PhoneCodeExpired)
from pyrogram.types import (ChatJoinRequest, Message, CallbackQuery,
                            InlineKeyboardMarkup, InlineKeyboardButton as B)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("joinbot")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
MONGO_URI = os.environ["MONGO_URI"]
OWNERS = {int(x) for x in os.environ.get("OWNER_IDS", "").replace(" ", "").split(",") if x}
SESSION_STRING = os.environ.get("SESSION_STRING")      # optional (bot ke andar bhi login ho sakta hai)
PORT = int(os.environ.get("PORT", 8080))
SELF_URL = os.environ.get("RENDER_EXTERNAL_URL")
WORKERS = int(os.environ.get("WORKERS", 8))
MAX_CHATS = int(os.environ.get("MAX_CHATS_PER_USER", 10))  # public mode me ek user ke max chats
PAGE = 8

db = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=8000)["joinbot"]
bot = Client("bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)
user = None      # helper (user) account
helper = None    # helper ki details: {"id", "name", "username"}
BOT_ID = {"id": 0}
STATE = {"public": False}

queue: asyncio.Queue = asyncio.Queue()
old_tasks: dict = {}
old_sem = asyncio.Semaphore(1)      # ek time par ek hi old-approve task chalega (flood se bachav)
logins: dict = {}
pending: dict = {}                   # uid -> ("add",) / ("cdelay", cid) / ("whyid", cid) / ...

DEFAULTS = {"active": True, "auto_all": True, "min": 2, "max": 15, "welcome_on": False, "public": False,
            "welcome": "Hello {name}! {chat} me aapki request approve ho gayi hai."}
tick = lambda b: "✅" if b else "❌"
clean = lambda t: re.sub(r"[\[\]*_`~|]", "", str(t))

REASONS = {
    "USER_CHANNELS_TOO_MUCH": ("🚫", "User Telegram ki 500 channels/groups wali limit par hai, isliye aur kisi me join nahi ho sakta. (Koi upay nahi)"),
    "USER_DEACTIVATED": ("👻", "Account delete/deactivate ho chuka hai."),
    "INPUT_USER_DEACTIVATED": ("👻", "Account delete/deactivate ho chuka hai."),
    "USER_DEACTIVATED_BAN": ("⛔", "Account Telegram ne ban kar diya hai."),
    "HIDE_REQUESTER_MISSING": ("↩️", "Request ab maujood nahi: user ne wapas le li ya kisi aur admin ne pehle hi handle kar di."),
    "USER_ALREADY_PARTICIPANT": ("✔️", "User pehle se member hai."),
    "USER_BANNED_IN_CHANNEL": ("🔨", "User is chat me banned hai. Pehle unban karo."),
    "USER_PRIVACY_RESTRICTED": ("🔒", "User ki privacy settings ki wajah se add nahi ho sakta."),
    "USER_ID_INVALID": ("❓", "User nahi mila (deleted/invalid)."),
    "PEER_ID_INVALID": ("❓", "User nahi mila (deleted/invalid)."),
    "CHAT_ADMIN_REQUIRED": ("🔑", "Admin permission kam hai. 'Invite Users' permission do."),
}
def fmt_ts(ts) -> str:
    return time.strftime("%d %b %y, %H:%M", time.gmtime(float(ts) + 19800)) + " IST"

def reason_info(rid):
    return REASONS.get(rid, ("⚠️", f"Anjaan karan: {rid}"))
def reason_of(e):
    return getattr(e, "ID", None) or type(e).__name__


class UserError(Exception):
    """User ko seedha dikhne wala saaf error."""


def mention(name, uid):
    return f"[{clean(name or uid)}](tg://user?id={uid})"

def helper_note():
    if not helper:
        return "ℹ️ Purani pending requests wali facility abhi available nahi hai (admin ne helper account set nahi kiya)."
    uname = f" (@{helper['username']})" if helper.get("username") else ""
    return ("🕰 Purani pending requests approve karne ke liye is **helper account** ko apne chat me "
            "**admin** banao (\"Invite Users\" permission ke saath):\n\n"
            f"👤 {mention(helper['name'], helper['id'])}{uname}\nID: `{helper['id']}`")


def _owner(_, __, m):
    return bool(m.from_user) and m.from_user.id in OWNERS
def _user(_, __, m):
    return bool(m.from_user) and (m.from_user.id in OWNERS or STATE["public"])
is_owner = filters.create(_owner)
is_user = filters.create(_user)


def safe(fn):
    @functools.wraps(fn)
    async def w(client, update, *a, **k):
        try:
            return await fn(client, update, *a, **k)
        except Exception as e:
            target = update.message if isinstance(update, CallbackQuery) else update
            if isinstance(e, UserError):
                text = f"❌ {e}"
            else:
                log.exception("handler error in %s", fn.__name__)
                uid = update.from_user.id if getattr(update, "from_user", None) else 0
                text = (f"❌ **Error** (`{fn.__name__}`)\n`{type(e).__name__}: {str(e)[:600]}`"
                        if uid in OWNERS else "❌ Kuch gadbad ho gayi, thodi der baad try karo.")
            try:
                await target.reply(text)
            except Exception:
                pass
    return w


# ---------------- settings helpers ----------------
async def gcfg() -> dict:
    return {**DEFAULTS, **(await db.settings.find_one({"_id": "global"}) or {})}

async def gset(**kw):
    await db.settings.update_one({"_id": "global"}, {"$set": kw}, upsert=True)

async def ccfg(cid: int):
    return await db.chats.find_one({"_id": cid})

def welcome_of(c: dict, g: dict):
    """Per-chat welcome. User ke chat par owner ka global welcome kabhi nahi jata."""
    oid = c.get("owner_id")
    if "welcome_on" in c:
        return c["welcome_on"], c.get("welcome") or g["welcome"]
    if oid is not None and oid not in OWNERS:
        return False, g["welcome"]
    return g["welcome_on"], g["welcome"]

async def effective(cid: int):
    g = await gcfg()
    if not g["active"]:
        return None
    c = await ccfg(cid)
    if c is None:
        if not g["auto_all"]:
            return None
        c = {}
    if c.get("enabled", True) is False:
        return None
    w_on, w_text = welcome_of(c, g)
    return {"min": c.get("min", g["min"]), "max": c.get("max", g["max"]),
            "welcome_on": w_on, "welcome": w_text}

async def can_manage(uid: int, cid: int) -> bool:
    if uid in OWNERS:
        return True
    c = await ccfg(cid)
    return bool(c) and c.get("owner_id") == uid

async def register_chat(chat, owner_id: int, force: bool = False):
    await db.chats.update_one({"_id": chat.id}, {"$set": {"title": chat.title}, "$setOnInsert": {"enabled": True}}, upsert=True)
    if force:
        await db.chats.update_one({"_id": chat.id}, {"$set": {"owner_id": owner_id, "enabled": True}})
    else:
        await db.chats.update_one({"_id": chat.id, "owner_id": {"$exists": False}}, {"$set": {"owner_id": owner_id}})

async def record_fail(cid, uid, name, username, rid, src):
    try:
        await db.fails.update_one({"_id": f"{cid}:{uid}"},
                                  {"$set": {"chat_id": cid, "user_id": uid, "name": name, "username": username,
                                            "reason": rid, "src": src, "ts": time.time()}}, upsert=True)
    except Exception:
        log.exception("record_fail")

async def touch_user(u):
    """Bot se baat karne wale har user ka record (sirf admin dekh sakta hai)."""
    if not u or getattr(u, "is_bot", False):
        return
    now = time.time()
    try:
        await db.users.update_one(
            {"_id": u.id},
            {"$set": {"first_name": u.first_name, "last_name": u.last_name, "username": u.username, "last_seen": now},
             "$setOnInsert": {"started": now}}, upsert=True)
    except Exception:
        log.exception("touch_user")

async def clear_fail(cid, uid):
    try:
        await db.fails.delete_one({"_id": f"{cid}:{uid}"})
    except Exception:
        pass


# ---------------- new requests ----------------
@bot.on_chat_join_request()
async def on_request(_, r: ChatJoinRequest):
    cfg = await effective(r.chat.id)
    if cfg is None:
        return
    await db.chats.update_one({"_id": r.chat.id}, {"$setOnInsert": {"title": r.chat.title}}, upsert=True)
    await queue.put((r.chat.id, r.chat.title, r.from_user.id, r.from_user.first_name, r.from_user.username, cfg))

async def worker():
    while True:
        cid, title, uid, name, uname, cfg = await queue.get()
        try:
            await asyncio.sleep(random.uniform(cfg["min"], cfg["max"]))  # human-like delay
            while True:
                try:
                    await bot.approve_chat_join_request(cid, uid)
                    break
                except FloodWait as e:
                    await asyncio.sleep(e.value + 1)
            await db.chats.update_one({"_id": cid}, {"$inc": {"approved": 1}})
            await db.settings.update_one({"_id": "global"}, {"$inc": {"total": 1}}, upsert=True)
            await clear_fail(cid, uid)
            if cfg["welcome_on"]:
                try:
                    await bot.send_message(uid, cfg["welcome"].format(name=name, chat=title))
                except Exception:
                    pass
        except Exception as e:
            rid = reason_of(e)
            log.warning("approve fail %s/%s: %s", cid, uid, rid)
            await record_fail(cid, uid, name, uname, rid, "new")
        finally:
            queue.task_done()


# ---------------- helper (user) account ----------------
async def stop_user():
    global user, helper
    for t in list(old_tasks.values()):
        t.cancel()
    if user:
        try:
            await user.stop()
        except Exception:
            pass
    user, helper = None, None

async def start_user(sess: str):
    global user, helper
    await stop_user()
    c = Client("user", api_id=API_ID, api_hash=API_HASH, session_string=sess, in_memory=True)
    await c.start()
    me = await c.get_me()
    user = c
    helper = {"id": me.id, "name": me.first_name or "Helper", "username": me.username}

async def load_user():
    doc = await db.settings.find_one({"_id": "session"})
    sess = (doc or {}).get("string") or SESSION_STRING
    if sess:
        await start_user(sess)

async def ensure_peer(cid: int) -> bool:
    try:
        await user.get_chat(cid)
        return True
    except Exception:
        pass
    try:
        async for _ in user.get_dialogs():  # peer cache bharo
            pass
        await user.get_chat(cid)
        return True
    except Exception:
        return False

async def helper_status(cid: int) -> str:
    if not await ensure_peer(cid):
        return "notmember"
    try:
        m = await user.get_chat_member(cid, "me")
    except Exception:
        return "notmember"
    if m.status == ChatMemberStatus.OWNER:
        return "ok"
    if m.status == ChatMemberStatus.ADMINISTRATOR:
        return "ok" if (m.privileges and m.privileges.can_invite_users) else "noperm"
    return "notadmin"

async def old_precheck(cid: int):
    """None => theek hai, warna user ko dikhane wala message."""
    if not user:
        return helper_note()
    if cid in old_tasks:
        return "Is chat ka old-approve task pehle se chal raha hai."
    st = await helper_status(cid)
    msgs = {"notmember": "❌ Helper account abhi is chat me nahi hai.",
            "notadmin": "❌ Helper account is chat me hai par admin nahi hai.",
            "noperm": "❌ Helper account admin hai par 'Invite Users' permission nahi hai."}
    if st != "ok":
        return msgs[st] + "\n\n" + helper_note() + "\n\nAdmin banane ke baad dobara try karo."
    return None

def fail_summary(cnt: Counter) -> str:
    lines = []
    for rid, n in cnt.most_common():
        e, d = reason_info(rid)
        lines.append(f"{e} **×{n}** — {d}")
    return "\n".join(lines)

async def approve_old(cid: int, notify: int):
    g = await gcfg()
    c = await ccfg(cid) or {}
    lo, hi = c.get("min", g["min"]), c.get("max", g["max"])
    done, fails = 0, Counter()
    try:
        if old_sem.locked():
            await bot.send_message(notify, "⏳ Pehle ka old-approve chal raha hai, aapka number queue me hai...")
        async with old_sem:
            if not await ensure_peer(cid):
                raise RuntimeError("Helper account is chat ka member nahi hai")
            async for req in user.get_chat_join_requests(cid):
                u = req.user
                while True:
                    try:
                        await user.approve_chat_join_request(cid, u.id)
                        done += 1
                        await db.chats.update_one({"_id": cid}, {"$inc": {"approved": 1}}, upsert=True)
                        await clear_fail(cid, u.id)
                        break
                    except FloodWait as e:
                        await asyncio.sleep(e.value + 1)
                    except Exception as e:
                        rid = reason_of(e)
                        fails[rid] += 1
                        await record_fail(cid, u.id, u.first_name, u.username, rid, "old")
                        break
                await asyncio.sleep(random.uniform(lo, hi))
                if done and done % 50 == 0:
                    await bot.send_message(notify, f"⏳ `{cid}` → {done} approve ho chuke...")
        text = f"✅ Old requests done `{cid}`\nApproved: {done} | Failed: {sum(fails.values())}"
        if fails:
            text += "\n\n" + fail_summary(fails) + "\n\nDetail: chat → ❌ Failed Report"
        await bot.send_message(notify, text)
    except asyncio.CancelledError:
        await bot.send_message(notify, f"🛑 Stopped `{cid}`. Approved: {done}")
    except Exception as e:
        await bot.send_message(notify, f"❌ Error `{cid}`: {type(e).__name__}: {e}")
    finally:
        old_tasks.pop(cid, None)


# ---------------- session login (sirf owner) ----------------
async def cancel_login(uid: int):
    pending.pop(uid, None)
    lg = logins.pop(uid, None)
    if lg:
        try:
            await lg["c"].disconnect()
        except Exception:
            pass

async def try_delete(m: Message):
    try:
        await m.delete()
    except Exception:
        pass

async def finish_login(m: Message, uid: int):
    lg = logins.pop(uid)
    sess = await lg["c"].export_session_string()
    await lg["c"].disconnect()
    pending.pop(uid, None)
    await db.settings.update_one({"_id": "session"}, {"$set": {"string": sess}}, upsert=True)
    await start_user(sess)
    await m.reply("✅ Login ho gaya! Session save ho gaya, Render me kuch badalna nahi hai.")
    await show(m, *await session_panel())


# ---------------- panels ----------------
async def show(x, text, markup):
    try:
        if isinstance(x, CallbackQuery):
            await x.message.edit_text(text, reply_markup=markup)
        else:
            await x.reply(text, reply_markup=markup)
    except MessageNotModified:
        pass

async def main_panel(uid: int):
    g = await gcfg()
    if uid in OWNERS:
        pub = STATE["public"]
        text = (f"**⚙️ Auto Join Request Bot (Admin)**\n\n"
                f"Mode: {'🌐 Public' if pub else '🔒 Private'}\n"
                f"Delay: {g['min']}-{g['max']}s | Queue: {queue.qsize()}\n"
                f"Total approved: {g.get('total', 0)}\n"
                f"Helper session: {'✅ ' + clean(helper['name']) if helper else '❌'}\n\n"
                "Public mode me Auto-all OFF rakhna behtar hai.")
        kb = [[B(f"Bot Active {tick(g['active'])}", callback_data="g:active")],
              [B(f"Auto-all chats {tick(g['auto_all'])}", callback_data="g:auto_all")],
              [B(f"Welcome DM (global) {tick(g['welcome_on'])}", callback_data="g:welcome_on")],
              [B("📺 Chats", callback_data="chats"), B("⏱ Delay", callback_data="delay")],
              [B("👋 Welcome Text", callback_data="wt"), B("🔑 Old-Req Session", callback_data="ses")],
              [B(f"Bot Mode: {'🌐 Public' if pub else '🔒 Private'} (badlo)", callback_data="pub")],
              [B("👥 Users", callback_data="us:0"), B("🔄 Refresh", callback_data="main")]]
    else:
        n, tot = 0, 0
        async for c in db.chats.find({"owner_id": uid}):
            n += 1
            tot += c.get("approved", 0)
        text = ("**🤖 Auto Join Request Bot**\n\n"
                "Apne channel/group ki join requests automatic approve karwao.\n\n"
                "1️⃣ Bot ko apne chat me **admin** banao (\"Invite Users\" permission ke saath)\n"
                "2️⃣ Chat apne aap add ho jayega, ya ➕ Add Chat dabao\n"
                "3️⃣ 📺 My Chats me settings karo\n\n"
                f"Aapke chats: {n} | Approved: {tot}")
        kb = [[B("📺 My Chats", callback_data="chats"), B("➕ Add Chat", callback_data="add")],
              [B("ℹ️ Old Requests Help", callback_data="oh")],
              [B("🔄 Refresh", callback_data="main")]]
    return text, InlineKeyboardMarkup(kb)

async def chats_panel(uid: int, page=0):
    q = {} if uid in OWNERS else {"owner_id": uid}
    docs = [c async for c in db.chats.find(q).sort("title", 1)]
    chunk = docs[page * PAGE:(page + 1) * PAGE]
    rows = [[B(f"{'🟢' if c.get('enabled', True) else '🔴'} {str(c.get('title') or c['_id'])[:24]} · {c.get('approved', 0)}",
               callback_data=f"c:{c['_id']}")] for c in chunk]
    nav = []
    if page > 0:
        nav.append(B("⬅️", callback_data=f"p:{page - 1}"))
    if (page + 1) * PAGE < len(docs):
        nav.append(B("➡️", callback_data=f"p:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([B("➕ Add Chat", callback_data="add"), B("🔙 Back", callback_data="main")])
    return f"**📺 Chats ({len(docs)})**\nChat par tap karke uski settings kholo.", InlineKeyboardMarkup(rows)

async def chat_panel(cid: int):
    c, g = await ccfg(cid) or {}, await gcfg()
    on = c.get("enabled", True)
    lo, hi = c.get("min", g["min"]), c.get("max", g["max"])
    w_on, _w = welcome_of(c, g)
    running = cid in old_tasks
    nfail = await db.fails.count_documents({"chat_id": cid})
    text = (f"**{clean(c.get('title', cid))}**\n`{cid}`\n\nApprove: {tick(on)}\n"
            f"Delay: {lo}-{hi}s {'(custom)' if 'min' in c else '(global)'}\n"
            f"Welcome DM: {tick(w_on)}\nApproved: {c.get('approved', 0)} | Failed records: {nfail}\n"
            f"Old task: {'running ▶️' if running else 'idle'}")
    kb = InlineKeyboardMarkup([
        [B(f"Approve {tick(on)}", callback_data=f"ct:{cid}")],
        [B("⏱ Custom Delay", callback_data=f"cd:{cid}"), B("♻️ Reset Delay", callback_data=f"cr:{cid}")],
        [B(f"Welcome DM {tick(w_on)}", callback_data=f"cw:{cid}"), B("✏️ Welcome Text", callback_data=f"ce:{cid}")],
        [B("⏹ Stop Old Approve", callback_data=f"cs:{cid}") if running
         else B("🕰 Approve Old Requests", callback_data=f"co:{cid}")],
        [B("❌ Failed Report", callback_data=f"fr:{cid}"), B("ℹ️ Helper Info", callback_data="oh")],
        [B("🗑 Remove", callback_data=f"cx:{cid}"), B("🔙 Back", callback_data="chats")]])
    return text, kb

async def fails_panel(cid: int):
    c = await ccfg(cid) or {}
    docs = [f async for f in db.fails.find({"chat_id": cid}).sort("ts", -1).limit(2000)]
    title = clean(c.get("title", cid))
    if not docs:
        text = f"**❌ Failed Report — {title}**\n\n✅ Koi failed request record nahi hai."
    else:
        cnt = Counter(f["reason"] for f in docs)
        recent = []
        for f in docs[:12]:
            e, _d = reason_info(f["reason"])
            un = f" @{f['username']}" if f.get("username") else ""
            recent.append(f"{e} {mention(f.get('name'), f['user_id'])}{un} `{f['user_id']}` — {f['reason']}")
        text = (f"**❌ Failed Report — {title}**\nTotal: {len(docs)}\n\n{fail_summary(cnt)}\n\n"
                "**Recent:**\n" + "\n".join(recent))[:4000]
    kb = InlineKeyboardMarkup([
        [B("🔍 Ek user check karo", callback_data=f"fq:{cid}"), B("🧹 Report saaf", callback_data=f"fc:{cid}")],
        [B("🔙 Back", callback_data=f"c:{cid}")]])
    return text, kb

USERS_PER_PAGE = 10

async def users_panel(page: int = 0):
    total = await db.users.count_documents({})
    pages = max(1, -(-total // USERS_PER_PAGE))
    page = max(0, min(page, pages - 1))
    now = time.time()
    new24 = await db.users.count_documents({"started": {"$gte": now - 86400}})
    act24 = await db.users.count_documents({"last_seen": {"$gte": now - 86400}})
    docs = [u async for u in db.users.find({}).sort("started", -1).skip(page * USERS_PER_PAGE).limit(USERS_PER_PAGE)]
    lines = []
    for i, u in enumerate(docs, page * USERS_PER_PAGE + 1):
        n_chats, appr = 0, 0
        async for c in db.chats.find({"owner_id": u["_id"]}):
            n_chats += 1
            appr += c.get("approved", 0)
        full = " ".join(x for x in (u.get("first_name"), u.get("last_name")) if x) or "NoName"
        uname = f"@{u['username']}" if u.get("username") else "no username"
        crown = "👑 " if u["_id"] in OWNERS else ""
        lines.append(f"**{i}.** {crown}{mention(full, u['_id'])}\n"
                     f"   {uname} | ID: `{u['_id']}`\n"
                     f"   Start: {fmt_ts(u.get('started', 0))}\n"
                     f"   Last: {fmt_ts(u.get('last_seen', 0))}\n"
                     f"   Chats: {n_chats} | Approved: {appr}")
    head = (f"**👥 Users**\nTotal: {total} | Naye (24h): {new24} | Active (24h): {act24}\n"
            f"Page {page + 1}/{pages}\n\n")
    text = (head + ("\n\n".join(lines) if lines else "Abhi koi user record nahi hai."))[:4000]
    nav = []
    if page > 0:
        nav.append(B("⬅️ Back", callback_data=f"us:{page - 1}"))
    nav.append(B(f"{page + 1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(B("Next ➡️", callback_data=f"us:{page + 1}"))
    return text, InlineKeyboardMarkup([nav, [B("🔙 Menu", callback_data="main")]])


async def delay_panel():
    g = await gcfg()
    kb = InlineKeyboardMarkup([
        [B("🚀 Fast 1-5s", callback_data="dp:1:5"), B("🙂 Normal 2-15s", callback_data="dp:2:15")],
        [B("🐢 Slow 10-40s", callback_data="dp:10:40"), B("✏️ Custom", callback_data="dc")],
        [B("🔙 Back", callback_data="main")]])
    return f"**⏱ Global Delay**\nAbhi: {g['min']}-{g['max']}s", kb

async def session_panel():
    if user:
        text = (f"**🔑 Old-Request Session**\n\nLogged in: {mention(helper['name'], helper['id'])} (`{helper['id']}`) ✅\n\n"
                "Ye account har chat me **admin** hona chahiye (Invite Users). Users ko bot ye details khud dikhata hai.")
        rows = [[B("🚪 Logout / Remove", callback_data="lo")], [B("🔙 Back", callback_data="main")]]
    else:
        text = ("**🔑 Old-Request Session**\n\nAbhi koi session nahi hai ❌\n"
                "Purani pending requests ke liye ek user account login karo (better: alag/secondary account).")
        rows = [[B("📲 Login (Phone + OTP)", callback_data="lg")],
                [B("📋 Paste Session String", callback_data="ls")],
                [B("🔙 Back", callback_data="main")]]
    return text, InlineKeyboardMarkup(rows)

def helper_panel():
    return (helper_note() + "\n\nAdmin banane ke baad: 📺 Chats → apna chat → 🕰 Approve Old Requests.",
            InlineKeyboardMarkup([[B("🔙 Back", callback_data="main")]]))


# ---------------- commands ----------------
@bot.on_message(filters.command(["start", "settings", "help"]) & filters.private)
@safe
async def start(_, m: Message):
    uid = m.from_user.id
    await touch_user(m.from_user)
    if uid not in OWNERS and not STATE["public"]:
        return await m.reply(f"🔒 Ye bot abhi private hai.\nAapki ID: `{uid}`")
    await cancel_login(uid)
    await show(m, *await main_panel(uid))

@bot.on_message(filters.command("id") & filters.private)
async def my_id(_, m: Message):
    uid = m.from_user.id
    extra = f"\nOWNER_IDS: `{sorted(OWNERS)}`\nOwner match: {uid in OWNERS}" if uid in OWNERS or not OWNERS else ""
    await m.reply(f"Aapki ID: `{uid}`{extra}")

async def mongo_status() -> str:
    try:
        await asyncio.wait_for(db.command("ping"), 10)
        return "✅ MongoDB OK"
    except Exception as e:
        return f"❌ MongoDB error: {type(e).__name__}: {str(e)[:500]}"

@bot.on_message(filters.command("ping") & filters.private)
async def ping(_, m: Message):
    if m.from_user.id not in OWNERS:
        return await m.reply("🏓 pong")
    await m.reply(f"🏓 pong\n{await mongo_status()}\nHelper session: {'✅' if user else '❌'}\n"
                  f"Mode: {'🌐 Public' if STATE['public'] else '🔒 Private'}")

@bot.on_message(filters.command(["public", "private"]) & filters.private & is_owner)
@safe
async def cmd_mode(_, m: Message):
    pub = m.command[0] == "public"
    STATE["public"] = pub
    await gset(public=pub)
    await m.reply("🌐 Bot ab **Public** hai: koi bhi apne chat add kar sakta hai." if pub
                  else "🔒 Bot ab **Private** hai: sirf admin panel use kar sakta hai.\n"
                       "(Pehle se add chats me approve chalta rahega.)")

@bot.on_message(filters.command("users") & filters.private & is_owner)
@safe
async def cmd_users(_, m: Message):
    await show(m, *await users_panel(0))

@bot.on_message(filters.command("approveold") & filters.private & is_owner)
@safe
async def cmd_old(_, m: Message):
    cid = int(m.command[1])
    err = await old_precheck(cid)
    if err:
        return await m.reply(err)
    old_tasks[cid] = asyncio.create_task(approve_old(cid, m.chat.id))
    await m.reply("▶️ Started")

@bot.on_message(filters.command("stopold") & filters.private & is_owner)
@safe
async def cmd_stop(_, m: Message):
    t = old_tasks.get(int(m.command[1]))
    if t:
        t.cancel()
    await m.reply("🛑 Stopping..." if t else "Koi task nahi.")


# ---------------- chat add / membership ----------------
async def verify_add(uid: int, chat):
    if uid not in OWNERS:
        if await db.chats.count_documents({"owner_id": uid, "_id": {"$ne": chat.id}}) >= MAX_CHATS:
            raise UserError(f"Ek user max {MAX_CHATS} chats add kar sakta hai.")
        try:
            mem = await bot.get_chat_member(chat.id, uid)
        except Exception:
            raise UserError("Aap is chat ke admin nahi lag rahe.")
        if mem.status not in (ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR):
            raise UserError("Sirf chat ke admin/owner hi chat add kar sakte hain.")
    try:
        me = await bot.get_chat_member(chat.id, "me")
    except Exception:
        raise UserError("Bot is chat me nahi hai. Pehle bot ko chat me add karke admin banao.")
    if me.status != ChatMemberStatus.ADMINISTRATOR:
        raise UserError("Bot is chat me admin nahi hai. Pehle bot ko admin banao.")
    if not (me.privileges and me.privileges.can_invite_users):
        raise UserError("Bot ko \"Invite Users\" (Add Members) permission do.")

@bot.on_chat_member_updated()
async def on_member(_, u):
    """Bot ko admin banate hi chat user ke naam register ho jata hai."""
    try:
        new = u.new_chat_member
        if not new or not new.user or new.user.id != BOT_ID["id"] or new.status != ChatMemberStatus.ADMINISTRATOR:
            return
        if not u.from_user:
            return
        uid = u.from_user.id
        if uid not in OWNERS:
            if not STATE["public"]:
                return
            if await db.chats.count_documents({"owner_id": uid, "_id": {"$ne": u.chat.id}}) >= MAX_CHATS:
                await bot.send_message(uid, f"❌ Aap max {MAX_CHATS} chats hi add kar sakte ho.")
                return
        await register_chat(u.chat, uid)
        c = await ccfg(u.chat.id) or {}
        if uid not in OWNERS and c.get("owner_id") != uid:
            return
        warn = "" if (new.privileges and new.privileges.can_invite_users) else \
            "\n\n⚠️ Bot ko \"Invite Users\" permission nahi mili, iske bina requests approve nahi hongi."
        await bot.send_message(uid, f"✅ **{clean(u.chat.title)}** add ho gaya!{warn}\n\n{helper_note()}\n\n"
                                    "Settings ke liye /settings → 📺 Chats")
    except Exception as e:
        log.warning("on_member: %s", e)


# ---------------- callbacks ----------------
OWNER_ACTS = {"us", "g", "ses", "lg", "ls", "lo", "delay", "dp", "dc", "wt", "pub"}
CHAT_ACTS = {"c", "ct", "cr", "cd", "co", "cs", "cx", "cw", "ce", "fr", "fc", "fq"}

@bot.on_callback_query(is_user)
@safe
async def cb(_, q: CallbackQuery):
    act, _, arg = q.data.partition(":")
    uid = q.from_user.id
    await touch_user(q.from_user)
    try:
        if act in OWNER_ACTS and uid not in OWNERS:
            return await q.answer("Ye option sirf admin ke liye hai.", show_alert=True)
        if act in CHAT_ACTS and not await can_manage(uid, int(arg)):
            return await q.answer("Aapko is chat ki access nahi hai.", show_alert=True)

        if act == "noop":
            return
        if act == "us":
            return await show(q, *await users_panel(int(arg)))
        if act == "main":
            await cancel_login(uid)
            return await show(q, *await main_panel(uid))
        if act == "g":
            g = await gcfg()
            await gset(**{arg: not g[arg]})
            return await show(q, *await main_panel(uid))
        if act == "pub":
            STATE["public"] = not STATE["public"]
            await gset(public=STATE["public"])
            return await show(q, *await main_panel(uid))
        if act == "chats":
            await cancel_login(uid)
            return await show(q, *await chats_panel(uid))
        if act == "p":
            return await show(q, *await chats_panel(uid, int(arg)))
        if act == "oh":
            return await show(q, *helper_panel())
        if act == "add":
            pending[uid] = ("add",)
            return await q.message.reply("Chat ID bhejo (`-100...`), @username, ya channel/group ka koi message forward karo.\n"
                                         "Bot us chat me admin hona chahiye aur aap bhi us chat ke admin.")
        # ---- chat actions
        if act == "c":
            return await show(q, *await chat_panel(int(arg)))
        if act == "ct":
            cid = int(arg)
            c = await ccfg(cid) or {}
            await db.chats.update_one({"_id": cid}, {"$set": {"enabled": not c.get("enabled", True)}}, upsert=True)
            return await show(q, *await chat_panel(cid))
        if act == "cr":
            await db.chats.update_one({"_id": int(arg)}, {"$unset": {"min": "", "max": ""}})
            return await show(q, *await chat_panel(int(arg)))
        if act == "cd":
            pending[uid] = ("cdelay", int(arg))
            return await q.message.reply("Is chat ka delay bhejo: `min max`  (jaise `3 12`)")
        if act == "cw":
            cid = int(arg)
            c, g = await ccfg(cid) or {}, await gcfg()
            on, _w = welcome_of(c, g)
            await db.chats.update_one({"_id": cid}, {"$set": {"welcome_on": not on}}, upsert=True)
            return await show(q, *await chat_panel(cid))
        if act == "ce":
            pending[uid] = ("cwelcome", int(arg))
            return await q.message.reply("Is chat ke liye naya welcome DM text bhejo. `{name}` aur `{chat}` use kar sakte ho.")
        if act == "co":
            cid = int(arg)
            err = await old_precheck(cid)
            if err:
                return await q.message.reply(err)
            old_tasks[cid] = asyncio.create_task(approve_old(cid, uid))
            await q.answer("Old approve shuru ▶️")
            return await show(q, *await chat_panel(cid))
        if act == "cs":
            t = old_tasks.get(int(arg))
            if t:
                t.cancel()
            await q.answer("Stopping...")
            await asyncio.sleep(1)
            return await show(q, *await chat_panel(int(arg)))
        if act == "cx":
            await db.chats.delete_one({"_id": int(arg)})
            await db.fails.delete_many({"chat_id": int(arg)})
            await q.answer("Removed")
            return await show(q, *await chats_panel(uid))
        if act == "fr":
            return await show(q, *await fails_panel(int(arg)))
        if act == "fc":
            await db.fails.delete_many({"chat_id": int(arg)})
            await q.answer("Report saaf")
            return await show(q, *await fails_panel(int(arg)))
        if act == "fq":
            pending[uid] = ("whyid", int(arg))
            return await q.message.reply("Jis user ka reason dekhna hai uski **User ID** bhejo (number).")
        # ---- owner only
        if act == "ses":
            return await show(q, *await session_panel())
        if act == "lg":
            await cancel_login(uid)
            pending[uid] = ("lphone",)
            return await q.message.reply("📲 Helper account ka phone number bhejo country code ke saath, jaise `+919876543210`")
        if act == "ls":
            pending[uid] = ("lstr",)
            return await q.message.reply("📋 Pyrogram SESSION_STRING paste karo (message turant delete kar diya jayega).")
        if act == "lo":
            await stop_user()
            await db.settings.delete_one({"_id": "session"})
            await q.answer("Logout ho gaya")
            return await show(q, *await session_panel())
        if act == "delay":
            return await show(q, *await delay_panel())
        if act == "dp":
            lo, hi = arg.split(":")
            await gset(min=float(lo), max=float(hi))
            return await show(q, *await delay_panel())
        if act == "dc":
            pending[uid] = ("delay",)
            return await q.message.reply("Global delay bhejo: `min max`  (jaise `2 15`)")
        if act == "wt":
            pending[uid] = ("welcome",)
            g = await gcfg()
            return await q.message.reply(f"Abhi: `{g['welcome']}`\n\nNaya global welcome text bhejo. "
                                         "`{name}` aur `{chat}` use kar sakte ho.")
    finally:
        try:
            await q.answer()
        except Exception:
            pass


# ---------------- text input (button ke baad) ----------------
OWNER_KINDS = {"lphone", "lcode", "lpass", "lstr", "delay", "welcome"}

def check_template(text: str):
    try:
        text.format(name="x", chat="y")
    except Exception:
        raise UserError("Text me sirf {name} aur {chat} use kar sakte ho (doosre { } nahi).")

@bot.on_message(filters.private & is_user & ~filters.regex(r"^/"), group=1)
@safe
async def on_input(_, m: Message):
    uid = m.from_user.id
    st = pending.get(uid)
    if not st:
        return
    await touch_user(m.from_user)
    kind = st[0]
    if kind in OWNER_KINDS and uid not in OWNERS:
        pending.pop(uid, None)
        return
    try:
        if kind == "lphone":
            phone = m.text.strip().replace(" ", "")
            tmp = Client(f"login{uid}", api_id=API_ID, api_hash=API_HASH, in_memory=True)
            await tmp.connect()
            try:
                sent = await tmp.send_code(phone)
            except Exception:
                await tmp.disconnect()
                raise
            logins[uid] = {"c": tmp, "phone": phone, "hash": sent.phone_code_hash}
            pending[uid] = ("lcode",)
            return await m.reply("📨 Telegram ne us account me OTP bheja hai.\n\n"
                                 "Code **spaces ke saath** bhejo, jaise `1 2 3 4 5` "
                                 "(seedha bhejoge to Telegram code block kar deta hai).")
        if kind == "lcode":
            lg = logins[uid]
            code = "".join(ch for ch in m.text if ch.isdigit())
            await try_delete(m)
            try:
                await lg["c"].sign_in(lg["phone"], lg["hash"], code)
            except SessionPasswordNeeded:
                pending[uid] = ("lpass",)
                return await m.reply("🔐 2-step verification password bhejo (message delete ho jayega).")
            return await finish_login(m, uid)
        if kind == "lpass":
            lg = logins[uid]
            await try_delete(m)
            await lg["c"].check_password(m.text)
            return await finish_login(m, uid)
        if kind == "lstr":
            await try_delete(m)
            await start_user(m.text.strip())
            await db.settings.update_one({"_id": "session"}, {"$set": {"string": m.text.strip()}}, upsert=True)
            pending.pop(uid, None)
            await m.reply("✅ Session set ho gaya.")
            return await show(m, *await session_panel())
        if kind == "add":
            fo = getattr(m, "forward_from_chat", None) or getattr(getattr(m, "forward_origin", None), "chat", None)
            ref = fo.id if fo else (m.text or "").strip()
            ref = int(ref) if str(ref).lstrip("-").isdigit() else ref
            try:
                chat = await bot.get_chat(ref)
            except Exception:
                raise UserError("Chat nahi mila. Pehle bot ko us chat me admin banao, phir ID/@username "
                                "ya forwarded message bhejo.")
            await verify_add(uid, chat)
            pending.pop(uid, None)
            await register_chat(chat, uid, force=(uid not in OWNERS))
            await m.reply(f"✅ Added: {clean(chat.title)}\n\n{helper_note()}")
            return await show(m, *await chat_panel(chat.id))
        if kind in ("delay", "cdelay"):
            lo, hi = sorted(float(x) for x in m.text.split()[:2])
            if lo < 0 or (uid not in OWNERS and (lo < 1 or hi > 300)):
                raise UserError("Delay 1 se 300 second ke beech rakho.")
            if kind == "delay":
                await gset(min=lo, max=hi)
                pending.pop(uid, None)
                return await show(m, *await delay_panel())
            await db.chats.update_one({"_id": st[1]}, {"$set": {"min": lo, "max": hi}}, upsert=True)
            pending.pop(uid, None)
            return await show(m, *await chat_panel(st[1]))
        if kind == "welcome":
            check_template(m.text)
            await gset(welcome=m.text, welcome_on=True)
            pending.pop(uid, None)
            await m.reply("✅ Global welcome text save + ON")
            return await show(m, *await main_panel(uid))
        if kind == "cwelcome":
            check_template(m.text)
            await db.chats.update_one({"_id": st[1]}, {"$set": {"welcome": m.text, "welcome_on": True}}, upsert=True)
            pending.pop(uid, None)
            await m.reply("✅ Is chat ka welcome DM save + ON")
            return await show(m, *await chat_panel(st[1]))
        if kind == "whyid":
            target = int(m.text.strip())
            f = await db.fails.find_one({"_id": f"{st[1]}:{target}"})
            pending.pop(uid, None)
            if not f:
                await m.reply("Is user ka failed record nahi mila. Ya to request approve ho chuki hai, "
                              "ya abhi tak try hi nahi hui.")
            else:
                e, d = reason_info(f["reason"])
                when = fmt_ts(f["ts"])
                await m.reply(f"{e} {mention(f.get('name'), target)} `{target}`\n\n**Reason:** {d}\n"
                              f"Code: `{f['reason']}`\nKab: {when} ({'purani' if f.get('src') == 'old' else 'nayi'} request)")
            return await show(m, *await fails_panel(st[1]))
    except Exception as e:
        if isinstance(e, UserError):
            return await m.reply(f"❌ {e}")
        if kind == "lphone" or isinstance(e, PhoneCodeExpired) or kind == "lstr":
            await cancel_login(uid)
            return await m.reply(f"❌ {type(e).__name__}: {str(e)[:500]}\nPhir se Login button dabao.")
        log.exception("input error")
        detail = f"{type(e).__name__}: {str(e)[:500]}" if uid in OWNERS else "Galat input"
        await m.reply(f"❌ {detail}\nDobara bhejo ya /settings dabao.")


# ---------------- web server + self ping ----------------
async def web_app():
    app = web.Application()
    app.add_routes([web.get("/", lambda r: web.Response(text="Bot is running ✅"))])
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()

async def self_ping():
    if not SELF_URL:
        return
    async with ClientSession() as s:
        while True:
            await asyncio.sleep(300)  # 5 min
            try:
                async with s.get(SELF_URL, timeout=20) as r:
                    log.info("self-ping %s", r.status)
            except Exception as e:
                log.warning("ping fail: %s", e)

async def main():
    await web_app()
    await bot.start()
    BOT_ID["id"] = (await bot.get_me()).id
    try:
        STATE["public"] = bool((await gcfg()).get("public", False))
    except Exception as e:
        log.warning("settings load fail: %s", e)
    try:
        await load_user()
    except Exception as e:
        log.warning("session load fail: %s", e)
    for _ in range(WORKERS):
        asyncio.create_task(worker())
    asyncio.create_task(self_ping())
    status = await mongo_status()
    log.info("Bot started | %s | owners=%s | public=%s", status, sorted(OWNERS), STATE["public"])
    for oid in OWNERS:
        try:
            await bot.send_message(oid, f"🚀 Bot started\n{status}\nMode: {'🌐 Public' if STATE['public'] else '🔒 Private'}"
                                        f"\nHelper: {'✅' if user else '❌'}")
        except Exception as e:
            log.warning("owner %s ko msg nahi gaya: %s", oid, e)
    await asyncio.Event().wait()

if __name__ == "__main__":
    loop.run_until_complete(main())
