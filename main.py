import os, asyncio, random, logging
from aiohttp import web, ClientSession
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, RPCError, MessageNotModified
from pyrogram.types import (ChatJoinRequest, Message, CallbackQuery,
                            InlineKeyboardMarkup, InlineKeyboardButton as B)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("joinbot")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
MONGO_URI = os.environ["MONGO_URI"]
OWNERS = {int(x) for x in os.environ.get("OWNER_IDS", "").replace(" ", "").split(",") if x}
SESSION_STRING = os.environ.get("SESSION_STRING")  # optional: old requests ke liye
PORT = int(os.environ.get("PORT", 8080))
SELF_URL = os.environ.get("RENDER_EXTERNAL_URL")
WORKERS = int(os.environ.get("WORKERS", 3))
PAGE = 8

db = AsyncIOMotorClient(MONGO_URI)["joinbot"]
bot = Client("bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)
user = Client("user", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING,
              in_memory=True) if SESSION_STRING else None

queue: asyncio.Queue = asyncio.Queue()
old_tasks: dict[int, asyncio.Task] = {}
pending: dict[int, tuple] = {}   # owner_id -> ("add",) / ("delay",) / ("cdelay", cid) / ("welcome",)

DEFAULTS = {"active": True, "auto_all": True, "min": 2, "max": 15, "welcome_on": False,
            "welcome": "Hello {name}! {chat} me aapki request approve ho gayi hai."}
tick = lambda b: "✅" if b else "❌"


# ---------------- settings helpers ----------------
async def gcfg() -> dict:
    return {**DEFAULTS, **(await db.settings.find_one({"_id": "global"}) or {})}

async def gset(**kw):
    await db.settings.update_one({"_id": "global"}, {"$set": kw}, upsert=True)

async def ccfg(cid: int):
    return await db.chats.find_one({"_id": cid})

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
    return {"min": c.get("min", g["min"]), "max": c.get("max", g["max"]),
            "welcome_on": g["welcome_on"], "welcome": g["welcome"]}

is_owner = filters.create(lambda _, __, m: bool(m.from_user) and m.from_user.id in OWNERS)


# ---------------- new requests ----------------
@bot.on_chat_join_request()
async def on_request(_, r: ChatJoinRequest):
    cfg = await effective(r.chat.id)
    if cfg is None:
        return
    await db.chats.update_one({"_id": r.chat.id}, {"$setOnInsert": {"title": r.chat.title}}, upsert=True)
    await queue.put((r.chat.id, r.chat.title, r.from_user.id, r.from_user.first_name, cfg))

async def worker():
    while True:
        cid, title, uid, name, cfg = await queue.get()
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
            if cfg["welcome_on"]:
                try:
                    await bot.send_message(uid, cfg["welcome"].format(name=name, chat=title))
                except Exception:
                    pass
        except RPCError as e:
            log.warning("approve fail %s/%s: %s", cid, uid, e)
        except Exception:
            log.exception("worker error")
        finally:
            queue.task_done()


# ---------------- old requests (user session) ----------------
async def approve_old(cid: int, notify: int):
    g = await gcfg()
    c = await ccfg(cid) or {}
    lo, hi = c.get("min", g["min"]), c.get("max", g["max"])
    done = fail = 0
    try:
        async for req in user.get_chat_join_requests(cid):
            try:
                await user.approve_chat_join_request(cid, req.user.id)
                done += 1
                await db.chats.update_one({"_id": cid}, {"$inc": {"approved": 1}}, upsert=True)
            except FloodWait as e:
                await asyncio.sleep(e.value + 1)
            except RPCError:
                fail += 1
            await asyncio.sleep(random.uniform(lo, hi))
            if done and done % 50 == 0:
                await bot.send_message(notify, f"⏳ `{cid}` → {done} approve ho chuke...")
        await bot.send_message(notify, f"✅ Old requests done `{cid}`\nApproved: {done} | Failed: {fail}")
    except asyncio.CancelledError:
        await bot.send_message(notify, f"🛑 Stopped `{cid}`. Approved: {done}")
    except Exception as e:
        await bot.send_message(notify, f"❌ Error `{cid}`: {e}")
    finally:
        old_tasks.pop(cid, None)


# ---------------- panels ----------------
async def show(x, text, markup):
    try:
        if isinstance(x, CallbackQuery):
            await x.message.edit_text(text, reply_markup=markup)
        else:
            await x.reply(text, reply_markup=markup)
    except MessageNotModified:
        pass

async def main_panel():
    g = await gcfg()
    text = (f"**⚙️ Auto Join Request Bot**\n\n"
            f"Delay: {g['min']}-{g['max']}s\nQueue: {queue.qsize()}\n"
            f"Total approved: {g.get('total', 0)}\n"
            f"Old-request session: {'✅' if user else '❌ (SESSION_STRING nahi)'}")
    kb = InlineKeyboardMarkup([
        [B(f"Bot Active {tick(g['active'])}", callback_data="g:active")],
        [B(f"Auto-all chats {tick(g['auto_all'])}", callback_data="g:auto_all")],
        [B(f"Welcome DM {tick(g['welcome_on'])}", callback_data="g:welcome_on")],
        [B("📺 Chats", callback_data="chats"), B("⏱ Delay", callback_data="delay")],
        [B("👋 Welcome Text", callback_data="wt"), B("🔄 Refresh", callback_data="main")]])
    return text, kb

async def chats_panel(page=0):
    docs = [c async for c in db.chats.find().sort("title", 1)]
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
    custom = "min" in c
    running = cid in old_tasks
    text = (f"**{c.get('title', cid)}**\n`{cid}`\n\nApprove: {tick(on)}\n"
            f"Delay: {lo}-{hi}s {'(custom)' if custom else '(global)'}\n"
            f"Approved: {c.get('approved', 0)}\nOld task: {'running ▶️' if running else 'idle'}")
    kb = InlineKeyboardMarkup([
        [B(f"Approve {tick(on)}", callback_data=f"ct:{cid}")],
        [B("⏱ Custom Delay", callback_data=f"cd:{cid}"), B("♻️ Reset Delay", callback_data=f"cr:{cid}")],
        [B("⏹ Stop Old Approve", callback_data=f"cs:{cid}") if running
         else B("🕰 Approve Old Requests", callback_data=f"co:{cid}")],
        [B("🗑 Remove", callback_data=f"cx:{cid}"), B("🔙 Back", callback_data="chats")]])
    return text, kb

async def delay_panel():
    g = await gcfg()
    kb = InlineKeyboardMarkup([
        [B("🚀 Fast 1-5s", callback_data="dp:1:5"), B("🙂 Normal 2-15s", callback_data="dp:2:15")],
        [B("🐢 Slow 10-40s", callback_data="dp:10:40"), B("✏️ Custom", callback_data="dc")],
        [B("🔙 Back", callback_data="main")]])
    return f"**⏱ Global Delay**\nAbhi: {g['min']}-{g['max']}s", kb


# ---------------- commands ----------------
@bot.on_message(filters.command(["start", "settings", "help"]) & filters.private)
async def start(_, m: Message):
    if m.from_user.id not in OWNERS:
        return await m.reply("Main ek auto join-request bot hoon.")
    pending.pop(m.from_user.id, None)
    await show(m, *await main_panel())

@bot.on_message(filters.command("approveold") & filters.private & is_owner)
async def cmd_old(_, m: Message):
    if not user:
        return await m.reply("SESSION_STRING set nahi hai.")
    cid = int(m.command[1])
    if cid in old_tasks:
        return await m.reply("Already running.")
    old_tasks[cid] = asyncio.create_task(approve_old(cid, m.chat.id))
    await m.reply("▶️ Started")

@bot.on_message(filters.command("stopold") & filters.private & is_owner)
async def cmd_stop(_, m: Message):
    t = old_tasks.get(int(m.command[1]))
    if t:
        t.cancel()
    await m.reply("🛑 Stopping..." if t else "Koi task nahi.")


# ---------------- callbacks ----------------
@bot.on_callback_query(is_owner)
async def cb(_, q: CallbackQuery):
    act, _, arg = q.data.partition(":")
    uid = q.from_user.id
    try:
        if act == "main":
            pending.pop(uid, None)
            return await show(q, *await main_panel())
        if act == "g":
            g = await gcfg()
            await gset(**{arg: not g[arg]})
            return await show(q, *await main_panel())
        if act == "chats":
            pending.pop(uid, None)
            return await show(q, *await chats_panel())
        if act == "p":
            return await show(q, *await chats_panel(int(arg)))
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
        if act == "co":
            if not user:
                return await q.answer("SESSION_STRING set nahi hai.", show_alert=True)
            cid = int(arg)
            if cid not in old_tasks:
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
            await q.answer("Removed")
            return await show(q, *await chats_panel())
        if act == "add":
            pending[uid] = ("add",)
            return await q.message.reply("Chat ID bhejo (`-100...`), @username, ya channel/group ka koi message forward karo.\n"
                                         "Bot us chat me admin hona chahiye.")
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
            return await q.message.reply(f"Abhi: `{g['welcome']}`\n\nNaya welcome text bhejo. "
                                         "`{name}` aur `{chat}` use kar sakte ho.")
    finally:
        try:
            await q.answer()
        except Exception:
            pass


# ---------------- text input (button ke baad) ----------------
@bot.on_message(filters.private & is_owner & ~filters.regex(r"^/"), group=1)
async def on_input(_, m: Message):
    state = pending.get(m.from_user.id)
    if not state:
        return
    kind = state[0]
    try:
        if kind == "add":
            fo = getattr(m, "forward_from_chat", None) or getattr(getattr(m, "forward_origin", None), "chat", None)
            ref = fo.id if fo else (m.text or "").strip()
            ref = int(ref) if str(ref).lstrip("-").isdigit() else ref
            chat = await bot.get_chat(ref)
            await db.chats.update_one({"_id": chat.id}, {"$set": {"title": chat.title, "enabled": True}}, upsert=True)
            pending.pop(m.from_user.id)
            await m.reply(f"✅ Added: {chat.title}")
            return await show(m, *await chat_panel(chat.id))
        if kind in ("delay", "cdelay"):
            lo, hi = sorted(float(x) for x in m.text.split()[:2])
            if kind == "delay":
                await gset(min=lo, max=hi)
                pending.pop(m.from_user.id)
                return await show(m, *await delay_panel())
            await db.chats.update_one({"_id": state[1]}, {"$set": {"min": lo, "max": hi}}, upsert=True)
            pending.pop(m.from_user.id)
            return await show(m, *await chat_panel(state[1]))
        if kind == "welcome":
            await gset(welcome=m.text, welcome_on=True)
            pending.pop(m.from_user.id)
            await m.reply("✅ Welcome text save + ON")
            return await show(m, *await main_panel())
    except Exception as e:
        await m.reply(f"❌ Galat input: {e}\nDobara bhejo ya /settings dabao.")


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
            await asyncio.sleep(300)
            try:
                async with s.get(SELF_URL, timeout=20) as r:
                    log.info("self-ping %s", r.status)
            except Exception as e:
                log.warning("ping fail: %s", e)

async def main():
    await web_app()
    await bot.start()
    if user:
        await user.start()
    for _ in range(WORKERS):
        asyncio.create_task(worker())
    asyncio.create_task(self_ping())
    log.info("Bot started")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
