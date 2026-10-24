import os, re, asyncio, random, logging, functools, time, contextvars, hashlib, base64, calendar
from collections import Counter
from aiohttp import web, ClientSession
from motor.motor_asyncio import AsyncIOMotorClient

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)  # Pyrogram/Motor isi loop par bandhenge

from pyrogram import Client, filters
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import (FloodWait, RPCError, MessageNotModified, SessionPasswordNeeded,
                             PhoneCodeInvalid, PhoneCodeExpired)
from pyrogram.handlers import (MessageHandler, CallbackQueryHandler, ChatJoinRequestHandler,
                               ChatMemberUpdatedHandler)
from pyrogram.types import (ChatJoinRequest, Message, CallbackQuery,
                            InlineKeyboardMarkup, InlineKeyboardButton as B)
try:
    from cryptography.fernet import Fernet
except Exception:  # cryptography na ho to tokens/sessions plain store honge (requirements.txt me hai)
    Fernet = None

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("joinbot")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
MONGO_URI = os.environ["MONGO_URI"]
MAIN_OWNERS = {int(x) for x in os.environ.get("OWNER_IDS", "").replace(" ", "").split(",") if x}
SESSION_STRING = os.environ.get("SESSION_STRING")          # optional (sirf main bot ke liye)
PORT = int(os.environ.get("PORT", 8080))
SELF_URL = os.environ.get("RENDER_EXTERNAL_URL")
WORKERS = int(os.environ.get("WORKERS", 8))
CLONE_WORKERS = int(os.environ.get("CLONE_WORKERS", 3))     # har clone ke workers
MAX_CLONES = int(os.environ.get("MAX_CLONES", 10))          # total clones (RAM bachane ke liye)
MAX_CHATS = int(os.environ.get("MAX_CHATS_PER_USER", 10))   # public mode me ek user ke max chats
SECRET_KEY = os.environ.get("SECRET_KEY") or (API_HASH + "|joinbot-v1")
REPORT_HOUR = int(os.environ.get("REPORT_HOUR", 21))     # daily report ka time (IST, 0-23)
PAGE = 8

MDB = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=8000)["joinbot"]


# ======================= multi-bot core (main + clones) =======================
CUR = contextvars.ContextVar("inst")     # abhi kaunsa bot (main/clone) handler chala raha hai

class Store:
    """Ek bot ka alag data (collections prefix se). Main = '' (purana data wahi), clone = 'c<id>_'."""
    NAMES = ("chats", "settings", "fails", "users")
    def __init__(self, prefix: str = ""):
        self.prefix = prefix
        for n in self.NAMES:
            setattr(self, n, MDB[prefix + n])
    async def drop(self):
        for n in self.NAMES:
            await MDB[self.prefix + n].drop()

class Inst:
    def __init__(self, key, client, owners, store, clone_owner=None, bot_id=0):
        self.key, self.client, self.owners, self.db = key, client, set(owners), store
        self.is_clone = clone_owner is not None
        self.clone_owner = clone_owner
        self.user = None            # helper (user) account
        self.helper = None          # {"id", "name", "username"}
        self.state = {"public": False}
        self.queue = asyncio.Queue()
        self.old_tasks = {}
        self.old_sem = asyncio.Semaphore(1)   # ek time par ek hi old-approve (flood se bachav)
        self.logins, self.pending, self.langs = {}, {}, {}
        self.bot_id = {"id": bot_id}
        self.workers = []
        self.username = ""
        self.owner_name = None      # clone owner ka naam (clone header ke liye)

class P:
    """Module-level naam (db, bot, OWNERS...) ko 'abhi wale bot' ke attribute par bhejta hai."""
    __slots__ = ("_a",)
    def __init__(self, a): object.__setattr__(self, "_a", a)
    def _t(self): return getattr(CUR.get(), object.__getattribute__(self, "_a"))
    def __getattr__(self, n): return getattr(self._t(), n)
    def __getitem__(self, k): return self._t()[k]
    def __setitem__(self, k, v): self._t()[k] = v
    def __delitem__(self, k): del self._t()[k]
    def __contains__(self, k): return k in self._t()
    def __iter__(self): return iter(self._t())
    def __len__(self): return len(self._t())
    def __bool__(self): return bool(self._t())
    async def __aenter__(self): return await self._t().__aenter__()   # async with old_sem:
    async def __aexit__(self, *a): return await self._t().__aexit__(*a)

db, bot, user, helper = P("db"), P("client"), P("user"), P("helper")
OWNERS, STATE, queue = P("owners"), P("state"), P("queue")
old_tasks, old_sem, logins, pending = P("old_tasks"), P("old_sem"), P("logins"), P("pending")
LANG, BOT_ID, IS_CLONE = P("langs"), P("bot_id"), P("is_clone")

INSTANCES: dict = {}       # client.name -> Inst
RUNNING: dict = {}         # clone bot_id -> Inst
CLONES = MDB["clones"]     # clones ka registry (sirf main store me)

MAIN = Inst("main", Client("bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True),
            MAIN_OWNERS, Store(""))
INSTANCES[MAIN.client.name] = MAIN

async def run_in(inst, fn, *a, **k):
    tok = CUR.set(inst)
    try:
        return await fn(*a, **k)
    finally:
        CUR.reset(tok)

# ---- handler registry: ek hi handlers sab bots (main + clones) par lagte hain
HANDLERS = []
def on_message(flt, group=0):
    def d(fn): HANDLERS.append((MessageHandler, fn, flt, group)); return fn
    return d
def on_callback(flt):
    def d(fn): HANDLERS.append((CallbackQueryHandler, fn, flt, 0)); return fn
    return d
def on_join_request():
    def d(fn): HANDLERS.append((ChatJoinRequestHandler, fn, None, 0)); return fn
    return d
def on_member_update():
    def d(fn): HANDLERS.append((ChatMemberUpdatedHandler, fn, None, 0)); return fn
    return d

def bind(fn, inst):
    @functools.wraps(fn)
    async def w(client, *a, **k):
        tok = CUR.set(inst)
        try:
            return await fn(client, *a, **k)
        finally:
            CUR.reset(tok)
    return w

def attach(inst):
    for H, fn, flt, group in HANDLERS:
        inst.client.add_handler(H(bind(fn, inst), flt) if flt is not None else H(bind(fn, inst)), group)

# ---- tokens / sessions ko database me encrypt karke rakhna
_F = Fernet(base64.urlsafe_b64encode(hashlib.sha256(SECRET_KEY.encode()).digest())) if Fernet else None
def enc(s: str) -> str:
    return "enc:" + _F.encrypt(s.encode()).decode() if _F else s
def dec(s):
    if s and s.startswith("enc:"):
        try:
            return _F.decrypt(s[4:].encode()).decode()
        except Exception:
            return None
    return s


DEFAULTS = {"active": True, "auto_all": True, "min": 2, "max": 15, "welcome_on": False, "public": False,
            "clone_on": True, "welcome": "Hello {name}! {chat} me aapki request approve ho gayi hai."}
tick = lambda b: "✅" if b else "❌"
clean = lambda t: re.sub(r"[\[\]*_`~|]", "", str(t))
TOKEN_RE = re.compile(r"\d{6,12}:[A-Za-z0-9_-]{30,50}")


# ======================= TEXTS (Hindi / English) =======================
TX = {
"hi": {
 "back": "🔙 वापस", "refresh": "🔄 रीफ्रेश", "my_chats": "📺 मेरे चैट", "add_chat": "➕ चैट जोड़ें",
 "old_help_btn": "ℹ️ पुरानी रिक्वेस्ट मदद", "lang_btn": "🌐 भाषा: हिंदी", "myclone_btn": "🤖 क्लोन बॉट",
 "private_bot": "🔒 यह बॉट अभी प्राइवेट है।\nआपकी ID: `{uid}`",
 "user_main": "**🤖 ऑटो जॉइन रिक्वेस्ट बॉट**\n\nअपने चैनल/ग्रुप की जॉइन रिक्वेस्ट अपने-आप approve कराओ।\n\n"
              "1️⃣ बॉट को अपने चैट में **admin** बनाओ (\"Invite Users\" permission के साथ)\n"
              "2️⃣ चैट अपने-आप जुड़ जाएगा, या ➕ चैट जोड़ें दबाओ\n3️⃣ 📺 मेरे चैट में सेटिंग्स बदलो\n\n"
              "आपके चैट: {n} | Approved: {tot}",
 "admin_main": "**⚙️ ऑटो जॉइन रिक्वेस्ट बॉट (Admin){tag}**\n\nमोड: {mode}\nडिले: {lo}-{hi}s | कतार: {q}\n"
               "कुल approved: {tot}\nHelper session: {hs}\n\n{hint}",
 "tag_clone": " — क्लोन", "mode_pub": "🌐 Public", "mode_priv": "🔒 Private",
 "hint_pub": "Public मोड में Auto-all OFF रखना बेहतर है।",
 "hint_nohelper": "💡 पुरानी pending रिक्वेस्ट के लिए 🔑 Old-Req Session से **अपना खुद का** helper अकाउंट लॉगिन करें।",
 "b_active": "बॉट चालू {s}", "b_autoall": "Auto-all चैट {s}", "b_wg": "वेलकम DM (ग्लोबल) {s}",
 "b_chats": "📺 चैट", "b_gdelay": "⏱ डिले", "b_gwtext": "👋 वेलकम टेक्स्ट", "b_sess": "🔑 Old-Req Session",
 "b_mode": "बॉट मोड: {m} (बदलो)", "b_users": "👥 Users", "b_clones_admin": "🤖 क्लोन (Admin)",
 "mode_set_pub": "🌐 बॉट अब **Public** है: कोई भी अपने चैट जोड़ सकता है।",
 "mode_set_priv": "🔒 बॉट अब **Private** है: सिर्फ admin पैनल इस्तेमाल कर सकता है।\n(पहले से जुड़े चैट में approve चलता रहेगा।)",
 "chats_hint": "**📺 चैट ({n})**\nसेटिंग्स खोलने के लिए चैट पर टैप करो।",
 "add_prompt": "चैट की ID (`-100...`), @username भेजो, या चैनल/ग्रुप का कोई मैसेज फॉरवर्ड करो।\n"
               "बॉट उस चैट में admin होना चाहिए और आप भी उस चैट के admin।",
 "chat_text": "**{title}**\n`{cid}`\n\nApprove: {on}\nडिले: {lo}-{hi}s {dt}\nवेलकम DM: {w}\n"
              "Approved: {ap} | Failed records: {nf}\nपुरानी task: {old}",
 "custom": "(कस्टम)", "global": "(ग्लोबल)", "running": "चल रही ▶️", "idle": "खाली",
 "b_approve": "Approve {s}", "b_cdelay": "⏱ कस्टम डिले", "b_reset": "♻️ रीसेट", "b_wdm": "वेलकम DM {s}",
 "b_wtext": "✏️ वेलकम टेक्स्ट", "b_old": "🕰 पुरानी रिक्वेस्ट approve", "b_oldstop": "⏹ पुरानी approve रोको",
 "b_fail": "❌ फेल रिपोर्ट", "b_help": "ℹ️ Helper जानकारी", "b_rm": "🗑 हटाओ",
 "f_title": "**❌ फेल रिपोर्ट — {title}**", "f_none": "✅ कोई failed रिक्वेस्ट रिकॉर्ड नहीं है।",
 "f_total": "कुल: {n}", "f_recent": "**हाल के:**", "f_check": "🔍 एक यूज़र चेक करो", "f_clear": "🧹 रिपोर्ट साफ़",
 "f_ask": "जिस यूज़र का कारण देखना है उसकी **User ID** भेजो (नंबर)।",
 "f_nf": "इस यूज़र का failed रिकॉर्ड नहीं मिला। या तो रिक्वेस्ट approve हो चुकी है, या अभी तक try ही नहीं हुई।",
 "f_one": "{e} {who} `{uid}`\n\n**कारण:** {d}\nकोड: `{code}`\nकब: {when} ({kind} रिक्वेस्ट)",
 "kind_old": "पुरानी", "kind_new": "नई",
 "helper_none": "ℹ️ पुरानी pending रिक्वेस्ट वाली सुविधा अभी उपलब्ध नहीं है (admin ने helper अकाउंट सेट नहीं किया)।",
 "helper_note": "🕰 पुरानी pending रिक्वेस्ट approve करने के लिए इस **helper अकाउंट** को अपने चैट में **admin** बनाओ "
                "(\"Invite Users\" permission के साथ):\n\n👤 {who}{uname}\nID: `{hid}`",
 "helper_tail": "\n\nadmin बनाने के बाद: 📺 मेरे चैट → अपना चैट → 🕰 पुरानी रिक्वेस्ट approve।",
 "old_running": "इस चैट का पुरानी-रिक्वेस्ट task पहले से चल रहा है।",
 "h_notmember": "❌ Helper अकाउंट अभी इस चैट में नहीं है।",
 "h_notadmin": "❌ Helper अकाउंट इस चैट में है पर admin नहीं है।",
 "h_noperm": "❌ Helper admin है पर \"Invite Users\" permission नहीं है।",
 "h_retry": "\n\nadmin बनाने के बाद दोबारा कोशिश करो।",
 "old_queue": "⏳ पहले का काम चल रहा है, आपका नंबर कतार में है...",
 "old_prog": "⏳ `{cid}` → {n} approve हो चुके...",
 "old_done": "✅ पुरानी रिक्वेस्ट पूरी `{cid}`\nApproved: {ok} | Failed: {bad}",
 "old_detail": "\n\nडिटेल: चैट → ❌ फेल रिपोर्ट",
 "old_stopped": "🛑 रोक दिया `{cid}`। Approved: {ok}",
 "old_err": "❌ Error `{cid}`: {e}",
 "old_started": "▶️ शुरू हो गया", "old_nothing": "कोई task नहीं है।", "old_stopping": "🛑 रोक रहे हैं...",
 "e_notfound": "चैट नहीं मिला। पहले बॉट को उस चैट में admin बनाओ, फिर ID/@username या फॉरवर्ड किया मैसेज भेजो।",
 "e_notadmin_user": "आप इस चैट के admin नहीं लग रहे।",
 "e_onlyadmin": "सिर्फ चैट के admin/owner ही चैट जोड़ सकते हैं।",
 "e_bot_not_in": "बॉट इस चैट में नहीं है। पहले बॉट को चैट में जोड़कर admin बनाओ।",
 "e_bot_not_admin": "बॉट इस चैट में admin नहीं है। पहले बॉट को admin बनाओ।",
 "e_bot_noinvite": "बॉट को \"Invite Users\" (Add Members) permission दो।",
 "e_delay": "डिले 1 से 300 सेकंड के बीच रखो।",
 "e_tpl": "टेक्स्ट में सिर्फ {{name}} और {{chat}} इस्तेमाल करो (दूसरे {{ }} नहीं)।",
 "max_chats": "एक यूज़र अधिकतम {lim} चैट जोड़ सकता है।",
 "added": "✅ जुड़ गया: {title}",
 "member_added": "✅ **{title}** जुड़ गया!{warn}\n\n{note}\n\nसेटिंग्स के लिए /settings → 📺 मेरे चैट",
 "warn_noinvite": "\n\n⚠️ बॉट को \"Invite Users\" permission नहीं मिली, इसके बिना रिक्वेस्ट approve नहीं होंगी।",
 "cd_prompt": "इस चैट का डिले भेजो: `min max` (जैसे `3 12`)",
 "ce_prompt": "इस चैट के लिए नया वेलकम DM टेक्स्ट भेजो। {{name}} और {{chat}} इस्तेमाल कर सकते हो।",
 "wt_prompt": "अभी: `{cur}`\n\nनया ग्लोबल वेलकम टेक्स्ट भेजो। {{name}} और {{chat}} इस्तेमाल कर सकते हो।",
 "gd_prompt": "ग्लोबल डिले भेजो: `min max` (जैसे `2 15`)",
 "saved_w": "✅ इस चैट का वेलकम DM सेव + ON", "saved_gw": "✅ ग्लोबल वेलकम टेक्स्ट सेव + ON",
 "q_no_access": "आपको इस चैट की access नहीं है।", "q_owner_only": "यह option सिर्फ admin के लिए है।",
 "q_main_only": "यह फीचर सिर्फ मुख्य बॉट में है।",
 "q_removed": "हटा दिया", "q_clear": "रिपोर्ट साफ़", "q_stopping": "रोक रहे हैं...", "q_started": "पुरानी approve शुरू ▶️",
 "in_err": "❌ गलत इनपुट", "in_err_full": "❌ {d}\nदोबारा भेजो या /settings दबाओ।",
 "ses_in": "**🔑 Old-Request Session**\n\nलॉगिन: {who} (`{hid}`) ✅\n\nयह अकाउंट हर चैट में **admin** होना चाहिए (Invite Users)। "
           "यूज़र्स को बॉट ये जानकारी खुद दिखाता है।",
 "ses_out": "**🔑 Old-Request Session**\n\nअभी कोई session नहीं है ❌\nपुरानी pending रिक्वेस्ट के लिए एक यूज़र अकाउंट लॉगिन करो "
            "(बेहतर: अलग/secondary अकाउंट)।",
 "ses_clone_note": "\n\n🤖 **क्लोन बॉट के लिए नियम:** यह आपके क्लोन का **अपना** helper अकाउंट है, मुख्य बॉट का नहीं। "
                   "कृपया अपने ही Telegram अकाउंट से (बेहतर: एक अलग अकाउंट से) खुद लॉगिन करें, फिर उसे अपने चैट में admin बनाएँ। "
                   "किसी और का अकाउंट या session उसकी अनुमति के बिना इस्तेमाल न करें।",
 "b_login": "📲 लॉगिन (फोन + OTP)", "b_paste": "📋 Session String पेस्ट करो", "b_logout": "🚪 लॉगआउट / हटाओ",
 "ses_phone": "📲 Helper अकाउंट का फोन नंबर देश-कोड के साथ भेजो, जैसे `+919876543210`",
 "ses_str": "📋 Pyrogram SESSION_STRING पेस्ट करो (मैसेज तुरंत डिलीट कर दिया जाएगा)।",
 "ses_otp": "📨 Telegram ने उस अकाउंट पर OTP भेजा है।\n\nकोड **स्पेस के साथ** भेजो, जैसे `1 2 3 4 5` "
            "(सीधे भेजोगे तो Telegram कोड ब्लॉक कर देता है)।",
 "ses_2fa": "🔐 2-step verification password भेजो (मैसेज डिलीट हो जाएगा)।",
 "ses_ok": "✅ लॉगिन हो गया! Session सेव हो गया है।", "ses_set": "✅ Session सेट हो गया।", "ses_bye": "लॉगआउट हो गया",
 "ses_fail": "❌ {err}\nफिर से लॉगिन बटन दबाओ।",
 "u_head": "**👥 Users**\nकुल: {total} | नए (24h): {new24} | सक्रिय (24h): {act24}\nपेज {p}/{pages}\n\n",
 "u_entry": "**{i}.** {crown}{who}\n   {uname} | ID: `{uid}`\n   Start: {st}\n   Last: {last}\n   चैट: {nc} | Approved: {ap}",
 "u_none": "अभी कोई यूज़र रिकॉर्ड नहीं है।", "no_username": "no username",
 "nav_back": "⬅️ पिछला", "nav_next": "अगला ➡️", "b_menu": "🔙 मेन्यू",
 "dl_title": "**⏱ ग्लोबल डिले**\nअभी: {lo}-{hi}s", "b_custom": "✏️ कस्टम",
 "ping_full": "🏓 pong\n{mongo}\nHelper session: {h}\nमोड: {mode}\nक्लोन: {nc}",
 "r_USER_CHANNELS_TOO_MUCH": "यूज़र Telegram की 500 चैनल/ग्रुप वाली सीमा पर है, इसलिए और कहीं जॉइन नहीं हो सकता। (कोई उपाय नहीं)",
 "r_USER_DEACTIVATED": "अकाउंट डिलीट/डिएक्टिवेट हो चुका है।",
 "r_USER_DEACTIVATED_BAN": "अकाउंट Telegram ने बैन कर दिया है।",
 "r_HIDE_REQUESTER_MISSING": "रिक्वेस्ट अब मौजूद नहीं: यूज़र ने वापस ले ली या किसी और admin ने पहले ही हैंडल कर दी।",
 "r_USER_ALREADY_PARTICIPANT": "यूज़र पहले से मेंबर है।",
 "r_USER_BANNED_IN_CHANNEL": "यूज़र इस चैट में banned है। पहले unban करो।",
 "r_USER_PRIVACY_RESTRICTED": "यूज़र की प्राइवेसी सेटिंग्स की वजह से add नहीं हो सकता।",
 "r_USER_ID_INVALID": "यूज़र नहीं मिला (डिलीट/invalid)।",
 "r_CHAT_ADMIN_REQUIRED": "Admin permission कम है। \"Invite Users\" permission दो।",
 "r_unknown": "अनजान कारण: {rid}",
 # ---- clone
 "k_rules": "**🤖 क्लोन बॉट — अपना खुद का बॉट**\n\nयहाँ से आप इसी बॉट जैसा **अपना बॉट** बना सकते हो, जिसके आप खुद admin होगे। नियम:\n\n"
            "1️⃣ @BotFather → /newbot से नया बॉट बनाओ और उसका **टोकन** यहाँ भेजो।\n"
            "2️⃣ आपके क्लोन का डेटा (चैट, सेटिंग्स, यूज़र) मुख्य बॉट से **बिल्कुल अलग** रहता है।\n"
            "3️⃣ क्लोन में कमांड और पैनल वही हैं जो इस बॉट में admin के पास हैं।\n"
            "4️⃣ पुरानी pending रिक्वेस्ट के लिए क्लोन के अंदर **अपना खुद का** helper अकाउंट लॉगिन करना होगा "
            "(⚙️ Settings → 🔑 Old-Req Session) और उसे अपने चैट में admin बनाना होगा।\n"
            "5️⃣ एक अकाउंट से **सिर्फ एक** क्लोन बन सकता है, और आप जब चाहें उसे डिलीट कर सकते हो।\n"
            "6️⃣ टोकन किसी और को न दें। आपका टोकन और session एन्क्रिप्ट करके सेव होते हैं।",
 "k_have": "**🤖 आपका क्लोन बॉट**\n\nबॉट: @{u}\nस्टेटस: {st}\nबना: {when}\n\nआप इसके admin हो। कमांड/पैनल वही हैं जो इस बॉट में admin के पास हैं। "
           "पुरानी रिक्वेस्ट के लिए क्लोन के अंदर अपना खुद का helper अकाउंट लॉगिन करें।",
 "k_st_run": "चालू ✅", "k_st_stop": "बंद ⏸",
 "k_open": "🔗 @{u} खोलो", "k_new": "➕ क्लोन बनाओ", "k_restart": "🔄 रीस्टार्ट", "k_del": "🗑 क्लोन डिलीट",
 "k_yes": "✅ हाँ, डिलीट करो", "k_no": "❌ नहीं",
 "k_ask_token": "अपने नए बॉट का **टोकन** भेजो (@BotFather → /newbot से मिलता है)।\n\n⚠️ टोकन किसी और को न दें। यह मैसेज मैं तुरंत डिलीट कर दूँगा।",
 "k_badtoken": "टोकन का फॉर्मेट सही नहीं है। @BotFather वाला पूरा टोकन भेजो (जैसे `123456:ABC...`)।",
 "k_same": "यह इसी (मुख्य) बॉट का टोकन है। अपना नया बॉट बनाकर उसका टोकन दो।",
 "k_dup": "इस टोकन से पहले ही क्लोन बना हुआ है।",
 "k_one": "आपका एक क्लोन पहले से है। नया बनाने के लिए पहले उसे डिलीट करो।",
 "k_full": "अभी क्लोन की सीमा पूरी हो चुकी है। कृपया बाद में कोशिश करें।",
 "k_off": "क्लोन फीचर अभी बंद है।",
 "k_invalid": "इस टोकन से बॉट शुरू नहीं हो सका। जाँच लो कि टोकन सही और चालू है, फिर दोबारा भेजो।",
 "k_wait": "⏳ क्लोन शुरू कर रहा हूँ...",
 "k_done": "✅ आपका क्लोन बॉट तैयार है: @{u}\n\n1️⃣ @{u} खोलकर /start दबाओ — वहाँ आप **admin** हो।\n"
           "2️⃣ वहाँ के कमांड/पैनल वैसे ही हैं जैसे इस बॉट में admin के पास हैं।\n"
           "3️⃣ पुरानी pending रिक्वेस्ट के लिए क्लोन में ⚙️ Settings → 🔑 Old-Req Session से **अपना खुद का** helper अकाउंट लॉगिन करें "
           "(बेहतर: अलग अकाउंट), और उसे अपने चैट में admin बनाएँ।\n"
           "4️⃣ एक अकाउंट से एक ही क्लोन बन सकता है; जब चाहें 🗑 से डिलीट कर सकते हैं।",
 "k_deleted": "🗑 क्लोन डिलीट हो गया और उसका सारा डेटा हटा दिया गया। चाहो तो @BotFather में बॉट डिलीट/टोकन revoke भी कर दो।",
 "k_del_ask": "⚠️ क्या आप सच में अपना क्लोन **@{u}** डिलीट करना चाहते हो? उसके चैट/सेटिंग्स/यूज़र का सारा डेटा हमेशा के लिए हट जाएगा।",
 "k_restarted": "🔄 क्लोन रीस्टार्ट हो गया।",
 "k_admin_removed": "⚠️ आपका क्लोन बॉट admin द्वारा हटा दिया गया है।",
 "k_failed_boot": "⚠️ आपका क्लोन **@{u}** शुरू नहीं हो पाया (टोकन बदला/revoke तो नहीं हुआ?)। 🤖 क्लोन बॉट से दोबारा बनाओ।",
 "ka_head": "**🤖 क्लोन बॉट ({n}/{max})**\nफीचर: {on}\n\n{items}",
 "ka_item": "• @{u} — owner `{o}` {st}", "ka_none": "अभी कोई क्लोन नहीं है।",
 "ka_toggle": "क्लोन फीचर {s}", "kv_text": "**🤖 @{u}**\nID: `{bid}`\nOwner: {owner}\nस्टेटस: {st}\nबना: {when}\nचैट: {nc} | यूज़र: {nu}",
 "kv_stop": "⏸ बंद करो", "kv_start": "▶️ चालू करो", "kz_ask": "⚠️ क्लोन **@{u}** डिलीट करें? उसका सारा डेटा हट जाएगा।",
},
"en": {
 "back": "🔙 Back", "refresh": "🔄 Refresh", "my_chats": "📺 My Chats", "add_chat": "➕ Add Chat",
 "old_help_btn": "ℹ️ Old Requests Help", "lang_btn": "🌐 Language: English", "myclone_btn": "🤖 Clone Bot",
 "private_bot": "🔒 This bot is private right now.\nYour ID: `{uid}`",
 "user_main": "**🤖 Auto Join Request Bot**\n\nGet your channel/group join requests approved automatically.\n\n"
              "1️⃣ Make the bot an **admin** in your chat (with \"Invite Users\" permission)\n"
              "2️⃣ The chat is added automatically, or tap ➕ Add Chat\n3️⃣ Change settings in 📺 My Chats\n\n"
              "Your chats: {n} | Approved: {tot}",
 "admin_main": "**⚙️ Auto Join Request Bot (Admin){tag}**\n\nMode: {mode}\nDelay: {lo}-{hi}s | Queue: {q}\n"
               "Total approved: {tot}\nHelper session: {hs}\n\n{hint}",
 "tag_clone": " — Clone", "mode_pub": "🌐 Public", "mode_priv": "🔒 Private",
 "hint_pub": "In Public mode it's better to keep Auto-all OFF.",
 "hint_nohelper": "💡 For old pending requests, log in **your own** helper account via 🔑 Old-Req Session.",
 "b_active": "Bot Active {s}", "b_autoall": "Auto-all chats {s}", "b_wg": "Welcome DM (global) {s}",
 "b_chats": "📺 Chats", "b_gdelay": "⏱ Delay", "b_gwtext": "👋 Welcome Text", "b_sess": "🔑 Old-Req Session",
 "b_mode": "Bot Mode: {m} (change)", "b_users": "👥 Users", "b_clones_admin": "🤖 Clones (Admin)",
 "mode_set_pub": "🌐 The bot is now **Public**: anyone can add their chats.",
 "mode_set_priv": "🔒 The bot is now **Private**: only the admin can use the panel.\n(Approvals keep running in chats already added.)",
 "chats_hint": "**📺 Chats ({n})**\nTap a chat to open its settings.",
 "add_prompt": "Send the chat ID (`-100...`), @username, or forward any message from the channel/group.\n"
               "The bot must be an admin there, and you must be an admin of that chat too.",
 "chat_text": "**{title}**\n`{cid}`\n\nApprove: {on}\nDelay: {lo}-{hi}s {dt}\nWelcome DM: {w}\n"
              "Approved: {ap} | Failed records: {nf}\nOld task: {old}",
 "custom": "(custom)", "global": "(global)", "running": "running ▶️", "idle": "idle",
 "b_approve": "Approve {s}", "b_cdelay": "⏱ Custom Delay", "b_reset": "♻️ Reset", "b_wdm": "Welcome DM {s}",
 "b_wtext": "✏️ Welcome Text", "b_old": "🕰 Approve old requests", "b_oldstop": "⏹ Stop old approve",
 "b_fail": "❌ Failed Report", "b_help": "ℹ️ Helper info", "b_rm": "🗑 Remove",
 "f_title": "**❌ Failed Report — {title}**", "f_none": "✅ No failed request records.",
 "f_total": "Total: {n}", "f_recent": "**Recent:**", "f_check": "🔍 Check one user", "f_clear": "🧹 Clear report",
 "f_ask": "Send the **User ID** (number) you want to check.",
 "f_nf": "No failed record for this user. Either the request was approved, or it hasn't been tried yet.",
 "f_one": "{e} {who} `{uid}`\n\n**Reason:** {d}\nCode: `{code}`\nWhen: {when} ({kind} request)",
 "kind_old": "old", "kind_new": "new",
 "helper_none": "ℹ️ The old pending requests feature isn't available right now (the admin hasn't set a helper account).",
 "helper_note": "🕰 To approve old pending requests, make this **helper account** an **admin** in your chat "
                "(with \"Invite Users\" permission):\n\n👤 {who}{uname}\nID: `{hid}`",
 "helper_tail": "\n\nAfter making it admin: 📺 My Chats → your chat → 🕰 Approve old requests.",
 "old_running": "The old-requests task for this chat is already running.",
 "h_notmember": "❌ The helper account is not in this chat yet.",
 "h_notadmin": "❌ The helper account is in this chat but is not an admin.",
 "h_noperm": "❌ The helper is an admin but lacks the \"Invite Users\" permission.",
 "h_retry": "\n\nTry again after making it admin.",
 "old_queue": "⏳ Another job is running, you're in the queue...",
 "old_prog": "⏳ `{cid}` → {n} approved so far...",
 "old_done": "✅ Old requests done `{cid}`\nApproved: {ok} | Failed: {bad}",
 "old_detail": "\n\nDetails: chat → ❌ Failed Report",
 "old_stopped": "🛑 Stopped `{cid}`. Approved: {ok}",
 "old_err": "❌ Error `{cid}`: {e}",
 "old_started": "▶️ Started", "old_nothing": "No task is running.", "old_stopping": "🛑 Stopping...",
 "e_notfound": "Chat not found. First make the bot an admin there, then send the ID/@username or a forwarded message.",
 "e_notadmin_user": "You don't seem to be an admin of this chat.",
 "e_onlyadmin": "Only the chat's admins/owner can add it.",
 "e_bot_not_in": "The bot isn't in this chat. Add the bot and make it admin first.",
 "e_bot_not_admin": "The bot isn't an admin in this chat. Make it admin first.",
 "e_bot_noinvite": "Give the bot the \"Invite Users\" (Add Members) permission.",
 "e_delay": "Keep the delay between 1 and 300 seconds.",
 "e_tpl": "Use only {{name}} and {{chat}} in the text (no other {{ }}).",
 "max_chats": "One user can add at most {lim} chats.",
 "added": "✅ Added: {title}",
 "member_added": "✅ **{title}** added!{warn}\n\n{note}\n\nFor settings: /settings → 📺 My Chats",
 "warn_noinvite": "\n\n⚠️ The bot lacks \"Invite Users\" permission, so requests won't be approved.",
 "cd_prompt": "Send this chat's delay: `min max` (e.g. `3 12`)",
 "ce_prompt": "Send the new welcome DM text for this chat. You can use {{name}} and {{chat}}.",
 "wt_prompt": "Current: `{cur}`\n\nSend the new global welcome text. You can use {{name}} and {{chat}}.",
 "gd_prompt": "Send the global delay: `min max` (e.g. `2 15`)",
 "saved_w": "✅ This chat's welcome DM saved + ON", "saved_gw": "✅ Global welcome text saved + ON",
 "q_no_access": "You don't have access to this chat.", "q_owner_only": "This option is admin-only.",
 "q_main_only": "This feature is only in the main bot.",
 "q_removed": "Removed", "q_clear": "Report cleared", "q_stopping": "Stopping...", "q_started": "Old approve started ▶️",
 "in_err": "❌ Invalid input", "in_err_full": "❌ {d}\nSend again or press /settings.",
 "ses_in": "**🔑 Old-Request Session**\n\nLogged in: {who} (`{hid}`) ✅\n\nThis account must be an **admin** in every chat (Invite Users). "
           "The bot shows these details to users itself.",
 "ses_out": "**🔑 Old-Request Session**\n\nNo session yet ❌\nFor old pending requests, log in a user account "
            "(better: a separate/secondary account).",
 "ses_clone_note": "\n\n🤖 **Rules for clone bots:** this is your clone's **own** helper account, not the main bot's. "
                   "Please log in with your own Telegram account (preferably a separate one) yourself, then make it an admin in your chats. "
                   "Do not use anyone else's account or session without their permission.",
 "b_login": "📲 Login (Phone + OTP)", "b_paste": "📋 Paste Session String", "b_logout": "🚪 Logout / Remove",
 "ses_phone": "📲 Send the helper account's phone number with country code, e.g. `+919876543210`",
 "ses_str": "📋 Paste the Pyrogram SESSION_STRING (the message will be deleted right away).",
 "ses_otp": "📨 Telegram sent an OTP to that account.\n\nSend the code **with spaces**, like `1 2 3 4 5` "
            "(if sent directly, Telegram blocks the code).",
 "ses_2fa": "🔐 Send your 2-step verification password (the message will be deleted).",
 "ses_ok": "✅ Logged in! The session is saved.", "ses_set": "✅ Session set.", "ses_bye": "Logged out",
 "ses_fail": "❌ {err}\nPress the Login button again.",
 "u_head": "**👥 Users**\nTotal: {total} | New (24h): {new24} | Active (24h): {act24}\nPage {p}/{pages}\n\n",
 "u_entry": "**{i}.** {crown}{who}\n   {uname} | ID: `{uid}`\n   Start: {st}\n   Last: {last}\n   Chats: {nc} | Approved: {ap}",
 "u_none": "No user records yet.", "no_username": "no username",
 "nav_back": "⬅️ Back", "nav_next": "Next ➡️", "b_menu": "🔙 Menu",
 "dl_title": "**⏱ Global Delay**\nCurrent: {lo}-{hi}s", "b_custom": "✏️ Custom",
 "ping_full": "🏓 pong\n{mongo}\nHelper session: {h}\nMode: {mode}\nClones: {nc}",
 "r_USER_CHANNELS_TOO_MUCH": "The user is at Telegram's 500 channels/groups limit, so they can't join any more. (Nothing can be done)",
 "r_USER_DEACTIVATED": "Account is deleted/deactivated.",
 "r_USER_DEACTIVATED_BAN": "Account was banned by Telegram.",
 "r_HIDE_REQUESTER_MISSING": "The request no longer exists: the user withdrew it or another admin already handled it.",
 "r_USER_ALREADY_PARTICIPANT": "User is already a member.",
 "r_USER_BANNED_IN_CHANNEL": "User is banned in this chat. Unban first.",
 "r_USER_PRIVACY_RESTRICTED": "Can't be added because of the user's privacy settings.",
 "r_USER_ID_INVALID": "User not found (deleted/invalid).",
 "r_CHAT_ADMIN_REQUIRED": "Missing admin permission. Give \"Invite Users\" permission.",
 "r_unknown": "Unknown reason: {rid}",
 # ---- clone
 "k_rules": "**🤖 Clone Bot — your own bot**\n\nFrom here you can create **your own bot** just like this one, and you will be its admin. Rules:\n\n"
            "1️⃣ Create a new bot with @BotFather → /newbot and send its **token** here.\n"
            "2️⃣ Your clone's data (chats, settings, users) stays **completely separate** from the main bot.\n"
            "3️⃣ The clone has the same commands and panels that the admin has in this bot.\n"
            "4️⃣ For old pending requests you must log in **your own** helper account inside the clone "
            "(⚙️ Settings → 🔑 Old-Req Session) and make it an admin in your chats.\n"
            "5️⃣ One account can have **only one** clone, and you can delete it any time.\n"
            "6️⃣ Never share your token. Your token and session are stored encrypted.",
 "k_have": "**🤖 Your Clone Bot**\n\nBot: @{u}\nStatus: {st}\nCreated: {when}\n\nYou are its admin. The commands/panels are the same as the admin has in this bot. "
           "For old requests, please log in your own helper account inside the clone.",
 "k_st_run": "running ✅", "k_st_stop": "stopped ⏸",
 "k_open": "🔗 Open @{u}", "k_new": "➕ Create clone", "k_restart": "🔄 Restart", "k_del": "🗑 Delete clone",
 "k_yes": "✅ Yes, delete", "k_no": "❌ No",
 "k_ask_token": "Send your new bot's **token** (you get it from @BotFather → /newbot).\n\n⚠️ Never share the token. I will delete this message right away.",
 "k_badtoken": "The token format is wrong. Send the full token from @BotFather (like `123456:ABC...`).",
 "k_same": "That is this (main) bot's own token. Create your own new bot and send its token.",
 "k_dup": "A clone already exists for this token.",
 "k_one": "You already have a clone. Delete it first to create a new one.",
 "k_full": "The clone limit is currently full. Please try again later.",
 "k_off": "The clone feature is currently off.",
 "k_invalid": "The bot couldn't start with this token. Check that the token is correct and active, then send it again.",
 "k_wait": "⏳ Starting your clone...",
 "k_done": "✅ Your clone bot is ready: @{u}\n\n1️⃣ Open @{u} and press /start — you are the **admin** there.\n"
           "2️⃣ Its commands/panels are the same as the admin has in this bot.\n"
           "3️⃣ For old pending requests, in the clone go to ⚙️ Settings → 🔑 Old-Req Session and log in **your own** helper account "
           "(preferably a separate one), then make it an admin in your chats.\n"
           "4️⃣ One account can have only one clone; you can delete it any time with 🗑.",
 "k_deleted": "🗑 The clone is deleted and all its data removed. You may also delete the bot / revoke its token in @BotFather.",
 "k_del_ask": "⚠️ Do you really want to delete your clone **@{u}**? All its chats/settings/users data will be removed permanently.",
 "k_restarted": "🔄 Clone restarted.",
 "k_admin_removed": "⚠️ Your clone bot was removed by the admin.",
 "k_failed_boot": "⚠️ Your clone **@{u}** couldn't start (was the token changed/revoked?). Create it again from 🤖 Clone Bot.",
 "ka_head": "**🤖 Clone Bots ({n}/{max})**\nFeature: {on}\n\n{items}",
 "ka_item": "• @{u} — owner `{o}` {st}", "ka_none": "No clones yet.",
 "ka_toggle": "Clone feature {s}", "kv_text": "**🤖 @{u}**\nID: `{bid}`\nOwner: {owner}\nStatus: {st}\nCreated: {when}\nChats: {nc} | Users: {nu}",
 "kv_stop": "⏸ Stop", "kv_start": "▶️ Start", "kz_ask": "⚠️ Delete clone **@{u}**? All its data will be removed.",
},
}

TX["hi"].update({
 "clone_head": "🤖 यह बॉट **@{main}** का **क्लोन** है।\nमालिक: {owner}\n\n",
 "chat_text": "**{title}**\n`{cid}`\n\nApprove: {on}\nडिले: {lo}-{hi}s {dt}\nवेलकम DM: {w}\n"
              "Approved: {ap} (आज: {td}) | Failed records: {nf}\nपुरानी task: {old}",
 "b_report": "🔔 डेली रिपोर्ट {s}", "q_rep_on": "डेली रिपोर्ट चालू", "q_rep_off": "डेली रिपोर्ट बंद",
 "rep_title": "📊 **आज की रिपोर्ट** ({day})\n\n✅ Approved: {ok}\n❌ Failed: {bad}",
 "rep_top": "\n\n**टॉप चैट:**\n{lines}", "rep_chat": "• {title}: ✅ {ok} | ❌ {bad}",
 "rep_foot": "\n\n🔕 बंद करने के लिए: /settings → 🔔 डेली रिपोर्ट",
})
TX["en"].update({
 "clone_head": "🤖 This bot is a **clone** of **@{main}**.\nOwner: {owner}\n\n",
 "chat_text": "**{title}**\n`{cid}`\n\nApprove: {on}\nDelay: {lo}-{hi}s {dt}\nWelcome DM: {w}\n"
              "Approved: {ap} (today: {td}) | Failed records: {nf}\nOld task: {old}",
 "b_report": "🔔 Daily report {s}", "q_rep_on": "Daily report ON", "q_rep_off": "Daily report OFF",
 "rep_title": "📊 **Today's report** ({day})\n\n✅ Approved: {ok}\n❌ Failed: {bad}",
 "rep_top": "\n\n**Top chats:**\n{lines}", "rep_chat": "• {title}: ✅ {ok} | ❌ {bad}",
 "rep_foot": "\n\n🔕 To turn off: /settings → 🔔 Daily report",
})
REASON_EMOJI = {"USER_CHANNELS_TOO_MUCH": "🚫", "USER_DEACTIVATED": "👻", "USER_DEACTIVATED_BAN": "⛔",
                "HIDE_REQUESTER_MISSING": "↩️", "USER_ALREADY_PARTICIPANT": "✔️", "USER_BANNED_IN_CHANNEL": "🔨",
                "USER_PRIVACY_RESTRICTED": "🔒", "USER_ID_INVALID": "❓", "CHAT_ADMIN_REQUIRED": "🔑"}
REASON_ALIAS = {"INPUT_USER_DEACTIVATED": "USER_DEACTIVATED", "PEER_ID_INVALID": "USER_ID_INVALID"}

def tx(lang, key, **kw):
    s = TX.get(lang, TX["hi"]).get(key) or TX["hi"][key]
    return s.format(**kw)

def reason_info(rid, lang="hi"):
    k = REASON_ALIAS.get(rid, rid)
    if f"r_{k}" in TX["hi"]:
        return REASON_EMOJI.get(k, "⚠️"), tx(lang, f"r_{k}")
    return "⚠️", tx(lang, "r_unknown", rid=rid)

def reason_of(e):
    return getattr(e, "ID", None) or type(e).__name__

def fmt_ts(ts) -> str:
    if not ts:
        return "—"
    return time.strftime("%d %b %y, %H:%M", time.gmtime(float(ts) + 19800)) + " IST"


def day_key(ts=None) -> str:
    """IST din ki key, jaise 20261024."""
    return time.strftime("%Y%m%d", time.gmtime((ts or time.time()) + 19800))

def day_start(day: str) -> float:
    return calendar.timegm(time.strptime(day, "%Y%m%d")) - 19800

def hour_ist() -> int:
    return time.gmtime(time.time() + 19800).tm_hour


class UserError(Exception):
    """User ko seedha dikhne wala saaf error."""


def mention(name, uid):
    return f"[{clean(name or uid)}](tg://user?id={uid})"

def clone_head(lang: str) -> str:
    """Clone me har user ko dikhne wala 'ye bot clone hai' header (clone owner hata nahi sakta)."""
    inst = CUR.get()
    if not inst.is_clone:
        return ""
    owner = mention(inst.owner_name or inst.clone_owner, inst.clone_owner)
    return tx(lang, "clone_head", main=MAIN.username or "main_bot", owner=owner)

def helper_note(lang="hi"):
    if not helper:
        return tx(lang, "helper_none")
    uname = f" (@{helper['username']})" if helper.get("username") else ""
    return tx(lang, "helper_note", who=mention(helper["name"], helper["id"]), uname=uname, hid=helper["id"])


# ---- filters: client se pata chalta hai ki ye main hai ya kaunsa clone
def _inst_of(client):
    return INSTANCES[client.name]
def _owner(_, client, m):
    return bool(m.from_user) and m.from_user.id in _inst_of(client).owners
def _user(_, client, m):
    i = _inst_of(client)
    return bool(m.from_user) and (m.from_user.id in i.owners or i.state["public"])
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
                        if uid in OWNERS else "❌ Something went wrong / कुछ गड़बड़ हुई. Try again later.")
            try:
                await target.reply(text)
            except Exception:
                pass
    return w


# ======================= settings helpers =======================
async def gcfg() -> dict:
    return {**DEFAULTS, **(await db.settings.find_one({"_id": "global"}) or {})}

async def gset(**kw):
    await db.settings.update_one({"_id": "global"}, {"$set": kw}, upsert=True)

async def ccfg(cid: int):
    return await db.chats.find_one({"_id": cid})

async def lang_of(uid: int) -> str:
    if uid in LANG:
        return LANG[uid]
    u = await db.users.find_one({"_id": uid})
    LANG[uid] = (u or {}).get("lang", "hi")
    return LANG[uid]

async def set_lang(uid: int, lang: str):
    LANG[uid] = lang
    await db.users.update_one({"_id": uid}, {"$set": {"lang": lang}}, upsert=True)

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


# ======================= new requests =======================
@on_join_request()
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
            await db.chats.update_one({"_id": cid}, {"$inc": {"approved": 1, f"daily.{day_key()}": 1}})
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


# ======================= helper (user) account =======================
async def stop_user():
    inst = CUR.get()
    for t in list(inst.old_tasks.values()):
        t.cancel()
    if inst.user:
        try:
            await inst.user.stop()
        except Exception:
            pass
    inst.user, inst.helper = None, None

async def start_user(sess: str):
    inst = CUR.get()
    await stop_user()
    c = Client(f"user_{inst.key}", api_id=API_ID, api_hash=API_HASH, session_string=sess, in_memory=True)
    await c.start()
    me = await c.get_me()
    inst.user = c
    inst.helper = {"id": me.id, "name": me.first_name or "Helper", "username": me.username}

async def load_user():
    doc = await db.settings.find_one({"_id": "session"})
    sess = dec((doc or {}).get("string")) or (SESSION_STRING if CUR.get().key == "main" else None)
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

async def old_precheck(cid: int, lang: str):
    """None => theek hai, warna user ko dikhane wala message."""
    if not user:
        return helper_note(lang)
    if cid in old_tasks:
        return tx(lang, "old_running")
    st = await helper_status(cid)
    if st != "ok":
        return tx(lang, {"notmember": "h_notmember", "notadmin": "h_notadmin", "noperm": "h_noperm"}[st]) \
            + "\n\n" + helper_note(lang) + tx(lang, "h_retry")
    return None

def fail_summary(cnt: Counter, lang: str = "hi") -> str:
    lines = []
    for rid, n in cnt.most_common():
        e, d = reason_info(rid, lang)
        lines.append(f"{e} **×{n}** — {d}")
    return "\n".join(lines)

async def approve_old(cid: int, notify: int):
    lang = await lang_of(notify)
    g = await gcfg()
    c = await ccfg(cid) or {}
    lo, hi = c.get("min", g["min"]), c.get("max", g["max"])
    done, fails = 0, Counter()
    try:
        if old_sem.locked():
            await bot.send_message(notify, tx(lang, "old_queue"))
        async with old_sem:
            if not await ensure_peer(cid):
                raise RuntimeError("Helper account is not a member of this chat")
            async for req in user.get_chat_join_requests(cid):
                u = req.user
                while True:
                    try:
                        await user.approve_chat_join_request(cid, u.id)
                        done += 1
                        await db.chats.update_one({"_id": cid}, {"$inc": {"approved": 1, f"daily.{day_key()}": 1}}, upsert=True)
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
                    await bot.send_message(notify, tx(lang, "old_prog", cid=cid, n=done))
        text = tx(lang, "old_done", cid=cid, ok=done, bad=sum(fails.values()))
        if fails:
            text += "\n\n" + fail_summary(fails, lang) + tx(lang, "old_detail")
        await bot.send_message(notify, text)
    except asyncio.CancelledError:
        await bot.send_message(notify, tx(lang, "old_stopped", cid=cid, ok=done))
    except Exception as e:
        await bot.send_message(notify, tx(lang, "old_err", cid=cid, e=f"{type(e).__name__}: {e}"))
    finally:
        old_tasks.pop(cid, None)


# ======================= session login (sirf owner) =======================
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

async def save_session(sess: str):
    await db.settings.update_one({"_id": "session"}, {"$set": {"string": enc(sess)}}, upsert=True)

async def finish_login(m: Message, uid: int, lang: str):
    lg = logins.pop(uid)
    sess = await lg["c"].export_session_string()
    await lg["c"].disconnect()
    pending.pop(uid, None)
    await save_session(sess)
    await start_user(sess)
    await m.reply(tx(lang, "ses_ok"))
    await show(m, *await session_panel(uid))


# ======================= panels =======================
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
    lang = await lang_of(uid)
    rep_on = ((await db.users.find_one({"_id": uid})) or {}).get("report", True)
    if uid in OWNERS:
        pub = STATE["public"]
        hs = ("✅ " + clean(helper["name"])) if helper else "❌"
        hint = tx(lang, "hint_pub") if (helper or not IS_CLONE) else tx(lang, "hint_nohelper")
        text = clone_head(lang) + tx(lang, "admin_main", tag=tx(lang, "tag_clone") if IS_CLONE else "",
                  mode=tx(lang, "mode_pub" if pub else "mode_priv"), lo=g["min"], hi=g["max"],
                  q=queue.qsize(), tot=g.get("total", 0), hs=hs, hint=hint)
        kb = [[B(tx(lang, "b_active", s=tick(g["active"])), callback_data="g:active")],
              [B(tx(lang, "b_autoall", s=tick(g["auto_all"])), callback_data="g:auto_all")],
              [B(tx(lang, "b_wg", s=tick(g["welcome_on"])), callback_data="g:welcome_on")],
              [B(tx(lang, "b_chats"), callback_data="chats"), B(tx(lang, "b_gdelay"), callback_data="delay")],
              [B(tx(lang, "b_gwtext"), callback_data="wt"), B(tx(lang, "b_sess"), callback_data="ses")],
              [B(tx(lang, "b_mode", m=tx(lang, "mode_pub" if pub else "mode_priv")), callback_data="pub")],
              [B(tx(lang, "b_users"), callback_data="us:0"), B(tx(lang, "lang_btn"), callback_data="lang")]]
        if not IS_CLONE:
            kb.append([B(tx(lang, "myclone_btn"), callback_data="k"), B(tx(lang, "b_clones_admin"), callback_data="ka:0")])
        kb.append([B(tx(lang, "b_report", s=tick(rep_on)), callback_data="rep")])
        kb.append([B(tx(lang, "refresh"), callback_data="main")])
    else:
        n, tot = 0, 0
        async for c in db.chats.find({"owner_id": uid}):
            n += 1
            tot += c.get("approved", 0)
        text = clone_head(lang) + tx(lang, "user_main", n=n, tot=tot)
        kb = [[B(tx(lang, "my_chats"), callback_data="chats"), B(tx(lang, "add_chat"), callback_data="add")],
              [B(tx(lang, "old_help_btn"), callback_data="oh")]]
        if not IS_CLONE:
            kb.append([B(tx(lang, "myclone_btn"), callback_data="k")])
        kb.append([B(tx(lang, "b_report", s=tick(rep_on)), callback_data="rep")])
        kb.append([B(tx(lang, "lang_btn"), callback_data="lang"), B(tx(lang, "refresh"), callback_data="main")])
    return text, InlineKeyboardMarkup(kb)

async def chats_panel(uid: int, page=0):
    lang = await lang_of(uid)
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
    rows.append([B(tx(lang, "add_chat"), callback_data="add"), B(tx(lang, "back"), callback_data="main")])
    return tx(lang, "chats_hint", n=len(docs)), InlineKeyboardMarkup(rows)

async def chat_panel(cid: int, uid: int):
    lang = await lang_of(uid)
    c, g = await ccfg(cid) or {}, await gcfg()
    on = c.get("enabled", True)
    lo, hi = c.get("min", g["min"]), c.get("max", g["max"])
    w_on, _w = welcome_of(c, g)
    running = cid in old_tasks
    nfail = await db.fails.count_documents({"chat_id": cid})
    text = tx(lang, "chat_text", title=clean(c.get("title", cid)), cid=cid, on=tick(on), lo=lo, hi=hi,
              dt=tx(lang, "custom") if "min" in c else tx(lang, "global"), w=tick(w_on),
              ap=c.get("approved", 0), td=(c.get("daily") or {}).get(day_key(), 0), nf=nfail,
              old=tx(lang, "running") if running else tx(lang, "idle"))
    kb = InlineKeyboardMarkup([
        [B(tx(lang, "b_approve", s=tick(on)), callback_data=f"ct:{cid}")],
        [B(tx(lang, "b_cdelay"), callback_data=f"cd:{cid}"), B(tx(lang, "b_reset"), callback_data=f"cr:{cid}")],
        [B(tx(lang, "b_wdm", s=tick(w_on)), callback_data=f"cw:{cid}"), B(tx(lang, "b_wtext"), callback_data=f"ce:{cid}")],
        [B(tx(lang, "b_oldstop"), callback_data=f"cs:{cid}") if running
         else B(tx(lang, "b_old"), callback_data=f"co:{cid}")],
        [B(tx(lang, "b_fail"), callback_data=f"fr:{cid}"), B(tx(lang, "b_help"), callback_data="oh")],
        [B(tx(lang, "b_rm"), callback_data=f"cx:{cid}"), B(tx(lang, "back"), callback_data="chats")]])
    return text, kb

async def fails_panel(cid: int, uid: int):
    lang = await lang_of(uid)
    c = await ccfg(cid) or {}
    docs = [f async for f in db.fails.find({"chat_id": cid}).sort("ts", -1).limit(2000)]
    title = tx(lang, "f_title", title=clean(c.get("title", cid)))
    if not docs:
        text = f"{title}\n\n{tx(lang, 'f_none')}"
    else:
        cnt = Counter(f["reason"] for f in docs)
        recent = []
        for f in docs[:12]:
            e, _d = reason_info(f["reason"], lang)
            un = f" @{f['username']}" if f.get("username") else ""
            recent.append(f"{e} {mention(f.get('name'), f['user_id'])}{un} `{f['user_id']}` — {f['reason']}")
        text = (f"{title}\n{tx(lang, 'f_total', n=len(docs))}\n\n{fail_summary(cnt, lang)}\n\n"
                f"{tx(lang, 'f_recent')}\n" + "\n".join(recent))[:4000]
    kb = InlineKeyboardMarkup([
        [B(tx(lang, "f_check"), callback_data=f"fq:{cid}"), B(tx(lang, "f_clear"), callback_data=f"fc:{cid}")],
        [B(tx(lang, "back"), callback_data=f"c:{cid}")]])
    return text, kb

USERS_PER_PAGE = 10

async def users_panel(uid: int, page: int = 0):
    lang = await lang_of(uid)
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
        uname = f"@{u['username']}" if u.get("username") else tx(lang, "no_username")
        lines.append(tx(lang, "u_entry", i=i, crown="👑 " if u["_id"] in OWNERS else "", who=mention(full, u["_id"]),
                        uname=uname, uid=u["_id"], st=fmt_ts(u.get("started")), last=fmt_ts(u.get("last_seen")),
                        nc=n_chats, ap=appr))
    head = tx(lang, "u_head", total=total, new24=new24, act24=act24, p=page + 1, pages=pages)
    text = (head + ("\n\n".join(lines) if lines else tx(lang, "u_none")))[:4000]
    nav = []
    if page > 0:
        nav.append(B(tx(lang, "nav_back"), callback_data=f"us:{page - 1}"))
    nav.append(B(f"{page + 1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(B(tx(lang, "nav_next"), callback_data=f"us:{page + 1}"))
    return text, InlineKeyboardMarkup([nav, [B(tx(lang, "b_menu"), callback_data="main")]])

async def delay_panel(uid: int):
    lang = await lang_of(uid)
    g = await gcfg()
    kb = InlineKeyboardMarkup([
        [B("🚀 1-5s", callback_data="dp:1:5"), B("🙂 2-15s", callback_data="dp:2:15")],
        [B("🐢 10-40s", callback_data="dp:10:40"), B(tx(lang, "b_custom"), callback_data="dc")],
        [B(tx(lang, "back"), callback_data="main")]])
    return tx(lang, "dl_title", lo=g["min"], hi=g["max"]), kb

async def session_panel(uid: int):
    lang = await lang_of(uid)
    note = tx(lang, "ses_clone_note") if IS_CLONE else ""
    if user:
        text = tx(lang, "ses_in", who=mention(helper["name"], helper["id"]), hid=helper["id"]) + note
        rows = [[B(tx(lang, "b_logout"), callback_data="lo")], [B(tx(lang, "back"), callback_data="main")]]
    else:
        text = tx(lang, "ses_out") + note
        rows = [[B(tx(lang, "b_login"), callback_data="lg")],
                [B(tx(lang, "b_paste"), callback_data="ls")],
                [B(tx(lang, "back"), callback_data="main")]]
    return text, InlineKeyboardMarkup(rows)

async def helper_panel(uid: int):
    lang = await lang_of(uid)
    return (helper_note(lang) + tx(lang, "helper_tail"),
            InlineKeyboardMarkup([[B(tx(lang, "back"), callback_data="main")]]))


# ======================= clone bots (main bot me) =======================
async def clone_panel(uid: int):
    lang = await lang_of(uid)
    g = await gcfg()
    d = await CLONES.find_one({"owner_id": uid})
    if not d:
        text = tx(lang, "k_rules") + ("" if g.get("clone_on", True) else "\n\n⚠️ " + tx(lang, "k_off"))
        rows = [[B(tx(lang, "k_new"), callback_data="kn")], [B(tx(lang, "back"), callback_data="main")]]
    else:
        run = d["_id"] in RUNNING
        text = tx(lang, "k_have", u=d.get("username") or d["_id"],
                  st=tx(lang, "k_st_run" if run else "k_st_stop"), when=fmt_ts(d.get("created")))
        rows = []
        if d.get("username"):
            rows.append([B(tx(lang, "k_open", u=d["username"]), url=f"https://t.me/{d['username']}")])
        rows.append([B(tx(lang, "k_restart"), callback_data="kr"), B(tx(lang, "k_del"), callback_data="kd")])
        rows.append([B(tx(lang, "back"), callback_data="main")])
    return text, InlineKeyboardMarkup(rows)

async def clones_admin_panel(uid: int, page: int = 0):
    lang = await lang_of(uid)
    g = await gcfg()
    docs = [d async for d in CLONES.find({}).sort("created", 1)]
    chunk = docs[page * PAGE:(page + 1) * PAGE]
    items = "\n".join(tx(lang, "ka_item", u=d.get("username") or d["_id"], o=d["owner_id"],
                         st="✅" if d["_id"] in RUNNING else "⏸") for d in chunk) or tx(lang, "ka_none")
    on = g.get("clone_on", True)
    rows = [[B(tx(lang, "ka_toggle", s=tick(on)), callback_data="kon")]]
    rows += [[B(f"@{d.get('username') or d['_id']}"[:40], callback_data=f"kv:{d['_id']}")] for d in chunk]
    nav = []
    if page > 0:
        nav.append(B("⬅️", callback_data=f"ka:{page - 1}"))
    if (page + 1) * PAGE < len(docs):
        nav.append(B("➡️", callback_data=f"ka:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([B(tx(lang, "back"), callback_data="main")])
    return tx(lang, "ka_head", n=len(docs), max=MAX_CLONES, on=tick(on), items=items), InlineKeyboardMarkup(rows)

async def clone_detail_panel(uid: int, bid: int):
    lang = await lang_of(uid)
    d = await CLONES.find_one({"_id": bid})
    if not d:
        raise UserError(tx(lang, "ka_none"))
    st = Store(f"c{bid}_")
    run = bid in RUNNING
    text = tx(lang, "kv_text", u=d.get("username") or bid, bid=bid, owner=mention(d["owner_id"], d["owner_id"]),
              st=tx(lang, "k_st_run" if run else "k_st_stop"), when=fmt_ts(d.get("created")),
              nc=await st.chats.count_documents({}), nu=await st.users.count_documents({}))
    rows = [[B(tx(lang, "kv_stop" if run else "kv_start"), callback_data=f"kt:{bid}"),
             B(tx(lang, "k_del"), callback_data=f"kz:{bid}")],
            [B(tx(lang, "back"), callback_data="ka:0")]]
    return text, InlineKeyboardMarkup(rows)

# ======================= daily report =======================
async def send_daily_report(day: str):
    """Aaj ka approved/failed summary: chat owners ko apne chats ka, admin ko sabka."""
    start_ts = day_start(day)
    bad_by_chat = Counter()
    async for f in db.fails.find({"ts": {"$gte": start_ts, "$lt": start_ts + 86400}}):
        bad_by_chat[f["chat_id"]] += 1
    buckets = {}
    async for c in db.chats.find({}):
        ok_n = (c.get("daily") or {}).get(day, 0)
        bad = bad_by_chat.get(c["_id"], 0)
        if not ok_n and not bad:
            continue
        item = (clean(c.get("title") or c["_id"]), ok_n, bad)
        oid = c.get("owner_id")
        targets = set(OWNERS) | ({oid} if oid is not None else set())
        for t in targets:
            buckets.setdefault(t, []).append(item)
    sent = 0
    for uid, items in buckets.items():
        if ((await db.users.find_one({"_id": uid})) or {}).get("report", True) is False:
            continue
        lang = await lang_of(uid)
        top = sorted(items, key=lambda x: (-x[1], -x[2]))[:5]
        text = tx(lang, "rep_title", day=f"{day[6:]}-{day[4:6]}-{day[:4]}", ok=sum(i[1] for i in items), bad=sum(i[2] for i in items))
        text += tx(lang, "rep_top", lines="\n".join(tx(lang, "rep_chat", title=t, ok=o, bad=b) for t, o, b in top))
        text += tx(lang, "rep_foot")
        try:
            await bot.send_message(uid, text)
            sent += 1
        except Exception:
            pass
    # purane din ke counters saaf (8 din se purane)
    cutoff = day_key(time.time() - 8 * 86400)
    async for c in db.chats.find({}):
        old = [k for k in (c.get("daily") or {}) if k < cutoff]
        if old:
            await db.chats.update_one({"_id": c["_id"]}, {"$unset": {f"daily.{k}": "" for k in old}})
    return sent

async def report_tick() -> bool:
    day = day_key()
    if hour_ist() >= REPORT_HOUR and (await gcfg()).get("last_report") != day:
        await gset(last_report=day)          # pehle mark: double send na ho
        await send_daily_report(day)
        return True
    return False

async def report_loop():
    await asyncio.sleep(30)
    while True:
        try:
            await report_tick()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("report_loop")
        await asyncio.sleep(300)


async def boot_instance(inst, workers: int):
    """Bot (main ya clone) ki settings + helper session load karke workers chalata hai."""
    try:
        STATE["public"] = bool((await gcfg()).get("public", False))
    except Exception as e:
        log.warning("settings load fail (%s): %s", inst.key, e)
    try:
        await load_user()
    except Exception as e:
        log.warning("session load fail (%s): %s", inst.key, e)
    for _ in range(workers):
        inst.workers.append(asyncio.create_task(worker()))
    inst.workers.append(asyncio.create_task(report_loop()))

async def start_clone(doc: dict):
    bid = int(doc["_id"])
    if bid in RUNNING:
        return RUNNING[bid]
    token = dec(doc.get("token"))
    if not token:
        raise RuntimeError("token decrypt nahi hua (SECRET_KEY badla?)")
    key = f"clone{bid}"
    client = Client(key, api_id=API_ID, api_hash=API_HASH, bot_token=token, in_memory=True)
    inst = Inst(key, client, {int(doc["owner_id"])}, Store(f"c{bid}_"), clone_owner=int(doc["owner_id"]), bot_id=bid)
    inst.owner_name = doc.get("owner_name")
    INSTANCES[key] = inst
    attach(inst)
    try:
        await client.start()
        me = await client.get_me()
        inst.bot_id["id"], inst.username = me.id, me.username
        await run_in(inst, boot_instance, inst, CLONE_WORKERS)
    except Exception:
        INSTANCES.pop(key, None)
        try:
            await client.stop()
        except Exception:
            pass
        raise
    RUNNING[bid] = inst
    return inst

async def stop_clone(bid: int):
    inst = RUNNING.pop(bid, None)
    if not inst:
        return
    for w in inst.workers:
        w.cancel()
    await run_in(inst, stop_user)
    try:
        await inst.client.stop()
    except Exception:
        pass
    INSTANCES.pop(inst.key, None)

async def delete_clone(bid: int):
    await stop_clone(bid)
    await Store(f"c{bid}_").drop()
    await CLONES.delete_one({"_id": bid})

async def create_clone(uid: int, token: str, lang: str, owner_name: str = None):
    g = await gcfg()
    if not g.get("clone_on", True) and uid not in OWNERS:
        raise UserError(tx(lang, "k_off"))
    if not TOKEN_RE.fullmatch(token):
        raise UserError(tx(lang, "k_badtoken"))
    bid = int(token.split(":")[0])
    if bid == BOT_ID["id"]:
        raise UserError(tx(lang, "k_same"))
    if await CLONES.find_one({"_id": bid}):
        raise UserError(tx(lang, "k_dup"))
    if await CLONES.find_one({"owner_id": uid}):
        raise UserError(tx(lang, "k_one"))
    if await CLONES.count_documents({}) >= MAX_CLONES:
        raise UserError(tx(lang, "k_full"))
    doc = {"_id": bid, "owner_id": uid, "owner_name": owner_name, "token": enc(token), "created": time.time(), "active": True}
    try:
        inst = await start_clone(doc)
    except Exception as e:
        log.warning("clone start fail: %s", type(e).__name__)
        raise UserError(tx(lang, "k_invalid"))
    doc["username"] = inst.username
    await CLONES.update_one({"_id": bid}, {"$set": doc}, upsert=True)
    return inst


# ======================= commands =======================
@on_message(filters.command(["start", "settings", "help"]) & filters.private)
@safe
async def start(_, m: Message):
    uid = m.from_user.id
    await touch_user(m.from_user)
    lang = await lang_of(uid)
    if uid not in OWNERS and not STATE["public"]:
        return await m.reply(clone_head(lang) + tx(lang, "private_bot", uid=uid))
    await cancel_login(uid)
    await show(m, *await main_panel(uid))

@on_message(filters.command("id") & filters.private)
async def my_id(_, m: Message):
    uid = m.from_user.id
    extra = f"\nOWNER_IDS: `{sorted(OWNERS)}`\nOwner match: {uid in OWNERS}" if uid in OWNERS or not OWNERS else ""
    await m.reply(f"ID: `{uid}`{extra}")

async def mongo_status() -> str:
    try:
        await asyncio.wait_for(MDB.command("ping"), 10)
        return "✅ MongoDB OK"
    except Exception as e:
        return f"❌ MongoDB error: {type(e).__name__}: {str(e)[:500]}"

@on_message(filters.command("ping") & filters.private)
async def ping(_, m: Message):
    if m.from_user.id not in OWNERS:
        return await m.reply("🏓 pong")
    lang = await lang_of(m.from_user.id)
    await m.reply(tx(lang, "ping_full", mongo=await mongo_status(), h="✅" if user else "❌",
                     mode=tx(lang, "mode_pub" if STATE["public"] else "mode_priv"), nc=len(RUNNING)))

@on_message(filters.command(["public", "private"]) & filters.private & is_owner)
@safe
async def cmd_mode(_, m: Message):
    pub = m.command[0] == "public"
    STATE["public"] = pub
    await gset(public=pub)
    await m.reply(tx(await lang_of(m.from_user.id), "mode_set_pub" if pub else "mode_set_priv"))

@on_message(filters.command("users") & filters.private & is_owner)
@safe
async def cmd_users(_, m: Message):
    await show(m, *await users_panel(m.from_user.id, 0))

@on_message(filters.command("approveold") & filters.private & is_owner)
@safe
async def cmd_old(_, m: Message):
    lang = await lang_of(m.from_user.id)
    cid = int(m.command[1])
    err = await old_precheck(cid, lang)
    if err:
        return await m.reply(err)
    old_tasks[cid] = asyncio.create_task(approve_old(cid, m.chat.id))
    await m.reply(tx(lang, "old_started"))

@on_message(filters.command("stopold") & filters.private & is_owner)
@safe
async def cmd_stop(_, m: Message):
    lang = await lang_of(m.from_user.id)
    t = old_tasks.get(int(m.command[1]))
    if t:
        t.cancel()
    await m.reply(tx(lang, "old_stopping") if t else tx(lang, "old_nothing"))


# ======================= chat add / membership =======================
async def verify_add(uid: int, chat, lang: str):
    if uid not in OWNERS:
        if await db.chats.count_documents({"owner_id": uid, "_id": {"$ne": chat.id}}) >= MAX_CHATS:
            raise UserError(tx(lang, "max_chats", lim=MAX_CHATS))
        try:
            mem = await bot.get_chat_member(chat.id, uid)
        except Exception:
            raise UserError(tx(lang, "e_notadmin_user"))
        if mem.status not in (ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR):
            raise UserError(tx(lang, "e_onlyadmin"))
    try:
        me = await bot.get_chat_member(chat.id, "me")
    except Exception:
        raise UserError(tx(lang, "e_bot_not_in"))
    if me.status != ChatMemberStatus.ADMINISTRATOR:
        raise UserError(tx(lang, "e_bot_not_admin"))
    if not (me.privileges and me.privileges.can_invite_users):
        raise UserError(tx(lang, "e_bot_noinvite"))

@on_member_update()
async def on_member(_, u):
    """Bot ko admin banate hi chat user ke naam register ho jata hai."""
    try:
        new = u.new_chat_member
        if not new or not new.user or new.user.id != BOT_ID["id"] or new.status != ChatMemberStatus.ADMINISTRATOR:
            return
        if not u.from_user:
            return
        uid = u.from_user.id
        lang = await lang_of(uid)
        if uid not in OWNERS:
            if not STATE["public"]:
                return
            if await db.chats.count_documents({"owner_id": uid, "_id": {"$ne": u.chat.id}}) >= MAX_CHATS:
                await bot.send_message(uid, tx(lang, "max_chats", lim=MAX_CHATS))
                return
        await register_chat(u.chat, uid)
        c = await ccfg(u.chat.id) or {}
        if uid not in OWNERS and c.get("owner_id") != uid:
            return
        warn = "" if (new.privileges and new.privileges.can_invite_users) else tx(lang, "warn_noinvite")
        await bot.send_message(uid, tx(lang, "member_added", title=clean(u.chat.title), warn=warn, note=helper_note(lang)))
    except Exception as e:
        log.warning("on_member: %s", e)


# ======================= callbacks =======================
OWNER_ACTS = {"us", "g", "ses", "lg", "ls", "lo", "delay", "dp", "dc", "wt", "pub", "ka", "kv", "kt", "kz", "kzy", "kon"}
MAIN_ONLY = {"k", "kn", "kr", "kd", "kdy", "ka", "kv", "kt", "kz", "kzy", "kon"}
CHAT_ACTS = {"c", "ct", "cr", "cd", "co", "cs", "cx", "cw", "ce", "fr", "fc", "fq"}

@on_callback(is_user)
@safe
async def cb(_, q: CallbackQuery):
    act, _, arg = q.data.partition(":")
    uid = q.from_user.id
    await touch_user(q.from_user)
    lang = await lang_of(uid)
    try:
        if act in OWNER_ACTS and uid not in OWNERS:
            return await q.answer(tx(lang, "q_owner_only"), show_alert=True)
        if act in MAIN_ONLY and IS_CLONE:
            return await q.answer(tx(lang, "q_main_only"), show_alert=True)
        if act in CHAT_ACTS and not await can_manage(uid, int(arg)):
            return await q.answer(tx(lang, "q_no_access"), show_alert=True)

        if act == "noop":
            return
        if act == "us":
            return await show(q, *await users_panel(uid, int(arg)))
        if act == "main":
            await cancel_login(uid)
            return await show(q, *await main_panel(uid))
        if act == "lang":
            await set_lang(uid, "en" if lang == "hi" else "hi")
            return await show(q, *await main_panel(uid))
        if act == "rep":
            cur = ((await db.users.find_one({"_id": uid})) or {}).get("report", True)
            await db.users.update_one({"_id": uid}, {"$set": {"report": not cur}}, upsert=True)
            await q.answer(tx(lang, "q_rep_off" if cur else "q_rep_on"))
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
            return await show(q, *await helper_panel(uid))
        if act == "add":
            pending[uid] = ("add",)
            return await q.message.reply(tx(lang, "add_prompt"))
        # ---- clone bots (main bot only)
        if act == "k":
            return await show(q, *await clone_panel(uid))
        if act == "kn":
            if await CLONES.find_one({"owner_id": uid}):
                return await q.answer(tx(lang, "k_one"), show_alert=True)
            pending[uid] = ("ctoken",)
            return await q.message.reply(tx(lang, "k_ask_token"))
        if act == "kr":
            d = await CLONES.find_one({"owner_id": uid})
            if d:
                await stop_clone(d["_id"])
                try:
                    await start_clone(d)
                except Exception:
                    raise UserError(tx(lang, "k_invalid"))
                await q.message.reply(tx(lang, "k_restarted"))
            return await show(q, *await clone_panel(uid))
        if act == "kd":
            d = await CLONES.find_one({"owner_id": uid})
            if not d:
                return await show(q, *await clone_panel(uid))
            return await show(q, tx(lang, "k_del_ask", u=d.get("username") or d["_id"]),
                              InlineKeyboardMarkup([[B(tx(lang, "k_yes"), callback_data="kdy"), B(tx(lang, "k_no"), callback_data="k")]]))
        if act == "kdy":
            d = await CLONES.find_one({"owner_id": uid})
            if d:
                await delete_clone(d["_id"])
                await q.message.reply(tx(lang, "k_deleted"))
            return await show(q, *await clone_panel(uid))
        if act == "ka":
            return await show(q, *await clones_admin_panel(uid, int(arg)))
        if act == "kon":
            g = await gcfg()
            await gset(clone_on=not g.get("clone_on", True))
            return await show(q, *await clones_admin_panel(uid))
        if act == "kv":
            return await show(q, *await clone_detail_panel(uid, int(arg)))
        if act == "kt":
            bid = int(arg)
            d = await CLONES.find_one({"_id": bid})
            if d:
                if bid in RUNNING:
                    await stop_clone(bid)
                    await CLONES.update_one({"_id": bid}, {"$set": {"active": False}})
                else:
                    try:
                        await start_clone(d)
                        await CLONES.update_one({"_id": bid}, {"$set": {"active": True}})
                    except Exception:
                        raise UserError(tx(lang, "k_invalid"))
            return await show(q, *await clone_detail_panel(uid, bid))
        if act == "kz":
            d = await CLONES.find_one({"_id": int(arg)})
            if not d:
                return await show(q, *await clones_admin_panel(uid))
            return await show(q, tx(lang, "kz_ask", u=d.get("username") or d["_id"]),
                              InlineKeyboardMarkup([[B(tx(lang, "k_yes"), callback_data=f"kzy:{arg}"), B(tx(lang, "k_no"), callback_data=f"kv:{arg}")]]))
        if act == "kzy":
            bid = int(arg)
            d = await CLONES.find_one({"_id": bid})
            if d:
                await delete_clone(bid)
                try:
                    await bot.send_message(d["owner_id"], tx(await lang_of(d["owner_id"]), "k_admin_removed"))
                except Exception:
                    pass
            return await show(q, *await clones_admin_panel(uid))
        # ---- chat actions
        if act == "c":
            return await show(q, *await chat_panel(int(arg), uid))
        if act == "ct":
            cid = int(arg)
            c = await ccfg(cid) or {}
            await db.chats.update_one({"_id": cid}, {"$set": {"enabled": not c.get("enabled", True)}}, upsert=True)
            return await show(q, *await chat_panel(cid, uid))
        if act == "cr":
            await db.chats.update_one({"_id": int(arg)}, {"$unset": {"min": "", "max": ""}})
            return await show(q, *await chat_panel(int(arg), uid))
        if act == "cd":
            pending[uid] = ("cdelay", int(arg))
            return await q.message.reply(tx(lang, "cd_prompt"))
        if act == "cw":
            cid = int(arg)
            c, g = await ccfg(cid) or {}, await gcfg()
            on, _w = welcome_of(c, g)
            await db.chats.update_one({"_id": cid}, {"$set": {"welcome_on": not on}}, upsert=True)
            return await show(q, *await chat_panel(cid, uid))
        if act == "ce":
            pending[uid] = ("cwelcome", int(arg))
            return await q.message.reply(tx(lang, "ce_prompt"))
        if act == "co":
            cid = int(arg)
            err = await old_precheck(cid, lang)
            if err:
                return await q.message.reply(err)
            old_tasks[cid] = asyncio.create_task(approve_old(cid, uid))
            await q.answer(tx(lang, "q_started"))
            return await show(q, *await chat_panel(cid, uid))
        if act == "cs":
            t = old_tasks.get(int(arg))
            if t:
                t.cancel()
            await q.answer(tx(lang, "q_stopping"))
            await asyncio.sleep(1)
            return await show(q, *await chat_panel(int(arg), uid))
        if act == "cx":
            await db.chats.delete_one({"_id": int(arg)})
            await db.fails.delete_many({"chat_id": int(arg)})
            await q.answer(tx(lang, "q_removed"))
            return await show(q, *await chats_panel(uid))
        if act == "fr":
            return await show(q, *await fails_panel(int(arg), uid))
        if act == "fc":
            await db.fails.delete_many({"chat_id": int(arg)})
            await q.answer(tx(lang, "q_clear"))
            return await show(q, *await fails_panel(int(arg), uid))
        if act == "fq":
            pending[uid] = ("whyid", int(arg))
            return await q.message.reply(tx(lang, "f_ask"))
        # ---- owner only
        if act == "ses":
            return await show(q, *await session_panel(uid))
        if act == "lg":
            await cancel_login(uid)
            pending[uid] = ("lphone",)
            return await q.message.reply(tx(lang, "ses_phone"))
        if act == "ls":
            pending[uid] = ("lstr",)
            return await q.message.reply(tx(lang, "ses_str"))
        if act == "lo":
            await stop_user()
            await db.settings.delete_one({"_id": "session"})
            await q.answer(tx(lang, "ses_bye"))
            return await show(q, *await session_panel(uid))
        if act == "delay":
            return await show(q, *await delay_panel(uid))
        if act == "dp":
            lo, hi = arg.split(":")
            await gset(min=float(lo), max=float(hi))
            return await show(q, *await delay_panel(uid))
        if act == "dc":
            pending[uid] = ("delay",)
            return await q.message.reply(tx(lang, "gd_prompt"))
        if act == "wt":
            pending[uid] = ("welcome",)
            g = await gcfg()
            return await q.message.reply(tx(lang, "wt_prompt", cur=g["welcome"]))
    finally:
        try:
            await q.answer()
        except Exception:
            pass


# ======================= text input (button ke baad) =======================
OWNER_KINDS = {"lphone", "lcode", "lpass", "lstr", "delay", "welcome"}

def check_template(text: str, lang: str = "hi"):
    try:
        text.format(name="x", chat="y")
    except Exception:
        raise UserError(tx(lang, "e_tpl"))

@on_message(filters.private & is_user & ~filters.regex(r"^/"), group=1)
@safe
async def on_input(_, m: Message):
    uid = m.from_user.id
    st = pending.get(uid)
    if not st:
        return
    await touch_user(m.from_user)
    kind = st[0]
    lang = await lang_of(uid)
    if (kind in OWNER_KINDS and uid not in OWNERS) or (kind == "ctoken" and IS_CLONE):
        pending.pop(uid, None)
        return
    try:
        if kind == "ctoken":
            token = (m.text or "").strip()
            await try_delete(m)              # token chat me na rahe
            pending.pop(uid, None)
            wait = await m.reply(tx(lang, "k_wait"))
            try:
                inst = await create_clone(uid, token, lang, m.from_user.first_name)
            finally:
                await try_delete(wait)
            return await m.reply(tx(lang, "k_done", u=inst.username),
                                 reply_markup=InlineKeyboardMarkup(
                                     [[B(tx(lang, "k_open", u=inst.username), url=f"https://t.me/{inst.username}")]]))
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
            return await m.reply(tx(lang, "ses_otp"))
        if kind == "lcode":
            lg = logins[uid]
            code = "".join(ch for ch in m.text if ch.isdigit())
            await try_delete(m)
            try:
                await lg["c"].sign_in(lg["phone"], lg["hash"], code)
            except SessionPasswordNeeded:
                pending[uid] = ("lpass",)
                return await m.reply(tx(lang, "ses_2fa"))
            return await finish_login(m, uid, lang)
        if kind == "lpass":
            lg = logins[uid]
            await try_delete(m)
            await lg["c"].check_password(m.text)
            return await finish_login(m, uid, lang)
        if kind == "lstr":
            await try_delete(m)
            sess = m.text.strip()
            await start_user(sess)
            await save_session(sess)
            pending.pop(uid, None)
            await m.reply(tx(lang, "ses_set"))
            return await show(m, *await session_panel(uid))
        if kind == "add":
            fo = getattr(m, "forward_from_chat", None) or getattr(getattr(m, "forward_origin", None), "chat", None)
            ref = fo.id if fo else (m.text or "").strip()
            ref = int(ref) if str(ref).lstrip("-").isdigit() else ref
            try:
                chat = await bot.get_chat(ref)
            except Exception:
                raise UserError(tx(lang, "e_notfound"))
            await verify_add(uid, chat, lang)
            pending.pop(uid, None)
            await register_chat(chat, uid, force=(uid not in OWNERS))
            await m.reply(f"{tx(lang, 'added', title=clean(chat.title))}\n\n{helper_note(lang)}")
            return await show(m, *await chat_panel(chat.id, uid))
        if kind in ("delay", "cdelay"):
            lo, hi = sorted(float(x) for x in m.text.split()[:2])
            if lo < 0 or (uid not in OWNERS and (lo < 1 or hi > 300)):
                raise UserError(tx(lang, "e_delay"))
            if kind == "delay":
                await gset(min=lo, max=hi)
                pending.pop(uid, None)
                return await show(m, *await delay_panel(uid))
            await db.chats.update_one({"_id": st[1]}, {"$set": {"min": lo, "max": hi}}, upsert=True)
            pending.pop(uid, None)
            return await show(m, *await chat_panel(st[1], uid))
        if kind == "welcome":
            check_template(m.text, lang)
            await gset(welcome=m.text, welcome_on=True)
            pending.pop(uid, None)
            await m.reply(tx(lang, "saved_gw"))
            return await show(m, *await main_panel(uid))
        if kind == "cwelcome":
            check_template(m.text, lang)
            await db.chats.update_one({"_id": st[1]}, {"$set": {"welcome": m.text, "welcome_on": True}}, upsert=True)
            pending.pop(uid, None)
            await m.reply(tx(lang, "saved_w"))
            return await show(m, *await chat_panel(st[1], uid))
        if kind == "whyid":
            target = int(m.text.strip())
            f = await db.fails.find_one({"_id": f"{st[1]}:{target}"})
            pending.pop(uid, None)
            if not f:
                await m.reply(tx(lang, "f_nf"))
            else:
                e, d = reason_info(f["reason"], lang)
                await m.reply(tx(lang, "f_one", e=e, who=mention(f.get("name"), target), uid=target, d=d, code=f["reason"],
                                 when=fmt_ts(f["ts"]), kind=tx(lang, "kind_old" if f.get("src") == "old" else "kind_new")))
            return await show(m, *await fails_panel(st[1], uid))
    except Exception as e:
        if isinstance(e, UserError):
            return await m.reply(f"❌ {e}")
        if kind == "lphone" or isinstance(e, PhoneCodeExpired) or kind == "lstr":
            await cancel_login(uid)
            return await m.reply(tx(lang, "ses_fail", err=f"{type(e).__name__}: {str(e)[:500]}"))
        log.exception("input error")
        detail = f"{type(e).__name__}: {str(e)[:500]}" if uid in OWNERS else tx(lang, "in_err")
        await m.reply(tx(lang, "in_err_full", d=detail))


# ======================= web server + self ping =======================
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

async def boot_clones():
    """Restart ke baad saare saved clones wapas chalu."""
    async for d in CLONES.find({"active": {"$ne": False}}):
        try:
            await start_clone(d)
            log.info("clone started: @%s", d.get("username"))
        except Exception as e:
            log.warning("clone %s start fail: %s", d["_id"], type(e).__name__)
            await CLONES.update_one({"_id": d["_id"]}, {"$set": {"active": False}})
            try:
                lang = await run_in(MAIN, lang_of, d["owner_id"])
                await MAIN.client.send_message(d["owner_id"], tx(lang, "k_failed_boot", u=d.get("username") or d["_id"]))
            except Exception:
                pass
        await asyncio.sleep(1)

async def main():
    await web_app()
    attach(MAIN)
    await MAIN.client.start()
    CUR.set(MAIN)                                   # main task ka context = main bot
    me = await MAIN.client.get_me()
    MAIN.bot_id["id"], MAIN.username = me.id, me.username
    await boot_instance(MAIN, WORKERS)
    asyncio.create_task(self_ping())
    await boot_clones()
    status = await mongo_status()
    log.info("Bot started | %s | owners=%s | public=%s | clones=%s", status, sorted(MAIN.owners), MAIN.state["public"], len(RUNNING))
    for oid in MAIN.owners:
        try:
            await MAIN.client.send_message(
                oid, f"🚀 Bot started\n{status}\nMode: {'🌐 Public' if MAIN.state['public'] else '🔒 Private'}"
                     f"\nHelper: {'✅' if MAIN.user else '❌'} | Clones: {len(RUNNING)}")
        except Exception as e:
            log.warning("owner %s ko msg nahi gaya: %s", oid, e)
    await asyncio.Event().wait()

if __name__ == "__main__":
    loop.run_until_complete(main())
