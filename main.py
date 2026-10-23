import os, re, asyncio, random, logging, functools, time, math
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
SESSION_STRING = os.environ.get("SESSION_STRING")          # optional (bot ke andar bhi login ho sakta hai)
PORT = int(os.environ.get("PORT", 8080))
SELF_URL = os.environ.get("RENDER_EXTERNAL_URL")
WORKERS = int(os.environ.get("WORKERS", 8))
FREE_CHATS = int(os.environ.get("FREE_CHATS", 2))           # Free plan: max chats
PREMIUM_CHATS = int(os.environ.get("PREMIUM_CHATS", 20))    # Premium plan: max chats
GATE_MINUTES = int(os.environ.get("GATE_MINUTES", 30))      # captcha / force-sub ka time limit
ADMIN_CONTACT = os.environ.get("ADMIN_CONTACT", "")         # jaise @myusername (Premium lene ke liye)
ENV_LOG = os.environ.get("LOG_CHAT_ID")                     # optional log channel
PAGE = 8

db = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=8000)["joinbot"]
bot = Client("bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)
user = None      # helper (user) account
helper = None    # {"id", "name", "username"}
BOT_ID = {"id": 0}
STATE = {"public": False}
LOG = {"chat": None}
BANNED: set = set()
LANG: dict = {}

queue: asyncio.Queue = asyncio.Queue()
old_tasks: dict = {}
old_sem = asyncio.Semaphore(1)
logins: dict = {}
pending: dict = {}

DEFAULTS = {"active": True, "auto_all": True, "min": 2, "max": 15, "welcome_on": False, "public": False,
            "welcome": "Hello {name}! {chat} me aapki request approve ho gayi hai."}
tick = lambda b: "✅" if b else "❌"
clean = lambda t: re.sub(r"[\[\]*_`~|]", "", str(t))
AUTH_DEAD = {"AUTH_KEY_UNREGISTERED", "SESSION_REVOKED", "SESSION_EXPIRED", "AUTH_KEY_INVALID",
             "USER_DEACTIVATED", "USER_DEACTIVATED_BAN", "AUTH_KEY_DUPLICATED"}


# ======================= TEXTS (Hindi / English) =======================
TX = {
"hi": {
 "back": "🔙 वापस", "refresh": "🔄 रीफ्रेश", "my_chats": "📺 मेरे चैट", "add_chat": "➕ चैट जोड़ें",
 "old_help_btn": "ℹ️ पुरानी रिक्वेस्ट मदद", "plan_btn": "💎 प्लान", "lang_btn": "🌐 भाषा: हिंदी",
 "plan_free": "Free", "plan_prem": "Premium ({d} दिन बाकी)", "plan_prem_owner": "Premium (Admin)",
 "user_main": "**🤖 ऑटो जॉइन रिक्वेस्ट बॉट**\n\nअपने चैनल/ग्रुप की जॉइन रिक्वेस्ट अपने-आप approve कराओ।\n\n"
              "1️⃣ बॉट को अपने चैट में **admin** बनाओ (\"Invite Users\" permission के साथ)\n"
              "2️⃣ चैट अपने-आप जुड़ जाएगा, या ➕ चैट जोड़ें दबाओ\n3️⃣ 📺 मेरे चैट में सेटिंग्स बदलो\n\n"
              "💎 प्लान: **{plan}** | चैट: {n}/{lim}\nApproved: {tot}",
 "private_bot": "🔒 यह बॉट अभी प्राइवेट है।\nआपकी ID: `{uid}`",
 "banned": "🚫 आपको इस बॉट से बैन कर दिया गया है।",
 "chats_hint": "**📺 चैट ({n})**\nसेटिंग्स खोलने के लिए चैट पर टैप करो।",
 "add_prompt": "चैट की ID (`-100...`), @username भेजो, या चैनल/ग्रुप का कोई मैसेज फॉरवर्ड करो।\n"
               "बॉट उस चैट में admin होना चाहिए और आप भी उस चैट के admin।",
 "chat_text": "**{title}**\n`{cid}`\n\nApprove: {on}\nDelay: {lo}-{hi}s {dt}\nWelcome DM: {w}\n"
              "🛡 कैप्चा: {cap}\n📢 Force Subscribe: {fs} चैनल\n🌐 DM भाषा: {cl}\n"
              "Approved: {ap} | Failed records: {nf}\nOld task: {old}",
 "custom": "(कस्टम)", "global": "(ग्लोबल)", "running": "चल रहा ▶️", "idle": "खाली",
 "b_approve": "Approve {s}", "b_delay": "⏱ कस्टम डिले", "b_reset": "♻️ रीसेट", "b_wdm": "वेलकम DM {s}",
 "b_wtext": "✏️ वेलकम टेक्स्ट", "b_cap": "🛡 कैप्चा: {m}{lock}", "b_fs": "📢 Force Subscribe ({n}){lock}",
 "b_cl": "🌐 DM भाषा: {l}", "b_old": "🕰 पुरानी रिक्वेस्ट approve{lock}", "b_oldstop": "⏹ पुरानी approve रोको",
 "b_fail": "❌ फेल रिपोर्ट", "b_help": "ℹ️ Helper जानकारी", "b_rm": "🗑 हटाओ",
 "cap_off": "बंद", "cap_human": "इंसान बटन", "cap_math": "गणित", "lang_hi": "हिंदी", "lang_en": "English",
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
 "locked": "💎 यह Premium फीचर है। 💎 प्लान दबाकर देखो।",
 "max_chats": "आपके प्लान में अधिकतम {lim} चैट जुड़ सकते हैं। ज़्यादा के लिए Premium लो (💎 प्लान)।",
 "plan_text": "**💎 प्लान**\n\nआपका प्लान: **{plan}**\n\n**Free**\n• {fc} चैट तक\n• ऑटो approve, डिले, वेलकम DM, फेल रिपोर्ट\n\n"
              "**Premium**\n• {pc} चैट तक\n• 🛡 कैप्चा (इंसान बटन / गणित)\n• 📢 Force Subscribe\n• 🕰 पुरानी रिक्वेस्ट approve\n\n"
              "Premium के लिए संपर्क करो: {contact}",
 "e_notfound": "चैट नहीं मिला। पहले बॉट को उस चैट में admin बनाओ, फिर ID/@username या फॉरवर्ड किया मैसेज भेजो।",
 "e_notadmin_user": "आप इस चैट के admin नहीं लग रहे।",
 "e_onlyadmin": "सिर्फ चैट के admin/owner ही चैट जोड़ सकते हैं।",
 "e_bot_not_in": "बॉट इस चैट में नहीं है। पहले बॉट को चैट में जोड़कर admin बनाओ।",
 "e_bot_not_admin": "बॉट इस चैट में admin नहीं है। पहले बॉट को admin बनाओ।",
 "e_bot_noinvite": "बॉट को \"Invite Users\" (Add Members) permission दो।",
 "e_delay": "डिले 1 से 300 सेकंड के बीच रखो।",
 "e_tpl": "टेक्स्ट में सिर्फ {{name}} और {{chat}} इस्तेमाल करो (दूसरे {{ }} नहीं)।",
 "e_nolink": "इस चैनल का public username नहीं है और बॉट invite link नहीं बना पाया। बॉट को \"Invite Users\" permission दो।",
 "added": "✅ जुड़ गया: {title}",
 "member_added": "✅ **{title}** जुड़ गया!{warn}\n\n{note}\n\nसेटिंग्स के लिए /settings → 📺 मेरे चैट",
 "warn_noinvite": "\n\n⚠️ बॉट को \"Invite Users\" permission नहीं मिली, इसके बिना रिक्वेस्ट approve नहीं होंगी।",
 "bot_removed": "⚠️ बॉट को **{title}** से हटा दिया गया है, अब वहाँ रिक्वेस्ट approve नहीं होंगी।",
 "bot_demoted": "⚠️ **{title}** में बॉट का admin हटा दिया गया है, अब रिक्वेस्ट approve नहीं होंगी।",
 "cd_prompt": "इस चैट का डिले भेजो: `min max` (जैसे `3 12`)",
 "ce_prompt": "इस चैट के लिए नया वेलकम DM टेक्स्ट भेजो। {{name}} और {{chat}} इस्तेमाल कर सकते हो।",
 "saved_w": "✅ इस चैट का वेलकम DM सेव + ON",
 "q_no_access": "आपको इस चैट की access नहीं है।", "q_owner_only": "यह option सिर्फ admin के लिए है।",
 "q_removed": "हटा दिया", "q_clear": "रिपोर्ट साफ़", "q_stopping": "रोक रहे हैं...", "q_started": "पुरानी approve शुरू ▶️",
 "fs_title": "**📢 Force Subscribe — {title}**\n\nरिक्वेस्ट approve होने से पहले यूज़र को ये चैनल जॉइन करने होंगे (अधिकतम 3)। "
             "बॉट को उन चैनलों में **admin** होना चाहिए।\n\n{items}",
 "fs_empty": "अभी कोई चैनल नहीं जोड़ा।", "fs_add": "➕ चैनल जोड़ें", "fs_max": "अधिकतम 3 चैनल जोड़ सकते हो।",
 "fs_ask": "जो चैनल जोड़ना है उसकी ID, @username भेजो या उसका कोई मैसेज फॉरवर्ड करो।",
 "cap_msg_human": "🛡 आपने **{chat}** में जॉइन रिक्वेस्ट भेजी है।\n\nवेरिफाई करने के लिए नीचे बटन दबाओ। ({m} मिनट में)",
 "human_btn": "✅ मैं इंसान हूँ",
 "cap_msg_math": "🛡 आपने **{chat}** में जॉइन रिक्वेस्ट भेजी है।\n\nवेरिफाई करने के लिए सही जवाब चुनो:\n\n**{q} = ?**\n\n({m} मिनट में)",
 "cap_wrong": "❌ गलत जवाब। नया सवाल देखो।",
 "cap_failed": "❌ वेरिफिकेशन फेल। आपकी रिक्वेस्ट रद्द कर दी गई है, चाहो तो दोबारा रिक्वेस्ट भेजो।",
 "cap_ok": "✅ वेरिफाई हो गया! आपकी रिक्वेस्ट approve हो रही है।",
 "gate_expired": "⌛ समय खत्म। दोबारा जॉइन रिक्वेस्ट भेजो।",
 "fs_msg": "📢 **{chat}** में approve होने के लिए पहले ये चैनल जॉइन करो, फिर नीचे \"मैंने जॉइन कर लिया\" दबाओ।",
 "joined_btn": "✅ मैंने जॉइन कर लिया", "not_joined": "आपने अभी सारे चैनल जॉइन नहीं किए।",
 "fs_ok": "✅ बढ़िया! आपकी रिक्वेस्ट approve हो रही है।",
 "r_USER_CHANNELS_TOO_MUCH": "यूज़र Telegram की 500 चैनल/ग्रुप वाली सीमा पर है, इसलिए और कहीं जॉइन नहीं हो सकता। (कोई उपाय नहीं)",
 "r_USER_DEACTIVATED": "अकाउंट डिलीट/डिएक्टिवेट हो चुका है।",
 "r_USER_DEACTIVATED_BAN": "अकाउंट Telegram ने बैन कर दिया है।",
 "r_HIDE_REQUESTER_MISSING": "रिक्वेस्ट अब मौजूद नहीं: यूज़र ने वापस ले ली या किसी और admin ने पहले ही हैंडल कर दी।",
 "r_USER_ALREADY_PARTICIPANT": "यूज़र पहले से मेंबर है।",
 "r_USER_BANNED_IN_CHANNEL": "यूज़र इस चैट में banned है। पहले unban करो।",
 "r_USER_PRIVACY_RESTRICTED": "यूज़र की प्राइवेसी सेटिंग्स की वजह से add नहीं हो सकता।",
 "r_USER_ID_INVALID": "यूज़र नहीं मिला (डिलीट/invalid)।",
 "r_CHAT_ADMIN_REQUIRED": "Admin permission कम है। \"Invite Users\" permission दो।",
 "r_CAPTCHA_DM_FAILED": "कैप्चा का मैसेज यूज़र को नहीं भेजा जा सका।",
 "r_CAPTCHA_FAILED": "कैप्चा में 3 बार गलत जवाब, रिक्वेस्ट रद्द।",
 "r_GATE_TIMEOUT": "यूज़र ने समय पर वेरिफाई/जॉइन नहीं किया, रिक्वेस्ट रद्द।",
 "r_unknown": "अनजान कारण: {rid}",
 "prem_got": "💎 आपको **Premium** मिल गया है: {d} दिन।\nनए फीचर: कैप्चा, Force Subscribe, पुरानी रिक्वेस्ट approve।",
 "prem_end": "⌛ आपका Premium खत्म हो गया। Premium फीचर बंद हो गए हैं। दोबारा लेने के लिए 💎 प्लान देखो।",
},
"en": {
 "back": "🔙 Back", "refresh": "🔄 Refresh", "my_chats": "📺 My Chats", "add_chat": "➕ Add Chat",
 "old_help_btn": "ℹ️ Old Requests Help", "plan_btn": "💎 Plan", "lang_btn": "🌐 Language: English",
 "plan_free": "Free", "plan_prem": "Premium ({d} days left)", "plan_prem_owner": "Premium (Admin)",
 "user_main": "**🤖 Auto Join Request Bot**\n\nGet your channel/group join requests approved automatically.\n\n"
              "1️⃣ Make the bot an **admin** in your chat (with \"Invite Users\" permission)\n"
              "2️⃣ The chat is added automatically, or tap ➕ Add Chat\n3️⃣ Change settings in 📺 My Chats\n\n"
              "💎 Plan: **{plan}** | Chats: {n}/{lim}\nApproved: {tot}",
 "private_bot": "🔒 This bot is private right now.\nYour ID: `{uid}`",
 "banned": "🚫 You have been banned from this bot.",
 "chats_hint": "**📺 Chats ({n})**\nTap a chat to open its settings.",
 "add_prompt": "Send the chat ID (`-100...`), @username, or forward any message from the channel/group.\n"
               "The bot must be an admin there, and you must be an admin of that chat too.",
 "chat_text": "**{title}**\n`{cid}`\n\nApprove: {on}\nDelay: {lo}-{hi}s {dt}\nWelcome DM: {w}\n"
              "🛡 Captcha: {cap}\n📢 Force Subscribe: {fs} channel(s)\n🌐 DM language: {cl}\n"
              "Approved: {ap} | Failed records: {nf}\nOld task: {old}",
 "custom": "(custom)", "global": "(global)", "running": "running ▶️", "idle": "idle",
 "b_approve": "Approve {s}", "b_delay": "⏱ Custom Delay", "b_reset": "♻️ Reset", "b_wdm": "Welcome DM {s}",
 "b_wtext": "✏️ Welcome Text", "b_cap": "🛡 Captcha: {m}{lock}", "b_fs": "📢 Force Subscribe ({n}){lock}",
 "b_cl": "🌐 DM language: {l}", "b_old": "🕰 Approve old requests{lock}", "b_oldstop": "⏹ Stop old approve",
 "b_fail": "❌ Failed Report", "b_help": "ℹ️ Helper info", "b_rm": "🗑 Remove",
 "cap_off": "Off", "cap_human": "Human button", "cap_math": "Math", "lang_hi": "हिंदी", "lang_en": "English",
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
 "locked": "💎 This is a Premium feature. Tap 💎 Plan to see details.",
 "max_chats": "Your plan allows at most {lim} chats. Get Premium for more (💎 Plan).",
 "plan_text": "**💎 Plan**\n\nYour plan: **{plan}**\n\n**Free**\n• Up to {fc} chats\n• Auto approve, delay, welcome DM, failed report\n\n"
              "**Premium**\n• Up to {pc} chats\n• 🛡 Captcha (human button / math)\n• 📢 Force Subscribe\n• 🕰 Approve old requests\n\n"
              "Contact for Premium: {contact}",
 "e_notfound": "Chat not found. First make the bot an admin there, then send the ID/@username or a forwarded message.",
 "e_notadmin_user": "You don't seem to be an admin of this chat.",
 "e_onlyadmin": "Only the chat's admins/owner can add it.",
 "e_bot_not_in": "The bot isn't in this chat. Add the bot and make it admin first.",
 "e_bot_not_admin": "The bot isn't an admin in this chat. Make it admin first.",
 "e_bot_noinvite": "Give the bot the \"Invite Users\" (Add Members) permission.",
 "e_delay": "Keep the delay between 1 and 300 seconds.",
 "e_tpl": "Use only {{name}} and {{chat}} in the text (no other {{ }}).",
 "e_nolink": "This channel has no public username and the bot couldn't create an invite link. Give the bot \"Invite Users\" permission.",
 "added": "✅ Added: {title}",
 "member_added": "✅ **{title}** added!{warn}\n\n{note}\n\nFor settings: /settings → 📺 My Chats",
 "warn_noinvite": "\n\n⚠️ The bot lacks \"Invite Users\" permission, so requests won't be approved.",
 "bot_removed": "⚠️ The bot was removed from **{title}**; requests there will no longer be approved.",
 "bot_demoted": "⚠️ The bot's admin rights were removed in **{title}**; requests will no longer be approved.",
 "cd_prompt": "Send this chat's delay: `min max` (e.g. `3 12`)",
 "ce_prompt": "Send the new welcome DM text for this chat. You can use {{name}} and {{chat}}.",
 "saved_w": "✅ This chat's welcome DM saved + ON",
 "q_no_access": "You don't have access to this chat.", "q_owner_only": "This option is admin-only.",
 "q_removed": "Removed", "q_clear": "Report cleared", "q_stopping": "Stopping...", "q_started": "Old approve started ▶️",
 "fs_title": "**📢 Force Subscribe — {title}**\n\nUsers must join these channels before their request is approved (max 3). "
             "The bot must be an **admin** in those channels.\n\n{items}",
 "fs_empty": "No channels added yet.", "fs_add": "➕ Add channel", "fs_max": "You can add at most 3 channels.",
 "fs_ask": "Send the channel's ID, @username, or forward any message from it.",
 "cap_msg_human": "🛡 You sent a join request to **{chat}**.\n\nTap the button below to verify. (within {m} min)",
 "human_btn": "✅ I am human",
 "cap_msg_math": "🛡 You sent a join request to **{chat}**.\n\nPick the correct answer to verify:\n\n**{q} = ?**\n\n(within {m} min)",
 "cap_wrong": "❌ Wrong answer. Here's a new question.",
 "cap_failed": "❌ Verification failed. Your request was cancelled; you can send a new request if you like.",
 "cap_ok": "✅ Verified! Your request is being approved.",
 "gate_expired": "⌛ Time's up. Please send the join request again.",
 "fs_msg": "📢 To get approved in **{chat}**, first join these channels, then tap \"I have joined\".",
 "joined_btn": "✅ I have joined", "not_joined": "You haven't joined all the channels yet.",
 "fs_ok": "✅ Great! Your request is being approved.",
 "r_USER_CHANNELS_TOO_MUCH": "The user is at Telegram's 500 channels/groups limit, so they can't join any more. (Nothing can be done)",
 "r_USER_DEACTIVATED": "Account is deleted/deactivated.",
 "r_USER_DEACTIVATED_BAN": "Account was banned by Telegram.",
 "r_HIDE_REQUESTER_MISSING": "The request no longer exists: the user withdrew it or another admin already handled it.",
 "r_USER_ALREADY_PARTICIPANT": "User is already a member.",
 "r_USER_BANNED_IN_CHANNEL": "User is banned in this chat. Unban first.",
 "r_USER_PRIVACY_RESTRICTED": "Can't be added because of the user's privacy settings.",
 "r_USER_ID_INVALID": "User not found (deleted/invalid).",
 "r_CHAT_ADMIN_REQUIRED": "Missing admin permission. Give \"Invite Users\" permission.",
 "r_CAPTCHA_DM_FAILED": "The captcha message couldn't be delivered to the user.",
 "r_CAPTCHA_FAILED": "3 wrong captcha answers, request cancelled.",
 "r_GATE_TIMEOUT": "The user didn't verify/join in time, request cancelled.",
 "r_unknown": "Unknown reason: {rid}",
 "prem_got": "💎 You've got **Premium**: {d} days.\nNew features: Captcha, Force Subscribe, old requests approve.",
 "prem_end": "⌛ Your Premium has ended. Premium features are now off. See 💎 Plan to renew.",
},
}
REASON_EMOJI = {"USER_CHANNELS_TOO_MUCH": "🚫", "USER_DEACTIVATED": "👻", "USER_DEACTIVATED_BAN": "⛔",
                "HIDE_REQUESTER_MISSING": "↩️", "USER_ALREADY_PARTICIPANT": "✔️", "USER_BANNED_IN_CHANNEL": "🔨",
                "USER_PRIVACY_RESTRICTED": "🔒", "USER_ID_INVALID": "❓", "CHAT_ADMIN_REQUIRED": "🔑",
                "CAPTCHA_DM_FAILED": "📭", "CAPTCHA_FAILED": "🛡", "GATE_TIMEOUT": "⌛"}
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


class UserError(Exception):
    """User ko seedha dikhne wala saaf error."""


def mention(name, uid):
    return f"[{clean(name or uid)}](tg://user?id={uid})"

def helper_note(lang="hi"):
    if not helper:
        return tx(lang, "helper_none")
    uname = f" (@{helper['username']})" if helper.get("username") else ""
    return tx(lang, "helper_note", who=mention(helper["name"], helper["id"]), uname=uname, hid=helper["id"])


def _owner(_, __, m):
    return bool(m.from_user) and m.from_user.id in OWNERS
def _user(_, __, m):
    if not m.from_user or m.from_user.id in BANNED:
        return False
    return m.from_user.id in OWNERS or STATE["public"]
is_owner = filters.create(_owner)
is_user = filters.create(_user)


# ======================= logging / alerts =======================
async def notify_admin(text: str, important: bool = False):
    """Log channel me bhejta hai; important ho aur log channel na ho/fail ho to owners ko DM."""
    sent = False
    if LOG["chat"]:
        try:
            await bot.send_message(LOG["chat"], text)
            sent = True
        except Exception as e:
            log.warning("log channel send fail: %s", e)
    if important and not sent:
        for oid in OWNERS:
            try:
                await bot.send_message(oid, text)
            except Exception:
                pass

def safe(fn):
    @functools.wraps(fn)
    async def w(client, update, *a, **k):
        try:
            return await fn(client, update, *a, **k)
        except Exception as e:
            target = update.message if isinstance(update, CallbackQuery) else update
            uid = update.from_user.id if getattr(update, "from_user", None) else 0
            if isinstance(e, UserError):
                text = f"❌ {e}"
            else:
                log.exception("handler error in %s", fn.__name__)
                text = (f"❌ **Error** (`{fn.__name__}`)\n`{type(e).__name__}: {str(e)[:600]}`"
                        if uid in OWNERS else "❌ Something went wrong / कुछ गड़बड़ हुई, try again later.")
                await notify_admin(f"⚠️ Error in `{fn.__name__}` (user `{uid}`)\n`{type(e).__name__}: {str(e)[:300]}`", important=True)
            try:
                await target.reply(text)
            except Exception:
                pass
    return w


# ======================= settings / users / plans =======================
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

async def is_premium(uid) -> bool:
    if uid is None or uid in OWNERS:
        return True
    u = await db.users.find_one({"_id": uid})
    return bool(u) and u.get("premium_until", 0) > time.time()

async def plan_status(uid: int):
    """(premium?, bache din)"""
    if uid in OWNERS:
        return True, 0
    u = await db.users.find_one({"_id": uid}) or {}
    left = u.get("premium_until", 0) - time.time()
    return (left > 0), max(0, math.ceil(left / 86400))

async def max_chats(uid: int) -> int:
    if uid in OWNERS:
        return 10 ** 6
    return PREMIUM_CHATS if await is_premium(uid) else FREE_CHATS

async def chat_is_premium(c: dict) -> bool:
    return await is_premium(c.get("owner_id"))

async def set_premium(uid: int, days: int):
    now = time.time()
    if days <= 0:
        await db.users.update_one({"_id": uid}, {"$unset": {"premium_until": ""}, "$set": {"exp_notified": True}}, upsert=True)
        return None
    u = await db.users.find_one({"_id": uid}) or {}
    until = max(now, u.get("premium_until", 0)) + days * 86400
    await db.users.update_one({"_id": uid}, {"$set": {"premium_until": until, "exp_notified": False}}, upsert=True)
    return until

async def set_ban(uid: int, flag: bool):
    if flag:
        BANNED.add(uid)
    else:
        BANNED.discard(uid)
    await db.users.update_one({"_id": uid}, {"$set": {"banned": flag}}, upsert=True)

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
    oid = c.get("owner_id")
    if oid is not None and oid in BANNED:
        return None
    prem = await is_premium(oid)       # Premium khatam => captcha/force-sub apne-aap band
    w_on, w_text = welcome_of(c, g)
    return {"min": c.get("min", g["min"]), "max": c.get("max", g["max"]),
            "welcome_on": w_on, "welcome": w_text,
            "captcha": c.get("captcha", "off") if prem else "off",
            "fsub": c.get("fsub", []) if prem else [],
            "lang": c.get("lang", "hi")}

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

async def clear_fail(cid, uid):
    try:
        await db.fails.delete_one({"_id": f"{cid}:{uid}"})
    except Exception:
        pass

async def touch_user(u):
    """Bot se baat karne wale har user ka record (sirf admin dekh sakta hai)."""
    if not u or getattr(u, "is_bot", False):
        return
    now = time.time()
    try:
        res = await db.users.update_one(
            {"_id": u.id},
            {"$set": {"first_name": u.first_name, "last_name": u.last_name, "username": u.username, "last_seen": now},
             "$setOnInsert": {"started": now}}, upsert=True)
        if getattr(res, "upserted_id", None) is not None:
            name = " ".join(x for x in (u.first_name, u.last_name) if x) or "NoName"
            await notify_admin(f"👤 Naya user: {mention(name, u.id)} "
                               f"{('@' + u.username) if u.username else ''} `{u.id}`")
    except Exception:
        log.exception("touch_user")


# ======================= gates: captcha + force-subscribe =======================
def make_math():
    a, b = random.randint(2, 9), random.randint(1, 9)
    if random.random() < 0.5:
        ans, q = a + b, f"{a} + {b}"
    else:
        a, b = max(a, b), min(a, b)
        ans, q = a - b, f"{a} - {b}"
    opts = {ans}
    while len(opts) < 4:
        opts.add(max(0, ans + random.choice([-3, -2, -1, 1, 2, 3, 4])))
    opts = list(opts)
    random.shuffle(opts)
    return q, ans, opts

async def fsub_missing(entries: list, uid: int) -> list:
    out = []
    for e in entries:
        try:
            m = await bot.get_chat_member(e["id"], uid)
            if m.status in (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED):
                out.append(e)
        except Exception as ex:
            if reason_of(ex) == "USER_NOT_PARTICIPANT" or type(ex).__name__ == "UserNotParticipant":
                out.append(e)
            else:  # check nahi ho paya (bot admin nahi?) => user ko mat rokna
                log.warning("fsub check fail %s: %s", e.get("id"), ex)
    return out

async def start_gates(cid, title, uid, name, uname, cfg):
    if cfg["captcha"] != "off":
        return await send_captcha(cid, title, uid, name, uname, cfg)
    if cfg["fsub"]:
        return await send_fsub(cid, title, uid, name, uname, cfg)
    await queue.put((cid, title, uid, name, uname, cfg))

async def gate_fail(cid, uid, name, uname, rid, decline=True):
    await db.gate.delete_one({"_id": f"{cid}:{uid}"})
    if decline:
        try:
            await bot.decline_chat_join_request(cid, uid)
        except Exception:
            pass
    await record_fail(cid, uid, name, uname, rid, "new")

async def send_captcha(cid, title, uid, name, uname, cfg, edit=None):
    lang, mode = cfg["lang"], cfg["captcha"]
    doc = {"chat_id": cid, "user_id": uid, "name": name, "username": uname, "title": title,
           "stage": "captcha", "tries": 0, "ts": time.time()}
    if mode == "math":
        q, ans, opts = make_math()
        doc["ans"] = ans
        text = tx(lang, "cap_msg_math", chat=clean(title), q=q, m=GATE_MINUTES)
        kb = [[B(str(o), callback_data=f"hm:{cid}:{o}") for o in opts]]
    else:
        text = tx(lang, "cap_msg_human", chat=clean(title), m=GATE_MINUTES)
        kb = [[B(tx(lang, "human_btn"), callback_data=f"hv:{cid}")]]
    await db.gate.update_one({"_id": f"{cid}:{uid}"}, {"$set": doc}, upsert=True)
    try:
        if edit is not None:
            await edit.edit_text(text, reply_markup=InlineKeyboardMarkup(kb))
        else:
            await bot.send_message(uid, text, reply_markup=InlineKeyboardMarkup(kb))
    except Exception as e:
        log.warning("captcha dm fail %s/%s: %s", cid, uid, e)
        await db.gate.delete_one({"_id": f"{cid}:{uid}"})
        await record_fail(cid, uid, name, uname, "CAPTCHA_DM_FAILED", "new")

async def send_fsub(cid, title, uid, name, uname, cfg, edit=None):
    lang = cfg["lang"]
    missing = await fsub_missing(cfg["fsub"], uid)
    if not missing:
        await db.gate.delete_one({"_id": f"{cid}:{uid}"})
        if edit is not None:
            try:
                await edit.edit_text(tx(lang, "cap_ok"))
            except Exception:
                pass
        await queue.put((cid, title, uid, name, uname, cfg))
        return
    doc = {"chat_id": cid, "user_id": uid, "name": name, "username": uname, "title": title,
           "stage": "fsub", "tries": 0, "ts": time.time()}
    await db.gate.update_one({"_id": f"{cid}:{uid}"}, {"$set": doc}, upsert=True)
    kb = [[B(f"📢 {e.get('title') or e['id']}"[:60], url=e["link"])] for e in missing]
    kb.append([B(tx(lang, "joined_btn"), callback_data=f"hf:{cid}")])
    text = tx(lang, "fs_msg", chat=clean(title))
    try:
        if edit is not None:
            await edit.edit_text(text, reply_markup=InlineKeyboardMarkup(kb))
        else:
            await bot.send_message(uid, text, reply_markup=InlineKeyboardMarkup(kb))
    except Exception as e:
        log.warning("fsub dm fail %s/%s: %s", cid, uid, e)
        await db.gate.delete_one({"_id": f"{cid}:{uid}"})
        await record_fail(cid, uid, name, uname, "CAPTCHA_DM_FAILED", "new")

@bot.on_callback_query(filters.regex(r"^h[vmf]:"))
@safe
async def gate_cb(_, q: CallbackQuery):
    """Join-request karne wale user ke buttons (captcha / force-sub). Sab ke liye khula."""
    act, _, rest = q.data.partition(":")
    parts = rest.split(":")
    cid, uid = int(parts[0]), q.from_user.id
    key = f"{cid}:{uid}"
    g = await db.gate.find_one({"_id": key})
    cfg = await effective(cid)
    lang = (cfg or {}).get("lang", "hi")
    if not g or cfg is None or time.time() - g["ts"] > GATE_MINUTES * 60:
        await db.gate.delete_one({"_id": key})
        return await q.answer(tx(lang, "gate_expired"), show_alert=True)
    name, uname, title = g.get("name"), g.get("username"), g.get("title")

    async def finish():
        """Captcha pass => agar force-sub hai to wo, warna approve."""
        if cfg["fsub"]:
            return await send_fsub(cid, title, uid, name, uname, cfg, edit=q.message)
        await db.gate.delete_one({"_id": key})
        await queue.put((cid, title, uid, name, uname, cfg))
        try:
            await q.message.edit_text(tx(lang, "cap_ok"))
        except Exception:
            pass

    if act == "hv" and g["stage"] == "captcha":
        await q.answer()
        return await finish()
    if act == "hm" and g["stage"] == "captcha":
        if int(parts[1]) == g.get("ans"):
            await q.answer()
            return await finish()
        tries = g.get("tries", 0) + 1
        if tries >= 3:
            await gate_fail(cid, uid, name, uname, "CAPTCHA_FAILED")
            await q.answer()
            try:
                await q.message.edit_text(tx(lang, "cap_failed"))
            except Exception:
                pass
            return
        await q.answer(tx(lang, "cap_wrong"), show_alert=True)
        qq, ans, opts = make_math()
        await db.gate.update_one({"_id": key}, {"$set": {"ans": ans, "tries": tries}})
        kb = [[B(str(o), callback_data=f"hm:{cid}:{o}") for o in opts]]
        try:
            await q.message.edit_text(tx(lang, "cap_msg_math", chat=clean(title), q=qq, m=GATE_MINUTES),
                                      reply_markup=InlineKeyboardMarkup(kb))
        except Exception:
            pass
        return
    if act == "hf" and g["stage"] == "fsub":
        if await fsub_missing(cfg["fsub"], uid):
            return await q.answer(tx(lang, "not_joined"), show_alert=True)
        await q.answer()
        await db.gate.delete_one({"_id": key})
        await queue.put((cid, title, uid, name, uname, cfg))
        try:
            await q.message.edit_text(tx(lang, "fs_ok"))
        except Exception:
            pass
        return
    await q.answer(tx(lang, "gate_expired"), show_alert=True)


# ======================= new requests =======================
@bot.on_chat_join_request()
async def on_request(_, r: ChatJoinRequest):
    cfg = await effective(r.chat.id)
    if cfg is None:
        return
    await db.chats.update_one({"_id": r.chat.id}, {"$setOnInsert": {"title": r.chat.title}}, upsert=True)
    await start_gates(r.chat.id, r.chat.title, r.from_user.id, r.from_user.first_name, r.from_user.username, cfg)

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


# ======================= helper (user) account =======================
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

async def helper_died(why: str):
    await stop_user()
    await db.settings.delete_one({"_id": "session"})
    await notify_admin(f"🚨 Helper session band ho gaya: `{why}`\nPurani requests tab tak nahi chalengi jab tak "
                       "/settings → 🔑 Old-Req Session se dobara login na karo.", important=True)

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
                        if rid in AUTH_DEAD:
                            raise
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
        if reason_of(e) in AUTH_DEAD:
            await helper_died(reason_of(e))
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

async def finish_login(m: Message, uid: int):
    lg = logins.pop(uid)
    sess = await lg["c"].export_session_string()
    await lg["c"].disconnect()
    pending.pop(uid, None)
    await db.settings.update_one({"_id": "session"}, {"$set": {"string": sess}}, upsert=True)
    await start_user(sess)
    await m.reply("✅ Login ho gaya! Session save ho gaya, Render me kuch badalna nahi hai.")
    await show(m, *await session_panel())


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
    if uid in OWNERS:
        pub = STATE["public"]
        text = (f"**⚙️ Auto Join Request Bot (Admin)**\n\n"
                f"Mode: {'🌐 Public' if pub else '🔒 Private'}\n"
                f"Delay: {g['min']}-{g['max']}s | Queue: {queue.qsize()}\n"
                f"Total approved: {g.get('total', 0)}\n"
                f"Helper session: {'✅ ' + clean(helper['name']) if helper else '❌'}\n"
                f"Log channel: {'✅' if LOG['chat'] else '❌'} | Banned: {len(BANNED)}\n\n"
                "Public mode me Auto-all OFF rakhna behtar hai.")
        kb = [[B(f"Bot Active {tick(g['active'])}", callback_data="g:active")],
              [B(f"Auto-all chats {tick(g['auto_all'])}", callback_data="g:auto_all")],
              [B(f"Welcome DM (global) {tick(g['welcome_on'])}", callback_data="g:welcome_on")],
              [B("📺 Chats", callback_data="chats"), B("⏱ Delay", callback_data="delay")],
              [B("👋 Welcome Text", callback_data="wt"), B("🔑 Old-Req Session", callback_data="ses")],
              [B("📣 Log Channel", callback_data="lgp"), B(tx(lang, "lang_btn"), callback_data="lang")],
              [B(f"Bot Mode: {'🌐 Public' if pub else '🔒 Private'} (badlo)", callback_data="pub")],
              [B("👥 Users", callback_data="us:0"), B("🔄 Refresh", callback_data="main")]]
    else:
        n, tot = 0, 0
        async for c in db.chats.find({"owner_id": uid}):
            n += 1
            tot += c.get("approved", 0)
        prem, left = await plan_status(uid)
        plan = tx(lang, "plan_prem", d=left) if prem else tx(lang, "plan_free")
        text = tx(lang, "user_main", plan=plan, n=n, lim=await max_chats(uid), tot=tot)
        kb = [[B(tx(lang, "my_chats"), callback_data="chats"), B(tx(lang, "add_chat"), callback_data="add")],
              [B(tx(lang, "plan_btn"), callback_data="plan"), B(tx(lang, "old_help_btn"), callback_data="oh")],
              [B(tx(lang, "lang_btn"), callback_data="lang"), B(tx(lang, "refresh"), callback_data="main")]]
    return text, InlineKeyboardMarkup(kb)

async def plan_panel(uid: int):
    lang = await lang_of(uid)
    prem, left = await plan_status(uid)
    plan = tx(lang, "plan_prem_owner") if uid in OWNERS else (tx(lang, "plan_prem", d=left) if prem else tx(lang, "plan_free"))
    contact = ADMIN_CONTACT or (mention("Admin", sorted(OWNERS)[0]) if OWNERS else "-")
    return (tx(lang, "plan_text", plan=plan, fc=FREE_CHATS, pc=PREMIUM_CHATS, contact=contact),
            InlineKeyboardMarkup([[B(tx(lang, "back"), callback_data="main")]]))

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
    prem = await chat_is_premium(c)
    lock = "" if prem else " 💎"
    on = c.get("enabled", True)
    lo, hi = c.get("min", g["min"]), c.get("max", g["max"])
    w_on, _w = welcome_of(c, g)
    running = cid in old_tasks
    nfail = await db.fails.count_documents({"chat_id": cid})
    cap = c.get("captcha", "off")
    cl = c.get("lang", "hi")
    fs_n = len(c.get("fsub", []))
    text = tx(lang, "chat_text", title=clean(c.get("title", cid)), cid=cid, on=tick(on), lo=lo, hi=hi,
              dt=tx(lang, "custom") if "min" in c else tx(lang, "global"), w=tick(w_on),
              cap=tx(lang, f"cap_{cap}") + lock, fs=fs_n, cl=tx(lang, f"lang_{cl}"),
              ap=c.get("approved", 0), nf=nfail, old=tx(lang, "running") if running else tx(lang, "idle"))
    kb = InlineKeyboardMarkup([
        [B(tx(lang, "b_approve", s=tick(on)), callback_data=f"ct:{cid}")],
        [B(tx(lang, "b_delay"), callback_data=f"cd:{cid}"), B(tx(lang, "b_reset"), callback_data=f"cr:{cid}")],
        [B(tx(lang, "b_wdm", s=tick(w_on)), callback_data=f"cw:{cid}"), B(tx(lang, "b_wtext"), callback_data=f"ce:{cid}")],
        [B(tx(lang, "b_cap", m=tx(lang, f"cap_{cap}"), lock=lock), callback_data=f"cc:{cid}")],
        [B(tx(lang, "b_fs", n=fs_n, lock=lock), callback_data=f"fs:{cid}")],
        [B(tx(lang, "b_cl", l=tx(lang, f"lang_{cl}")), callback_data=f"cl:{cid}")],
        [B(tx(lang, "b_oldstop"), callback_data=f"cs:{cid}") if running
         else B(tx(lang, "b_old", lock=lock), callback_data=f"co:{cid}")],
        [B(tx(lang, "b_fail"), callback_data=f"fr:{cid}"), B(tx(lang, "b_help"), callback_data="oh")],
        [B(tx(lang, "b_rm"), callback_data=f"cx:{cid}"), B(tx(lang, "back"), callback_data="chats")]])
    return text, kb

async def fsub_panel(cid: int, uid: int):
    lang = await lang_of(uid)
    c = await ccfg(cid) or {}
    ents = c.get("fsub", [])
    items = "\n".join(f"{i + 1}. {clean(e.get('title') or e['id'])} (`{e['id']}`)" for i, e in enumerate(ents)) or tx(lang, "fs_empty")
    rows = [[B(f"🗑 {clean(e.get('title') or e['id'])[:30]}", callback_data=f"fd:{cid}:{i}")] for i, e in enumerate(ents)]
    rows.append([B(tx(lang, "fs_add"), callback_data=f"fa:{cid}")])
    rows.append([B(tx(lang, "back"), callback_data=f"c:{cid}")])
    return tx(lang, "fs_title", title=clean(c.get("title", cid)), items=items), InlineKeyboardMarkup(rows)

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

async def helper_panel(uid: int):
    lang = await lang_of(uid)
    return (helper_note(lang) + tx(lang, "helper_tail"),
            InlineKeyboardMarkup([[B(tx(lang, "back"), callback_data="main")]]))

def log_panel():
    text = ("**📣 Log Channel**\n\nNaye users, naye chats, Premium/ban changes aur errors yahan aate hain.\n"
            f"Abhi: {('`' + str(LOG['chat']) + '` ✅') if LOG['chat'] else 'set nahi ❌ (important alerts aapko DM aayenge)'}\n\n"
            "Bot ko us channel/group me admin banao (post karne ki permission), phir Set dabao.")
    kb = InlineKeyboardMarkup([
        [B("✏️ Set Log Chat", callback_data="lgs"), B("🧪 Test", callback_data="lgt")],
        [B("🗑 Remove", callback_data="lgx"), B("🔙 Back", callback_data="main")]])
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
        badge = ("👑 " if u["_id"] in OWNERS else "") + ("🚫 " if u.get("banned") else "")
        left = u.get("premium_until", 0) - now
        plan = f"💎 Premium ({math.ceil(left / 86400)}d)" if left > 0 else "Free"
        lines.append(f"**{i}.** {badge}{mention(full, u['_id'])}\n"
                     f"   {uname} | ID: `{u['_id']}`\n"
                     f"   Start: {fmt_ts(u.get('started'))}\n"
                     f"   Last: {fmt_ts(u.get('last_seen'))}\n"
                     f"   Plan: {plan} | Chats: {n_chats} | Approved: {appr}")
    head = (f"**👥 Users**\nTotal: {total} | Naye (24h): {new24} | Active (24h): {act24}\n"
            f"Page {page + 1}/{pages}\n\n")
    text = (head + ("\n\n".join(lines) if lines else "Abhi koi user record nahi hai."))[:4000]
    nav = []
    if page > 0:
        nav.append(B("⬅️ Back", callback_data=f"us:{page - 1}"))
    nav.append(B(f"{page + 1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(B("Next ➡️", callback_data=f"us:{page + 1}"))
    return text, InlineKeyboardMarkup([
        nav,
        [B("🚫 Ban/Unban ID", callback_data="bn"), B("📋 Banned", callback_data="bl")],
        [B("💎 Premium Do/Hatao", callback_data="pa"), B("📋 Premiums", callback_data="pl")],
        [B("🔙 Menu", callback_data="main")]])

async def list_panel(kind: str):
    now = time.time()
    if kind == "ban":
        docs = [u async for u in db.users.find({"banned": True}).limit(40)]
        title = "🚫 Banned users"
    else:
        docs = [u async for u in db.users.find({"premium_until": {"$gt": now}}).limit(40)]
        title = "💎 Premium users"
    lines = []
    for u in docs:
        full = " ".join(x for x in (u.get("first_name"), u.get("last_name")) if x) or "NoName"
        extra = ""
        if kind != "ban":
            extra = f" — {math.ceil((u['premium_until'] - now) / 86400)} din baaki"
        lines.append(f"• {mention(full, u['_id'])} `{u['_id']}`{extra}")
    text = f"**{title} ({len(docs)})**\n\n" + ("\n".join(lines) or "Koi nahi.")
    return text[:4000], InlineKeyboardMarkup([[B("🔙 Users", callback_data="us:0")]])


# ======================= commands =======================
@bot.on_message(filters.command(["start", "settings", "help"]) & filters.private)
@safe
async def start(_, m: Message):
    uid = m.from_user.id
    await touch_user(m.from_user)
    lang = await lang_of(uid)
    if uid in BANNED:
        return await m.reply(tx(lang, "banned"))
    if uid not in OWNERS and not STATE["public"]:
        return await m.reply(tx(lang, "private_bot", uid=uid))
    await cancel_login(uid)
    await show(m, *await main_panel(uid))

@bot.on_message(filters.command("plan") & filters.private & is_user)
@safe
async def cmd_plan(_, m: Message):
    await show(m, *await plan_panel(m.from_user.id))

@bot.on_message(filters.command("id") & filters.private)
async def my_id(_, m: Message):
    uid = m.from_user.id
    extra = f"\nOWNER_IDS: `{sorted(OWNERS)}`\nOwner match: {uid in OWNERS}" if uid in OWNERS or not OWNERS else ""
    await m.reply(f"ID: `{uid}`{extra}")

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
                  f"Mode: {'🌐 Public' if STATE['public'] else '🔒 Private'}\nLog: {LOG['chat'] or '❌'}")

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

async def do_premium(uid: int, days: int, by: int):
    until = await set_premium(uid, days)
    lang = await lang_of(uid)
    if until:
        d = max(1, math.ceil((until - time.time()) / 86400))
        try:
            await bot.send_message(uid, tx(lang, "prem_got", d=days))
        except Exception:
            pass
        await notify_admin(f"💎 Premium diya: `{uid}` +{days} din (kul {d} din) — by `{by}`")
        return f"✅ `{uid}` ko {days} din Premium diya (kul {d} din baaki)."
    await notify_admin(f"💎 Premium hataya: `{uid}` — by `{by}`")
    return f"✅ `{uid}` ka Premium hata diya (Free)."

async def do_ban(uid: int, flag: bool, by: int):
    if uid in OWNERS:
        raise UserError("Admin ko ban nahi kar sakte.")
    await set_ban(uid, flag)
    await notify_admin(f"{'🚫 Ban' if flag else '✅ Unban'}: `{uid}` — by `{by}`")
    return f"{'🚫 Ban' if flag else '✅ Unban'} ho gaya: `{uid}`"

@bot.on_message(filters.command("premium") & filters.private & is_owner)
@safe
async def cmd_premium(_, m: Message):
    if len(m.command) < 3:
        raise UserError("Usage: /premium <user_id> <days>")
    await m.reply(await do_premium(int(m.command[1]), int(m.command[2]), m.from_user.id))

@bot.on_message(filters.command("free") & filters.private & is_owner)
@safe
async def cmd_free(_, m: Message):
    await m.reply(await do_premium(int(m.command[1]), 0, m.from_user.id))

@bot.on_message(filters.command("ban") & filters.private & is_owner)
@safe
async def cmd_ban(_, m: Message):
    await m.reply(await do_ban(int(m.command[1]), True, m.from_user.id))

@bot.on_message(filters.command("unban") & filters.private & is_owner)
@safe
async def cmd_unban(_, m: Message):
    await m.reply(await do_ban(int(m.command[1]), False, m.from_user.id))

@bot.on_message(filters.command("banned") & filters.private & is_owner)
@safe
async def cmd_banned(_, m: Message):
    await show(m, *await list_panel("ban"))

@bot.on_message(filters.command("premiums") & filters.private & is_owner)
@safe
async def cmd_premiums(_, m: Message):
    await show(m, *await list_panel("prem"))

@bot.on_message(filters.command("approveold") & filters.private & is_owner)
@safe
async def cmd_old(_, m: Message):
    cid = int(m.command[1])
    err = await old_precheck(cid, await lang_of(m.from_user.id))
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


# ======================= chat add / membership =======================
async def check_admin(uid: int, chat, lang: str, need_invite: bool):
    if uid not in OWNERS:
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
    if need_invite and not (me.privileges and me.privileges.can_invite_users):
        raise UserError(tx(lang, "e_bot_noinvite"))

async def verify_add(uid: int, chat, lang: str):
    if uid not in OWNERS:
        lim = await max_chats(uid)
        if await db.chats.count_documents({"owner_id": uid, "_id": {"$ne": chat.id}}) >= lim:
            raise UserError(tx(lang, "max_chats", lim=lim))
    await check_admin(uid, chat, lang, need_invite=True)

@bot.on_chat_member_updated()
async def on_member(_, u):
    """Bot ko admin banate hi chat user ke naam register ho jata hai; hataye jaane par alert."""
    try:
        new, old = u.new_chat_member, u.old_chat_member
        if not new or not new.user or new.user.id != BOT_ID["id"]:
            return
        c = await ccfg(u.chat.id) or {}
        oid = c.get("owner_id")
        lost = new.status in (ChatMemberStatus.LEFT, ChatMemberStatus.BANNED)
        demoted = (old is not None and old.status == ChatMemberStatus.ADMINISTRATOR
                   and new.status in (ChatMemberStatus.MEMBER, ChatMemberStatus.RESTRICTED))
        if lost or demoted:
            key = "bot_removed" if lost else "bot_demoted"
            if oid:
                try:
                    await bot.send_message(oid, tx(await lang_of(oid), key, title=clean(u.chat.title)))
                except Exception:
                    pass
            await notify_admin(f"⚠️ Bot {'hataya gaya' if lost else 'demote hua'}: {clean(u.chat.title)} `{u.chat.id}`"
                               f" (owner `{oid}`)", important=bool(oid is None))
            return
        if new.status != ChatMemberStatus.ADMINISTRATOR or not u.from_user:
            return
        uid = u.from_user.id
        lang = await lang_of(uid)
        if uid not in OWNERS:
            if not STATE["public"] or uid in BANNED:
                return
            lim = await max_chats(uid)
            if await db.chats.count_documents({"owner_id": uid, "_id": {"$ne": u.chat.id}}) >= lim:
                await bot.send_message(uid, tx(lang, "max_chats", lim=lim))
                return
        await register_chat(u.chat, uid)
        c = await ccfg(u.chat.id) or {}
        if uid not in OWNERS and c.get("owner_id") != uid:
            return
        warn = "" if (new.privileges and new.privileges.can_invite_users) else tx(lang, "warn_noinvite")
        await notify_admin(f"📺 Naya chat: {clean(u.chat.title)} `{u.chat.id}` — user `{uid}`")
        await bot.send_message(uid, tx(lang, "member_added", title=clean(u.chat.title), warn=warn, note=helper_note(lang)))
    except Exception as e:
        log.warning("on_member: %s", e)


# ======================= callbacks =======================
OWNER_ACTS = {"us", "g", "ses", "lg", "ls", "lo", "delay", "dp", "dc", "wt", "pub",
              "lgp", "lgs", "lgt", "lgx", "bn", "bl", "pa", "pl"}
CHAT_ACTS = {"c", "ct", "cr", "cd", "co", "cs", "cx", "cw", "ce", "fr", "fc", "fq", "cc", "cl", "fs", "fa", "fd"}

@bot.on_callback_query(is_user)
@safe
async def cb(_, q: CallbackQuery):
    act, _, arg = q.data.partition(":")
    uid = q.from_user.id
    await touch_user(q.from_user)
    lang = await lang_of(uid)
    try:
        if act in OWNER_ACTS and uid not in OWNERS:
            return await q.answer(tx(lang, "q_owner_only"), show_alert=True)
        if act in CHAT_ACTS and not await can_manage(uid, int(arg.split(":")[0])):
            return await q.answer(tx(lang, "q_no_access"), show_alert=True)

        if act == "noop":
            return
        if act == "main":
            await cancel_login(uid)
            return await show(q, *await main_panel(uid))
        if act == "lang":
            await set_lang(uid, "en" if lang == "hi" else "hi")
            return await show(q, *await main_panel(uid))
        if act == "plan":
            return await show(q, *await plan_panel(uid))
        if act == "oh":
            return await show(q, *await helper_panel(uid))
        if act == "chats":
            await cancel_login(uid)
            return await show(q, *await chats_panel(uid))
        if act == "p":
            return await show(q, *await chats_panel(uid, int(arg)))
        if act == "add":
            pending[uid] = ("add",)
            return await q.message.reply(tx(lang, "add_prompt"))
        # ---------- chat actions
        if act == "c":
            return await show(q, *await chat_panel(int(arg), uid))
        cid = int(arg.split(":")[0]) if act in CHAT_ACTS else 0
        if act == "ct":
            c = await ccfg(cid) or {}
            await db.chats.update_one({"_id": cid}, {"$set": {"enabled": not c.get("enabled", True)}}, upsert=True)
            return await show(q, *await chat_panel(cid, uid))
        if act == "cr":
            await db.chats.update_one({"_id": cid}, {"$unset": {"min": "", "max": ""}})
            return await show(q, *await chat_panel(cid, uid))
        if act == "cd":
            pending[uid] = ("cdelay", cid)
            return await q.message.reply(tx(lang, "cd_prompt"))
        if act == "cw":
            c, g = await ccfg(cid) or {}, await gcfg()
            on, _w = welcome_of(c, g)
            await db.chats.update_one({"_id": cid}, {"$set": {"welcome_on": not on}}, upsert=True)
            return await show(q, *await chat_panel(cid, uid))
        if act == "ce":
            pending[uid] = ("cwelcome", cid)
            return await q.message.reply(tx(lang, "ce_prompt"))
        if act == "cl":
            c = await ccfg(cid) or {}
            await db.chats.update_one({"_id": cid}, {"$set": {"lang": "en" if c.get("lang", "hi") == "hi" else "hi"}}, upsert=True)
            return await show(q, *await chat_panel(cid, uid))
        if act == "cc":
            c = await ccfg(cid) or {}
            if not await chat_is_premium(c):
                return await q.answer(tx(lang, "locked"), show_alert=True)
            modes = ["off", "human", "math"]
            nxt = modes[(modes.index(c.get("captcha", "off")) + 1) % 3]
            await db.chats.update_one({"_id": cid}, {"$set": {"captcha": nxt}}, upsert=True)
            return await show(q, *await chat_panel(cid, uid))
        if act == "fs":
            if not await chat_is_premium(await ccfg(cid) or {}):
                return await q.answer(tx(lang, "locked"), show_alert=True)
            return await show(q, *await fsub_panel(cid, uid))
        if act == "fa":
            if len((await ccfg(cid) or {}).get("fsub", [])) >= 3:
                return await q.answer(tx(lang, "fs_max"), show_alert=True)
            pending[uid] = ("fsadd", cid)
            return await q.message.reply(tx(lang, "fs_ask"))
        if act == "fd":
            c = await ccfg(cid) or {}
            lst = c.get("fsub", [])
            idx = int(arg.split(":")[1])
            if 0 <= idx < len(lst):
                lst.pop(idx)
                await db.chats.update_one({"_id": cid}, {"$set": {"fsub": lst}})
            return await show(q, *await fsub_panel(cid, uid))
        if act == "co":
            if not await chat_is_premium(await ccfg(cid) or {}):
                return await q.answer(tx(lang, "locked"), show_alert=True)
            err = await old_precheck(cid, lang)
            if err:
                return await q.message.reply(err)
            old_tasks[cid] = asyncio.create_task(approve_old(cid, uid))
            await q.answer(tx(lang, "q_started"))
            return await show(q, *await chat_panel(cid, uid))
        if act == "cs":
            t = old_tasks.get(cid)
            if t:
                t.cancel()
            await q.answer(tx(lang, "q_stopping"))
            await asyncio.sleep(1)
            return await show(q, *await chat_panel(cid, uid))
        if act == "cx":
            await db.chats.delete_one({"_id": cid})
            await db.fails.delete_many({"chat_id": cid})
            await q.answer(tx(lang, "q_removed"))
            return await show(q, *await chats_panel(uid))
        if act == "fr":
            return await show(q, *await fails_panel(cid, uid))
        if act == "fc":
            await db.fails.delete_many({"chat_id": cid})
            await q.answer(tx(lang, "q_clear"))
            return await show(q, *await fails_panel(cid, uid))
        if act == "fq":
            pending[uid] = ("whyid", cid)
            return await q.message.reply(tx(lang, "f_ask"))
        # ---------- owner only
        if act == "g":
            g = await gcfg()
            await gset(**{arg: not g[arg]})
            return await show(q, *await main_panel(uid))
        if act == "pub":
            STATE["public"] = not STATE["public"]
            await gset(public=STATE["public"])
            return await show(q, *await main_panel(uid))
        if act == "us":
            return await show(q, *await users_panel(int(arg)))
        if act == "bn":
            pending[uid] = ("ban",)
            return await q.message.reply("Ban/Unban karne wale user ki **ID** bhejo (banned ho to unban ho jayega).")
        if act == "pa":
            pending[uid] = ("prem",)
            return await q.message.reply("Format: `user_id din`  (jaise `123456789 30`)\n`din = 0` likhoge to Premium hat jayega.")
        if act == "bl":
            return await show(q, *await list_panel("ban"))
        if act == "pl":
            return await show(q, *await list_panel("prem"))
        if act == "lgp":
            return await show(q, *log_panel())
        if act == "lgs":
            pending[uid] = ("logchat",)
            return await q.message.reply("Log channel/group ki ID (`-100...`), @username bhejo ya uska koi message forward karo.")
        if act == "lgt":
            if not LOG["chat"]:
                return await q.answer("Log chat set nahi hai", show_alert=True)
            await bot.send_message(LOG["chat"], "🧪 Test log message ✅")
            return await q.answer("Test bhej diya")
        if act == "lgx":
            LOG["chat"] = None
            await gset(log_chat=None)
            await q.answer("Log chat hata diya")
            return await show(q, *log_panel())
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


# ======================= text input (button ke baad) =======================
OWNER_KINDS = {"lphone", "lcode", "lpass", "lstr", "delay", "welcome", "logchat", "ban", "prem"}

def check_template(text: str, lang: str = "hi"):
    try:
        text.format(name="x", chat="y")
    except Exception:
        raise UserError(tx(lang, "e_tpl"))

def chat_ref(m: Message):
    fo = getattr(m, "forward_from_chat", None) or getattr(getattr(m, "forward_origin", None), "chat", None)
    ref = fo.id if fo else (m.text or "").strip()
    return int(ref) if str(ref).lstrip("-").isdigit() else ref

@bot.on_message(filters.private & is_user & ~filters.regex(r"^/"), group=1)
@safe
async def on_input(_, m: Message):
    uid = m.from_user.id
    st = pending.get(uid)
    if not st:
        return
    await touch_user(m.from_user)
    kind = st[0]
    lang = await lang_of(uid)
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
            lgn = logins[uid]
            code = "".join(ch for ch in m.text if ch.isdigit())
            await try_delete(m)
            try:
                await lgn["c"].sign_in(lgn["phone"], lgn["hash"], code)
            except SessionPasswordNeeded:
                pending[uid] = ("lpass",)
                return await m.reply("🔐 2-step verification password bhejo (message delete ho jayega).")
            return await finish_login(m, uid)
        if kind == "lpass":
            lgn = logins[uid]
            await try_delete(m)
            await lgn["c"].check_password(m.text)
            return await finish_login(m, uid)
        if kind == "lstr":
            await try_delete(m)
            await start_user(m.text.strip())
            await db.settings.update_one({"_id": "session"}, {"$set": {"string": m.text.strip()}}, upsert=True)
            pending.pop(uid, None)
            await m.reply("✅ Session set ho gaya.")
            return await show(m, *await session_panel())
        if kind == "logchat":
            try:
                ch = await bot.get_chat(chat_ref(m))
                await bot.send_message(ch.id, "✅ Log channel set ho gaya.")
            except Exception as e:
                raise UserError(f"Is chat me message nahi bhej paaya ({type(e).__name__}). Bot ko wahan admin banao.")
            LOG["chat"] = ch.id
            await gset(log_chat=ch.id)
            pending.pop(uid, None)
            return await show(m, *log_panel())
        if kind == "ban":
            target = int(m.text.strip())
            res = await do_ban(target, target not in BANNED, uid)
            pending.pop(uid, None)
            await m.reply(res)
            return await show(m, *await users_panel(0))
        if kind == "prem":
            a = m.text.split()
            res = await do_premium(int(a[0]), int(a[1]), uid)
            pending.pop(uid, None)
            await m.reply(res)
            return await show(m, *await users_panel(0))
        if kind == "add":
            try:
                chat = await bot.get_chat(chat_ref(m))
            except Exception:
                raise UserError(tx(lang, "e_notfound"))
            await verify_add(uid, chat, lang)
            pending.pop(uid, None)
            await register_chat(chat, uid, force=(uid not in OWNERS))
            await notify_admin(f"📺 Naya chat (manual): {clean(chat.title)} `{chat.id}` — user `{uid}`")
            await m.reply(f"{tx(lang, 'added', title=clean(chat.title))}\n\n{helper_note(lang)}")
            return await show(m, *await chat_panel(chat.id, uid))
        if kind == "fsadd":
            cid = st[1]
            try:
                ch = await bot.get_chat(chat_ref(m))
            except Exception:
                raise UserError(tx(lang, "e_notfound"))
            await check_admin(uid, ch, lang, need_invite=False)
            link = f"https://t.me/{ch.username}" if getattr(ch, "username", None) else None
            if not link:
                try:
                    link = await bot.export_chat_invite_link(ch.id)
                except Exception:
                    raise UserError(tx(lang, "e_nolink"))
            c = await ccfg(cid) or {}
            lst = c.get("fsub", [])
            if len(lst) >= 3:
                raise UserError(tx(lang, "fs_max"))
            if not any(e["id"] == ch.id for e in lst):
                lst.append({"id": ch.id, "title": ch.title, "link": link})
            await db.chats.update_one({"_id": cid}, {"$set": {"fsub": lst}})
            pending.pop(uid, None)
            return await show(m, *await fsub_panel(cid, uid))
        if kind in ("delay", "cdelay"):
            lo, hi = sorted(float(x) for x in m.text.split()[:2])
            if lo < 0 or (uid not in OWNERS and (lo < 1 or hi > 300)):
                raise UserError(tx(lang, "e_delay"))
            if kind == "delay":
                await gset(min=lo, max=hi)
                pending.pop(uid, None)
                return await show(m, *await delay_panel())
            await db.chats.update_one({"_id": st[1]}, {"$set": {"min": lo, "max": hi}}, upsert=True)
            pending.pop(uid, None)
            return await show(m, *await chat_panel(st[1], uid))
        if kind == "welcome":
            check_template(m.text, lang)
            await gset(welcome=m.text, welcome_on=True)
            pending.pop(uid, None)
            await m.reply("✅ Global welcome text save + ON")
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
            return await m.reply(f"❌ {type(e).__name__}: {str(e)[:500]}\nPhir se Login button dabao.")
        log.exception("input error")
        detail = f"{type(e).__name__}: {str(e)[:500]}" if uid in OWNERS else "Invalid input"
        await m.reply(f"❌ {detail}")


# ======================= background tasks =======================
async def gate_cleaner():
    """Time limit khatam hone par captcha/force-sub wali requests decline."""
    while True:
        await asyncio.sleep(120)
        try:
            cutoff = time.time() - GATE_MINUTES * 60
            old = [g async for g in db.gate.find({"ts": {"$lt": cutoff}})]
            for g in old:
                await gate_fail(g["chat_id"], g["user_id"], g.get("name"), g.get("username"), "GATE_TIMEOUT")
                try:
                    c = await ccfg(g["chat_id"]) or {}
                    await bot.send_message(g["user_id"], tx(c.get("lang", "hi"), "gate_expired"))
                except Exception:
                    pass
        except Exception:
            log.exception("gate_cleaner")

async def plan_watcher():
    """Premium khatam hone par user ko batata hai."""
    while True:
        await asyncio.sleep(1800)
        try:
            now = time.time()
            ended = [u async for u in db.users.find({"premium_until": {"$gt": 0, "$lt": now}, "exp_notified": {"$ne": True}})]
            for u in ended:
                await db.users.update_one({"_id": u["_id"]}, {"$set": {"exp_notified": True}})
                try:
                    await bot.send_message(u["_id"], tx(await lang_of(u["_id"]), "prem_end"))
                except Exception:
                    pass
                await notify_admin(f"⌛ Premium khatam: `{u['_id']}`")
        except Exception:
            log.exception("plan_watcher")

async def helper_health():
    """Helper session mar gaya (logout/revoke) to turant alert."""
    while True:
        await asyncio.sleep(1800)
        if not user:
            continue
        try:
            await user.get_me()
        except Exception as e:
            if reason_of(e) in AUTH_DEAD:
                await helper_died(reason_of(e))
            else:
                log.warning("helper health: %s", e)


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

async def main():
    await web_app()
    await bot.start()
    BOT_ID["id"] = (await bot.get_me()).id
    try:
        g = await gcfg()
        STATE["public"] = bool(g.get("public", False))
        lc = g.get("log_chat") or (int(ENV_LOG) if ENV_LOG and ENV_LOG.lstrip("-").isdigit() else None)
        LOG["chat"] = lc
        async for u in db.users.find({"banned": True}):
            BANNED.add(u["_id"])
    except Exception as e:
        log.warning("settings load fail: %s", e)
    try:
        await load_user()
    except Exception as e:
        log.warning("session load fail: %s", e)
        await notify_admin(f"🚨 Helper session start nahi hua: `{type(e).__name__}`. /settings → 🔑 Old-Req Session se dobara login karo.", important=True)
    for _ in range(WORKERS):
        asyncio.create_task(worker())
    for t in (self_ping, gate_cleaner, plan_watcher, helper_health):
        asyncio.create_task(t())
    status = await mongo_status()
    log.info("Bot started | %s | owners=%s | public=%s", status, sorted(OWNERS), STATE["public"])
    for oid in OWNERS:
        try:
            await bot.send_message(oid, f"🚀 Bot started\n{status}\nMode: {'🌐 Public' if STATE['public'] else '🔒 Private'}"
                                        f"\nHelper: {'✅' if user else '❌'} | Log: {'✅' if LOG['chat'] else '❌'}")
        except Exception as e:
            log.warning("owner %s ko msg nahi gaya: %s", oid, e)
    await asyncio.Event().wait()

if __name__ == "__main__":
    loop.run_until_complete(main())
