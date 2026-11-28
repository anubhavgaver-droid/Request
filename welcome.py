"""
welcome.py - Welcome DM: text + optional photo + URL buttons.

main.py se import hota hai (`import welcome as W`). Is file me:
  - button likhne ka format parse karna (parse_buttons)
  - welcome bhejna (deliver)  -> photo + caption + buttons
  - welcome settings panel ka text + keyboard (panel)
  - is feature ke saare hindi/english texts (tw)
Ye file main.py ko import nahi karti, isliye circular import nahi hota.
"""
import re
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

MAX_BUTTONS = 6          # total buttons
MAX_PER_ROW = 2          # ek line me
MAX_LABEL = 40
MAX_TEXT = 3500
CAPTION_LIMIT = 1024     # Telegram: photo caption limit


class WelcomeError(ValueError):
    """User ko dikhane layak galti (text me)."""


# ----------------------------------------------------------------- small caps (English labels)
_SC = dict(zip("abcdefghijklmnopqrstuvwxyz", "ᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢ"))


def sc(t: str) -> str:
    return "".join(_SC.get(ch.lower(), ch) if ch.isascii() and ch.isalpha() else ch for ch in t)


# ----------------------------------------------------------------- texts
TXW = {
    "hi": {
        "title": "**वेलकम DM** · {scope}\n━━━━━━━━━━━━\nस्टेटस › {on}\nटेक्स्ट › {text}\nफोटो › {photo}\nबटन › {btn}",
        "scope_all": "सभी चैट",
        "photo_yes": "✅ लगी है", "photo_no": "❌ नहीं",
        "btn_n": "✅ {n} बटन", "btn_no": "❌ नहीं",
        "b_on": "वेलकम: {s}", "b_prev": "प्रीव्यू", "b_text": "टेक्स्ट बदलो",
        "b_photo_set": "फोटो लगाओ", "b_photo_chg": "फोटो बदलो", "b_photo_rm": "फोटो हटाओ",
        "b_btn_set": "बटन जोड़ो", "b_btn_chg": "बटन बदलो", "b_btn_rm": "बटन हटाओ", "back": "‹ वापस",
        "p_text": "नया वेलकम टेक्स्ट भेजो।\n\n{{name}} = मेंबर का नाम\n{{chat}} = चैट का नाम\nबोल्ड के लिए: **टेक्स्ट**",
        "p_photo": "वेलकम के लिए फोटो भेजो (फाइल नहीं, फोटो की तरह)।",
        "p_btn": "बटन भेजो, हर लाइन में एक:\n`नाम | लिंक`\n\nएक लाइन में 2 बटन:\n`नाम | लिंक && नाम | लिंक`\n\n"
                 "लिंक: https://... या @username\nज़्यादा से ज़्यादा {max} बटन।\n\n**उदाहरण**\n"
                 "`चैनल खोलो | https://t.me/example`\n`रूल्स | https://example.com && सपोर्ट | @support`",
        "saved": "✅ सेव हो गया।",
        "preview_ok": "प्रीव्यू आपको भेज दिया ✅",
        "preview_fail": "प्रीव्यू नहीं भेज सका: {e}",
        "e_photo": "फोटो भेजो (फाइल/टेक्स्ट नहीं)।",
        "e_text_empty": "टेक्स्ट खाली है।",
        "e_text_long": "टेक्स्ट बहुत लंबा है (अधिकतम {max} अक्षर)।",
        "e_btn_empty": "कोई बटन नहीं मिला।",
        "e_btn_line": "लाइन {n}: `नाम | लिंक` ऐसे लिखो।",
        "e_btn_label": "लाइन {n}: बटन का नाम 1 से {max} अक्षर का हो।",
        "e_btn_url": "लाइन {n}: लिंक सही नहीं है। https://... या @username दो।",
        "e_btn_many": "ज़्यादा से ज़्यादा {max} बटन और एक लाइन में {row} बटन चलेंगे।",
    },
    "en": {
        "title": "**" + sc("welcome dm") + "** · {scope}\n━━━━━━━━━━━━\n" + sc("status") + " › {on}\n"
                 + sc("text") + " › {text}\n" + sc("photo") + " › {photo}\n" + sc("buttons") + " › {btn}",
        "scope_all": "All chats",
        "photo_yes": "✅ set", "photo_no": "❌ none",
        "btn_n": "✅ {n} buttons", "btn_no": "❌ none",
        "b_on": "Welcome: {s}", "b_prev": "Preview", "b_text": "Edit text",
        "b_photo_set": "Add photo", "b_photo_chg": "Change photo", "b_photo_rm": "Remove photo",
        "b_btn_set": "Add buttons", "b_btn_chg": "Edit buttons", "b_btn_rm": "Remove buttons", "back": "‹ Back",
        "p_text": "Send the new welcome text.\n\n{{name}} = member's name\n{{chat}} = chat name\nBold: **text**",
        "p_photo": "Send a photo for the welcome (as a photo, not a file).",
        "p_btn": "Send the buttons, one per line:\n`Name | link`\n\nTwo buttons in one line:\n`Name | link && Name | link`\n\n"
                 "Link: https://... or @username\nMax {max} buttons.\n\n**Example**\n"
                 "`Open channel | https://t.me/example`\n`Rules | https://example.com && Support | @support`",
        "saved": "✅ Saved.",
        "preview_ok": "Preview sent to you ✅",
        "preview_fail": "Couldn't send the preview: {e}",
        "e_photo": "Send a photo (not a file or text).",
        "e_text_empty": "The text is empty.",
        "e_text_long": "The text is too long (max {max} characters).",
        "e_btn_empty": "No buttons found.",
        "e_btn_line": "Line {n}: write it as `Name | link`.",
        "e_btn_label": "Line {n}: the button name must be 1 to {max} characters.",
        "e_btn_url": "Line {n}: the link isn't valid. Use https://... or @username.",
        "e_btn_many": "At most {max} buttons, and {row} per line.",
    },
}


def tw(lang: str, key: str, **kw) -> str:
    s = TXW.get(lang, TXW["hi"]).get(key) or TXW["hi"][key]
    return s.format(**kw)


# ----------------------------------------------------------------- text / buttons
def render(text: str, name: str, chat: str) -> str:
    """{name} / {chat} badalta hai. Safe: galat braces se crash nahi hota."""
    return (text or "").replace("{name}", name or "").replace("{chat}", chat or "")


def check_text(text: str, lang: str = "hi") -> str:
    text = (text or "").strip()
    if not text:
        raise WelcomeError(tw(lang, "e_text_empty"))
    if len(text) > MAX_TEXT:
        raise WelcomeError(tw(lang, "e_text_long", max=MAX_TEXT))
    return text


_URL = re.compile(r"^(https?://|tg://)\S+$", re.I)
_USER = re.compile(r"^@([A-Za-z0-9_]{4,32})$")


def _norm_url(u: str):
    u = u.strip()
    m = _USER.match(u)
    if m:
        return f"https://t.me/{m.group(1)}"
    if re.match(r"^(t|telegram)\.me/\S+$", u, re.I):
        u = "https://" + u
    return u if _URL.match(u) else None


def parse_buttons(raw: str, lang: str = "hi"):
    """'Naam | link' (har line me ek, ek line me 2 ho to '&&') -> [[ [label, url], ... ], ...]"""
    rows, total = [], 0
    for n, line in enumerate([l for l in (raw or "").splitlines() if l.strip()], 1):
        row = []
        for part in line.split("&&"):
            label, sep, url = part.partition("|")
            label, url = label.strip(), url.strip()
            if not sep or not url:
                raise WelcomeError(tw(lang, "e_btn_line", n=n))
            if not 1 <= len(label) <= MAX_LABEL:
                raise WelcomeError(tw(lang, "e_btn_label", n=n, max=MAX_LABEL))
            fixed = _norm_url(url)
            if not fixed:
                raise WelcomeError(tw(lang, "e_btn_url", n=n))
            row.append([label, fixed])
        if len(row) > MAX_PER_ROW:
            raise WelcomeError(tw(lang, "e_btn_many", max=MAX_BUTTONS, row=MAX_PER_ROW))
        total += len(row)
        rows.append(row)
    if not rows:
        raise WelcomeError(tw(lang, "e_btn_empty"))
    if total > MAX_BUTTONS:
        raise WelcomeError(tw(lang, "e_btn_many", max=MAX_BUTTONS, row=MAX_PER_ROW))
    return rows


def count_buttons(rows) -> int:
    return sum(len(r) for r in (rows or []))


def markup(rows):
    if not rows:
        return None
    return InlineKeyboardMarkup([[InlineKeyboardButton(l, url=u) for l, u in row] for row in rows])


# ----------------------------------------------------------------- sending
async def deliver(client, uid: int, w: dict, name: str, chat_title: str, call):
    """Welcome bhejo: photo (caption ke saath) + buttons. Photo na chale to sirf text.
    w = {"text", "photo", "buttons"};  call = FloodWait-safe wrapper:  await call(fn, *args, **kw)"""
    text = render(w.get("text") or "", name, chat_title)
    photo = w.get("photo")
    kb = markup(w.get("buttons"))
    if photo:
        try:
            if len(text) <= CAPTION_LIMIT:
                return await call(client.send_photo, uid, photo, caption=text, reply_markup=kb)
            await call(client.send_photo, uid, photo)          # lamba text: photo alag, text + buttons neeche
        except Exception:
            pass                                               # photo kharab/expired: sirf text bhejo
    await call(client.send_message, uid, text, reply_markup=kb)


# ----------------------------------------------------------------- settings panel
def _onoff(b: bool) -> str:
    return "🟢 ON" if b else "🔴 OFF"


def _one_line(t: str, n: int = 60) -> str:
    t = re.sub(r"[*_`\[\]]", "", (t or "")).replace("\n", " ").strip()
    return t[:n] + ("…" if len(t) > n else "")


def panel(lang: str, scope: str, title: str, w: dict, B):
    """scope: 'g' (sabhi chat) ya chat id.  w = {"on","text","photo","buttons"}.  B = main.py ka button helper."""
    nb = count_buttons(w.get("buttons"))
    text = tw(lang, "title", scope=(tw(lang, "scope_all") if scope == "g" else title), on=_onoff(w["on"]),
              text=_one_line(w.get("text")) or "—",
              photo=tw(lang, "photo_yes" if w.get("photo") else "photo_no"),
              btn=tw(lang, "btn_n", n=nb) if nb else tw(lang, "btn_no"))
    rows = [
        [B(tw(lang, "b_on", s=_onoff(w["on"])), callback_data=f"wpo:{scope}")],
        [B(tw(lang, "b_prev"), callback_data=f"wpv:{scope}"), B(tw(lang, "b_text"), callback_data=f"wpt:{scope}")],
        [B(tw(lang, "b_photo_chg" if w.get("photo") else "b_photo_set"), callback_data=f"wph:{scope}"),
         B(tw(lang, "b_btn_chg" if nb else "b_btn_set"), callback_data=f"wpb:{scope}")],
    ]
    rm = []
    if w.get("photo"):
        rm.append(B(tw(lang, "b_photo_rm"), callback_data=f"wpx:{scope}"))
    if nb:
        rm.append(B(tw(lang, "b_btn_rm"), callback_data=f"wpc:{scope}"))
    if rm:
        rows.append(rm)
    rows.append([B(tw(lang, "back"), callback_data="main" if scope == "g" else f"c:{scope}")])
    return text, InlineKeyboardMarkup(rows)
