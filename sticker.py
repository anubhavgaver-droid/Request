"""
sticker.py - /start par pehle ek sticker (loading), 2-5 second baad delete, phir asli panel.

main.py se import hota hai (`import sticker as S`). Sticker sirf OWNER badal sakta hai:
  - sticker seedha bhejo, ya
  - sticker ki file_id (ID) paste karo.
Is file me: texts (hindi/english), settings panel, aur sticker dikhane wala helper.
"""
import asyncio
from pyrogram.types import InlineKeyboardMarkup

SECS_CHOICES = (2, 3, 5)
DEFAULT_SECS = 3


class StickerError(ValueError):
    """User ko dikhane layak galti."""


_SC = dict(zip("abcdefghijklmnopqrstuvwxyz", "ᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢ"))


def sc(t: str) -> str:
    return "".join(_SC.get(ch.lower(), ch) if ch.isascii() and ch.isalpha() else ch for ch in t)


TXS = {
    "hi": {
        "b_home": "स्टार्ट स्टिकर",
        "title": "**स्टार्ट स्टिकर**\n━━━━━━━━━━━━\nस्टेटस › {on}\nस्टिकर › {st}\nदिखने का समय › {secs} सेकंड\n\n"
                 "कोई /start दबाए तो पहले ये स्टिकर आता है, फिर अपने-आप हट जाता है, फिर पैनल दिखता है।",
        "st_yes": "✅ लगा है", "st_no": "❌ नहीं",
        "b_on": "स्टिकर: {s}", "b_set": "स्टिकर लगाओ", "b_chg": "स्टिकर बदलो", "b_rm": "स्टिकर हटाओ",
        "b_prev": "प्रीव्यू", "b_secs": "{n}s", "back": "‹ वापस",
        "prompt": "स्टिकर भेजो, या स्टिकर की **ID** (file_id) पेस्ट करो।",
        "saved": "✅ स्टिकर सेव हो गया।",
        "e_none": "स्टिकर या उसकी ID भेजो।",
        "e_bad": "ये स्टिकर / ID काम नहीं कर रही। दूसरा स्टिकर भेजो या सही ID दो।",
        "e_nosticker": "पहले स्टिकर लगाओ।",
        "removed": "स्टिकर हटा दिया।",
    },
    "en": {
        "b_home": "Start sticker",
        "title": "**" + sc("start sticker") + "**\n━━━━━━━━━━━━\n" + sc("status") + " › {on}\n"
                 + sc("sticker") + " › {st}\n" + sc("shown for") + " › {secs} sec\n\n"
                 "When someone presses /start, this sticker shows first, then disappears, then the panel appears.",
        "st_yes": "✅ set", "st_no": "❌ none",
        "b_on": "Sticker: {s}", "b_set": "Add sticker", "b_chg": "Change sticker", "b_rm": "Remove sticker",
        "b_prev": "Preview", "b_secs": "{n}s", "back": "‹ Back",
        "prompt": "Send a sticker, or paste the sticker **ID** (file_id).",
        "saved": "✅ Sticker saved.",
        "e_none": "Send a sticker or its ID.",
        "e_bad": "This sticker / ID doesn't work. Send another sticker or a correct ID.",
        "e_nosticker": "Add a sticker first.",
        "removed": "Sticker removed.",
    },
}


def ts(lang: str, key: str, **kw) -> str:
    return (TXS.get(lang, TXS["hi"]).get(key) or TXS["hi"][key]).format(**kw)


def _onoff(b: bool) -> str:
    return "🟢 ON" if b else "🔴 OFF"


def panel(lang: str, g: dict, B):
    """g = gcfg(); B = main.py ka button helper."""
    sid = g.get("start_sticker")
    on = bool(g.get("start_sticker_on", True))
    secs = int(g.get("start_sticker_secs") or DEFAULT_SECS)
    text = ts(lang, "title", on=_onoff(on and bool(sid)), st=ts(lang, "st_yes" if sid else "st_no"), secs=secs)
    rows = [
        [B(ts(lang, "b_on", s=_onoff(on)), callback_data="sko")],
        [B(ts(lang, "b_chg" if sid else "b_set"), callback_data="skt"),
         B(ts(lang, "b_prev"), callback_data="skv")],
        [B(("🟢 ✔ " if n == secs else "🔵 ") + ts(lang, "b_secs", n=n), callback_data=f"sks:{n}") for n in SECS_CHOICES],
    ]
    if sid:
        rows.append([B(ts(lang, "b_rm"), callback_data="skx")])
    rows.append([B(ts(lang, "back"), callback_data="main")])
    return text, InlineKeyboardMarkup(rows)


async def flash(reply_sticker, sticker_id: str, secs: float):
    """Sticker bhejo, `secs` second rukkar delete karo. Koi bhi galti ho to chupchap aage badho."""
    msg = None
    try:
        msg = await reply_sticker(sticker_id)
        await asyncio.sleep(secs)
    except Exception:
        pass
    finally:
        if msg is not None:
            try:
                await msg.delete()
            except Exception:
                pass
