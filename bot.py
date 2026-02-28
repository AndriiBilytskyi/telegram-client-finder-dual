import os
import sys
import re
import json
import time
import asyncio
import logging
import traceback
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Dict, Any, Tuple, List

import telethon
from telethon import TelegramClient, events
from telethon.errors import (
    FloodWaitError,
    PeerFloodError,
    UserPrivacyRestrictedError,
    UserNotMutualContactError,
    ChatWriteForbiddenError,
    RPCError,
)
from telethon.tl.functions.channels import InviteToChannelRequest
from telethon.tl.types import InputPeerUser

try:
    from openai import AsyncOpenAI
except Exception:
    AsyncOpenAI = None


# =============================================================================
# LOGGING
# =============================================================================

LOG_PATH = os.getenv("LOG_PATH", "/data/bot.log")
log_dir = os.path.dirname(LOG_PATH)
if log_dir:
    os.makedirs(log_dir, exist_ok=True)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

fh = RotatingFileHandler(LOG_PATH, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
fh.setFormatter(_formatter)

sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(_formatter)

root_logger.handlers = [fh, sh]


def excepthook(exc_type, exc, tb):
    logging.critical("UNCAUGHT EXCEPTION:\n%s", "".join(traceback.format_exception(exc_type, exc, tb)))


sys.excepthook = excepthook


def loop_exc_handler(loop, context):
    msg = context.get("exception") or context.get("message")
    logging.critical("ASYNC EXCEPTION: %s", msg, exc_info=context.get("exception"))


# =============================================================================
# CONFIG
# =============================================================================

print("Telethon version:", getattr(telethon, "__version__", "unknown"))

DEFAULT_GROUPS = sorted(set([
    '@NRWanzeigen', '@ukraineingermany1', '@ukrainians_in_germany1',
    '@berlin_ukrainians', '@deutscheukraine', '@ukraincifrankfurt',
    '@jobinde', '@hamburg_ukrainians', '@UkraineinMunich',
    '@workeuropeplus', '@UA_in_Germany', '@dusseldorfukrain',
    '@TruckingNordrheinWestfalen', '@Berlin_UA2025', '@bonn_help',
    '@GermanyTop1', '@germany_chatik', '@nrw_anzeige', '@bochum_ua',
    '@POZITYV_PUTESHESTVIYA', '@uahelpkoelnanzeigen', '@cologne_help',
    '@TheGermany1', '@germania_migranty', '@GLOBUSEXPRESS',
    '@nashipomogut', '@ukr_de_essen', '@save_ukraine_de_essen',
    '@solingen_UA', '@keln_baraholka',
    '@ukraine_dortmund', '@UADuesseldorf',
    '@beauty_dusseldorf', '@pomoshukraineaachen', '@AhlenNRW',
    '@alsdorfua', '@aschafenburg', '@NA6R_hilft', '@bad4ua',
    '@badenbaden_lkr', '@kreiskleve', '@Bernkastel_Wittlich',
    '@bielefeldhelps', '@ukraine_bochum_support', '@uahelp_ruhrgebiet',
    '@DeutschlandBottrop', '@BS_UA_HELP', '@refugeesbremen',
    '@Bruchsal_Chat', '@Ukrainians_in_Calw', '@hilfe_ukraine_chemnitz',
    '@cottbus_ua', '@hamburg_ukraine_chat', '@Magdeburg_ukrainian',
    '@Fainy_Kiel', '@ukraine_in_Hanover', '@uahelfen_arbeit',
    '@bremen_hannover_dresden', '@ukraine_in_dresden', '@BavariaLife',
    '@ErfurtUA', '@MunchenBavaria', '@ua_ka_help', '@Ukrainians_in_Berlin_ua',
    '@refugeesinAustria', '@KaiserslauternUA', '@Karlsruhe_Ukraine',
    '@MunchenGessenBremen', '@chatFreiburg', '@Pfaffenhofen',
    '@deutschland_diaspora', '@Manner_ClubNRW', '@Ukrainer_in_Deutschland',
    '@Ukrainer_in_Wuppertal', '@ukrainians_in_hamburg_ua', '@ukrainians_berlin',
    '@berlinhelpsukrainians', '@Bayreuth_Bamberg',
    '@austriaobiavlenia', '@ukraineat', '@ukraineaustriaat',
    '@Ukrainians_in_Wien', '@Vienna_Linz', '@TheAustria1',
    '@Salzburg_Vena', '@qXGhIDwK00A4MWM0', '@austria_ua',
    '@Ukrainians_Wels_Linz', '@cafe_kyiv_linz', '@usteiermark',
    '@koblenz_ta_navkolo',
]))

raw_groups = os.getenv("GROUPS_TO_MONITOR", "").strip()
GROUPS_TO_MONITOR = sorted(set(
    [g.strip() for g in raw_groups.split(",") if g.strip()] if raw_groups else DEFAULT_GROUPS
))

ACCOUNTS = [
    {
        "api_id": int(os.getenv("TG_API_ID_1", "0") or "0"),
        "api_hash": os.getenv("TG_API_HASH_1", "").strip(),
        "session_name": os.getenv("TG_SESSION_1", "session1").strip(),
        "your_username": os.getenv("TG_ME_USERNAME_1", "Andrii_Bilytskyi").strip().lstrip("@"),
    },
    {
        "api_id": int(os.getenv("TG_API_ID_2", "0") or "0"),
        "api_hash": os.getenv("TG_API_HASH_2", "").strip(),
        "session_name": os.getenv("TG_SESSION_2", "session2").strip(),
        "your_username": os.getenv("TG_ME_USERNAME_2", "Anwalt_Bilytskyi").strip().lstrip("@"),
    },
]

ADMIN_NOTIFY_USERNAME = os.getenv("ADMIN_NOTIFY_USERNAME", "Andrii_Bilytskyi").strip().lstrip("@")
TARGET_INVITE_GROUP = os.getenv("TARGET_INVITE_GROUP", "@advocate_ua_1").strip()

AUTO_SEND_HIGH_CONFIDENCE = os.getenv("AUTO_SEND_HIGH_CONFIDENCE", "0").strip() == "1"
AUTO_SEND_THRESHOLD = float(os.getenv("AUTO_SEND_THRESHOLD", "0.92"))
AUTO_INVITE_AFTER_DM = os.getenv("AUTO_INVITE_AFTER_DM", "0").strip() == "1"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-mini").strip()
OPENAI_TIMEOUT_SEC = int(os.getenv("OPENAI_TIMEOUT_SEC", "45"))
MAX_AI_INPUT_CHARS = int(os.getenv("MAX_AI_INPUT_CHARS", "2400"))

DEFAULT_DATA_DIR = "/data" if os.path.isdir("/data") else "."
DATA_DIR = os.getenv("DATA_DIR", DEFAULT_DATA_DIR).strip() or "."
CACHE_DIR = os.getenv("CACHE_DIR", os.path.join(DATA_DIR, "group_cache"))
SESSION_DIR = os.getenv("SESSION_DIR", os.path.join(DATA_DIR, "sessions"))
SEEN_FILE = os.path.join(DATA_DIR, "seen_messages.json")
LEADS_FILE = os.path.join(DATA_DIR, "leads.json")
ANALYTICS_FILE = os.path.join(DATA_DIR, "analytics.json")
FAVORITES_FILE = os.path.join(DATA_DIR, "favorites.json")
OUTBOUND_FILE = os.path.join(DATA_DIR, "outbound_stats.json")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(SESSION_DIR, exist_ok=True)

OUTBOUND_DM_PER_DAY = int(os.getenv("OUTBOUND_DM_PER_DAY", "25"))
OUTBOUND_DM_PER_HOUR = int(os.getenv("OUTBOUND_DM_PER_HOUR", "8"))
INVITE_PER_DAY = int(os.getenv("INVITE_PER_DAY", "20"))
MIN_SECONDS_BETWEEN_DMS = int(os.getenv("MIN_SECONDS_BETWEEN_DMS", "180"))

LAWYER_SITE = "https://andriibilytskyi.com"
LAWYER_ANWALT = "https://www.anwalt.de/andrii-bilytskyi"
LAWYER_GROUP = "https://t.me/advocate_ua_1"
LAWYER_BRIEF = (
    "Адвокат Андрій Білицький. Правова допомога в Україні та Німеччині. "
    "Практика включає кримінальні, цивільні, адміністративні справи, міграцію та інтеграцію, "
    "а також представництво в суді й консультації для українців у Німеччині."
)

CLIENTS: Dict[str, TelegramClient] = {}
ME_IDS: Dict[str, int] = {}
PERSIST_LOCK = asyncio.Lock()
OUTBOUND_LOCK = asyncio.Lock()
shutdown = asyncio.Event()

openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY) if (AsyncOpenAI and OPENAI_API_KEY) else None


# =============================================================================
# PERSISTENCE
# =============================================================================

def load_json(path: str, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            logging.error("Failed to load %s", path)
    return default


def save_json(path: str, data):
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


SEEN = load_json(SEEN_FILE, {})
LEADS = load_json(LEADS_FILE, {})
ANALYTICS = load_json(ANALYTICS_FILE, {})
FAVORITES = load_json(FAVORITES_FILE, {})
OUTBOUND_STATS = load_json(OUTBOUND_FILE, {})


# =============================================================================
# UTILS
# =============================================================================

SERVICE_PREFIXES = (
    "🆕 LEAD ",
    "📩 PRIVATE INBOUND [",
    "🤖 AUTO_SEND ",
    "🤖 AUTO_INVITE ",
    "⭐ FAVORITE ",
)

SERVICE_USERNAMES = {
    ADMIN_NOTIFY_USERNAME.lower(),
    *{
        (cfg.get("your_username") or "").lower()
        for cfg in ACCOUNTS
        if cfg.get("your_username")
    },
}


def normalize(text: str) -> str:
    text = (text or "").lower()
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"[^\w\s@§/+.-]", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def truncate(text: str, n: int) -> str:
    text = text or ""
    return text if len(text) <= n else text[: n - 1] + "…"


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def detect_language(text: str) -> str:
    t = text or ""
    cyr = len(re.findall(r"[А-Яа-яЁёІіЇїЄєҐґ]", t))
    lat = len(re.findall(r"[A-Za-zÄÖÜäöüß]", t))
    if cyr >= lat:
        if re.search(r"[ІіЇїЄєҐґ]", t):
            return "uk"
        return "ru"
    if re.search(r"\b(der|die|das|und|nicht|mit|für|anwalt|recht|versicherung)\b", t.lower()):
        return "de"
    return "en"


def phone_or_contact_present(text: str) -> bool:
    t = text or ""
    return bool(
        re.search(r"(?:\+?\d[\d\s().-]{7,}\d)", t)
        or re.search(r"@\w{4,}", t)
        or re.search(r"(?:whatsapp|viber|telegram|tg|instagram|insta|email|e-mail|webseite|site|сайт|личные сообщения|в личку|пишите в лс|пишіть у приват)", t, re.I)
        or re.search(r"https?://", t, re.I)
    )


def build_message_link(chat, message_id: int) -> str:
    if getattr(chat, "username", None):
        return f"https://t.me/{chat.username}/{message_id}"
    cid = str(chat.id)
    if cid.startswith("-100"):
        return f"https://t.me/c/{cid[4:]}/{message_id}"
    return "🔒 private group"


def safe_json_loads(text: str, default: Dict[str, Any]) -> Dict[str, Any]:
    if not text:
        return default
    try:
        return json.loads(text)
    except Exception:
        pass
    m = re.search(r"\{.*\}", text, flags=re.S)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            pass
    return default


def make_lead_id() -> str:
    return f"L{int(time.time())}{str(int(time.time_ns()))[-4:]}"


def update_analytics_bucket(group_title: str, category: str):
    group_data = ANALYTICS.get(group_title, {"total": 0, "categories": {}})
    group_data["total"] += 1
    group_data["categories"][category] = group_data["categories"].get(category, 0) + 1
    ANALYTICS[group_title] = group_data


def hash_fingerprint(sender_username: str, text: str) -> str:
    sender_key = (sender_username or "").lower().strip()
    base = normalize(text)[:300]
    return f"{sender_key}|{base}"


def purge_seen(hours: int = 72):
    now = time.time()
    stale = [k for k, ts in SEEN.items() if now - float(ts) > hours * 3600]
    for k in stale:
        SEEN.pop(k, None)


def is_service_message_text(text: str) -> bool:
    t = (text or "").strip()
    return t.startswith(SERVICE_PREFIXES)


def known_internal_sender(sender) -> bool:
    if not sender:
        return False
    sender_id = getattr(sender, "id", None)
    sender_username = (getattr(sender, "username", "") or "").lower()
    if sender_id and sender_id in set(ME_IDS.values()):
        return True
    if sender_username and sender_username in SERVICE_USERNAMES:
        return True
    return False


# =============================================================================
# RULES
# =============================================================================

SPAM_PATTERNS = [
    r"casino", r"казино", r"беттинг", r"ставк", r"промокод",
    r"личные кабинеты банков", r"лк банков", r"продажа аккаунтов",
    r"мошенн", r"обнал", r"отмыв", r"crypto.{0,8}bonus", r"bank accounts? for sale",
]

LEAD_SEARCH_PATTERNS = [
    r"\bищу\b.{0,25}\b(адвокат|юрист)",
    r"\bнужен\b.{0,25}\b(адвокат|юрист)",
    r"\bпорад(ьте|ьтеся)\b.{0,30}\b(адвокат|юрист)",
    r"\bконтакт\b.{0,20}\b(адвокат|юрист)",
    r"\brecommend\b.{0,20}\b(lawyer|attorney)",
    r"\blooking for\b.{0,20}\b(lawyer|attorney)",
    r"\b(rechtsanwalt|anwalt)\b.{0,20}\b(gesucht|empfehlen|kontakt)",
    r"\bпотріб(ен|на)\b.{0,20}\b(адвокат|юрист)",
    r"\bищу грамотного юриста\b",
    r"\bкто знает\b.{0,30}\b(адвокат|юрист)",
    r"\bberatunghilfeschein\b",
]

LEGAL_HINTS = [
    "адвокат", "юрист", "lawyer", "attorney", "anwalt", "rechtsanwalt",
    "внж", "aufenthalt", "§24", "fiktions", "widerspruch", "klage", "sozialgericht",
    "jobcenter", "sozialamt", "ausländerbehörde", "семейн", "развод", "опека",
    "arbeitsrecht", "mietrecht", "migration", "deport", "увольнен", "незаконн",
    "суд", "gericht", "polizei", "прокурат", "уголов", "arbeits", "medizinrecht",
    "patientenrecht", "пациент", "стоматолог", "страховка не покрывает", "arbeitsvertrag",
]

PARTNER_SERVICE_HINTS = [
    "versicherung", "страхов", "rechtsschutz", "kfz", "haftpflicht", "zahn",
    "krankenversicherung", "пенс", "финанс", "інвест", "ипотек", "baufinanzierung",
    "steuer", "налог", "strom", "gas", "immobil", "маклер", "broker", "консультант",
    "кредит", "leasing", "перевод", "übersetzung", "webseite", "сайт", "маркетинг",
    "jobcenter", "anmeldung", "schufa", "wbs",
]

LAWYER_COMPETITOR_HINTS = [
    "адвокат украины", "українським адвокатом", "свидоцтво адвоката", "rechtsanwalt", "anwalt"
]

QUESTION_RE = re.compile(
    r"\?|"
    r"\b(как|что|почему|зачем|когда|где|куда|сколько|можно ли|"
    r"подскажите|підкажіть|посоветуйте|порадьте|рекомендуйте|"
    r"как быть|was|wie|wo|warum|може|чому|хто може)\b",
    re.IGNORECASE,
)


def classify_message(text: str) -> Tuple[str, str]:
    t = normalize(text)
    if not t or len(t) < 3:
        return ("ignore", "empty_or_short")

    for pat in SPAM_PATTERNS:
        if re.search(pat, t, re.I):
            return ("reject_spam", f"spam:{pat}")

    for pat in LEAD_SEARCH_PATTERNS:
        if re.search(pat, t, re.I):
            return ("lead_search", f"lead_search:{pat}")

    has_contact = phone_or_contact_present(text)
    has_partner_hint = any(h in t for h in PARTNER_SERVICE_HINTS)
    is_likely_competitor_lawyer = any(h in t for h in LAWYER_COMPETITOR_HINTS)

    if has_contact and has_partner_hint and not is_likely_competitor_lawyer:
        return ("partner_services", "partner_services:contact+adjacent_service")

    if QUESTION_RE.search(text or "") and any(h in t for h in LEGAL_HINTS):
        return ("lead_question", "lead_question:question+legal_hint")

    if has_contact and is_likely_competitor_lawyer:
        return ("ignore", "other_lawyer_or_legal_promo")

    return ("ignore", "no_match")


# =============================================================================
# OPENAI
# =============================================================================

AI_JSON_FALLBACK = {
    "action": "skip",
    "confidence": 0.0,
    "language": "ru",
    "reason": "fallback",
    "reply_text": "",
}

AI_SYSTEM = f"""
Ты — Юстин, цифровой помощник адвоката Андрія Білицького.
Твоя задача — подготовить КОРОТКИЙ и ПРАВДОПОДОБНЫЙ текст личного сообщения в Telegram.

О юристе:
- {LAWYER_BRIEF}
- Сайт: {LAWYER_SITE}
- Профиль: {LAWYER_ANWALT}
- Telegram-группа: {LAWYER_GROUP}

Правила:
1. Пиши на языке исходного сообщения.
2. Не придумывай фактов и не обещай результат.
3. Не пиши как массовая реклама.
4. Тон: живой, вежливый, короткий, персональный.
5. Для lead_search / lead_question:
   - представься как Юстин, помощник адвоката;
   - предложи коротко описать ситуацию;
   - укажи 1-2 ссылки максимум.
6. Для partner_pitch:
   - предложи профессиональный контакт/взаимные рекомендации;
   - не дави и не спамь;
   - можно упомянуть, что адвокат работает с украинцами в Германии.
7. Для skip reply_text должен быть пустой.
8. Верни ответ строго в формате JSON.
9. Только JSON object. Без markdown, без пояснений, без лишнего текста.

Верни только JSON:
{{
  "action": "skip|lead_search_reply|lead_question_reply|partner_pitch",
  "confidence": 0.0,
  "language": "ru|uk|de|en",
  "reason": "short reason",
  "reply_text": "..."
}}
"""


def _normalize_ai_payload(message_text: str, parsed: Dict[str, Any]) -> Dict[str, Any]:
    parsed = parsed or {}

    action = str(parsed.get("action", "skip") or "skip").strip()
    if action not in {"skip", "lead_search_reply", "lead_question_reply", "partner_pitch"}:
        action = "skip"

    try:
        confidence = float(parsed.get("confidence", 0.0) or 0.0)
    except Exception:
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))

    language = str(parsed.get("language", "") or "").strip().lower()
    if language not in {"ru", "uk", "de", "en"}:
        language = detect_language(message_text)

    reason = str(parsed.get("reason", "no_reason") or "no_reason").strip()
    reply_text = str(parsed.get("reply_text", "") or "").strip()

    return {
        "action": action,
        "confidence": confidence,
        "language": language,
        "reason": reason,
        "reply_text": reply_text,
    }


async def ai_generate_reply(
    scenario_hint: str,
    message_text: str,
    group_title: str,
    sender_name: str,
) -> Dict[str, Any]:
    if not openai_client:
        return AI_JSON_FALLBACK

    compact_text = truncate((message_text or "").strip(), MAX_AI_INPUT_CHARS)

    json_instruction = (
        "Return valid JSON only. "
        "Output must be a single JSON object with keys: "
        "action, confidence, language, reason, reply_text. "
        "No markdown, no comments, no extra text."
    )

    user_prompt = (
        f"scenario_hint={scenario_hint}\n"
        f"group_title={group_title}\n"
        f"sender_name={sender_name}\n"
        f"{json_instruction}\n"
        f"message_text:\n{compact_text}\n\n"
        "Сначала оцени, стоит ли писать этому человеку. "
        "Если сообщение явно нецелевое, рискованное или похоже на спам — action=skip."
    )

    try:
        resp = await asyncio.wait_for(
            openai_client.responses.create(
                model=OPENAI_MODEL,
                instructions=AI_SYSTEM + "\nReturn JSON only.",
                input=user_prompt,
                store=False,
                text={"format": {"type": "json_object"}},
            ),
            timeout=OPENAI_TIMEOUT_SEC,
        )
        parsed = safe_json_loads(getattr(resp, "output_text", "") or "", AI_JSON_FALLBACK)
        return _normalize_ai_payload(message_text, parsed)

    except Exception as e:
        logging.warning("OpenAI json_object failed: %s", e)

    try:
        resp = await asyncio.wait_for(
            openai_client.responses.create(
                model=OPENAI_MODEL,
                instructions=AI_SYSTEM + "\nReturn JSON only. Return a single JSON object only.",
                input=user_prompt,
                store=False,
            ),
            timeout=OPENAI_TIMEOUT_SEC,
        )
        parsed = safe_json_loads(getattr(resp, "output_text", "") or "", AI_JSON_FALLBACK)
        result = _normalize_ai_payload(message_text, parsed)

        if not result.get("reply_text") and result["action"] in {
            "lead_search_reply",
            "lead_question_reply",
            "partner_pitch",
        }:
            result["reply_text"] = fallback_reply(scenario_hint, result["language"])

        return result

    except Exception as e:
        logging.warning("OpenAI plain json failed: %s", e)
        return AI_JSON_FALLBACK


def fallback_reply(category: str, language: str) -> str:
    if category in ("lead_search", "lead_question"):
        if language == "uk":
            return (
                "Вітаю! Я — Юстин, помічник адвоката Андрія Білицького. "
                "Побачив Ваше повідомлення. Якщо питання ще актуальне, можете коротко описати ситуацію тут у приватних повідомленнях. "
                f"Також інформація про адвоката: {LAWYER_SITE} або {LAWYER_ANWALT}"
            )
        if language == "de":
            return (
                "Guten Tag! Ich bin Justin, der Assistent von Rechtsanwalt Andrii Bilytskyi. "
                "Ich habe Ihre Nachricht gesehen. Wenn Ihr Anliegen noch aktuell ist, können Sie die Situation kurz privat schildern. "
                f"Infos: {LAWYER_SITE} oder {LAWYER_ANWALT}"
            )
        if language == "en":
            return (
                "Hello! I’m Justin, assistant to attorney Andrii Bilytskyi. "
                "I saw your message. If your issue is still relevant, feel free to briefly describe it in private messages. "
                f"More info: {LAWYER_SITE} or {LAWYER_ANWALT}"
            )
        return (
            "Здравствуйте! Я — Юстин, помощник адвоката Андрия Билицкого. "
            "Увидел ваше сообщение. Если вопрос еще актуален, можете коротко описать ситуацию в личных сообщениях. "
            f"Информация об адвокате: {LAWYER_SITE} или {LAWYER_ANWALT}"
        )

    if category == "partner_services":
        if language == "uk":
            return (
                "Вітаю! Я — Юстин, помічник адвоката Андрія Білицького. "
                "Побачив Ваше повідомлення. Якщо Вам цікаві професійні контакти та взаємні рекомендації для клієнтів у Німеччині, "
                f"буду радий зв’язку. Сайт: {LAWYER_SITE} | Telegram-група: {LAWYER_GROUP}"
            )
        if language == "de":
            return (
                "Guten Tag! Ich bin Justin, Assistent von Rechtsanwalt Andrii Bilytskyi. "
                "Ich habe Ihren Beitrag gesehen. Falls beruflicher Austausch oder gegenseitige Empfehlungen für Mandanten in Deutschland interessant sind, "
                f"freue ich mich über Kontakt. Website: {LAWYER_SITE} | Telegram: {LAWYER_GROUP}"
            )
        return (
            "Здравствуйте! Я — Юстин, помощник адвоката Андрия Билицкого. "
            "Увидел ваше сообщение. Если вам интересны профессиональные контакты и взаимные рекомендации клиентов в Германии, "
            f"буду рад связи. Сайт: {LAWYER_SITE} | Telegram-группа: {LAWYER_GROUP}"
        )
    return ""


# =============================================================================
# ENTITY CACHE
# =============================================================================

async def load_or_fetch_entities(client: TelegramClient, group_usernames: List[str]):
    import pickle

    entities = []
    for username in sorted(set(group_usernames)):
        try:
            filename = f"{username.strip('@')}.pkl"
            path = os.path.join(CACHE_DIR, filename)
            if os.path.exists(path):
                with open(path, "rb") as f:
                    ent = pickle.load(f)
                entities.append(ent)
                logging.info("✅ cache entity: %s", username)
            else:
                entity = await client.get_entity(username)
                with open(path, "wb") as f:
                    pickle.dump(entity, f)
                entities.append(entity)
                logging.info("📥 fetched entity: %s", username)
        except Exception as e:
            logging.error("❌ failed entity %s: %s", username, e)
    return entities


# =============================================================================
# OUTBOUND LIMITS
# =============================================================================

def _day_key() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _hour_key() -> str:
    return datetime.now().strftime("%Y-%m-%d %H")


def _session_stats(session_name: str) -> Dict[str, Any]:
    s = OUTBOUND_STATS.setdefault(session_name, {})
    s.setdefault("dm_day", {})
    s.setdefault("dm_hour", {})
    s.setdefault("invite_day", {})
    s.setdefault("last_dm_ts", 0.0)
    return s


async def can_send_dm(session_name: str) -> Tuple[bool, str]:
    async with OUTBOUND_LOCK:
        s = _session_stats(session_name)
        now = time.time()
        if now - float(s.get("last_dm_ts", 0)) < MIN_SECONDS_BETWEEN_DMS:
            wait = int(MIN_SECONDS_BETWEEN_DMS - (now - float(s.get("last_dm_ts", 0))))
            return False, f"wait_{wait}s"
        if int(s["dm_day"].get(_day_key(), 0)) >= OUTBOUND_DM_PER_DAY:
            return False, "dm_day_limit"
        if int(s["dm_hour"].get(_hour_key(), 0)) >= OUTBOUND_DM_PER_HOUR:
            return False, "dm_hour_limit"
        return True, "ok"


async def mark_dm_sent(session_name: str):
    async with OUTBOUND_LOCK:
        s = _session_stats(session_name)
        s["dm_day"][_day_key()] = int(s["dm_day"].get(_day_key(), 0)) + 1
        s["dm_hour"][_hour_key()] = int(s["dm_hour"].get(_hour_key(), 0)) + 1
        s["last_dm_ts"] = time.time()
        save_json(OUTBOUND_FILE, OUTBOUND_STATS)


async def can_invite(session_name: str) -> Tuple[bool, str]:
    async with OUTBOUND_LOCK:
        s = _session_stats(session_name)
        if int(s["invite_day"].get(_day_key(), 0)) >= INVITE_PER_DAY:
            return False, "invite_day_limit"
        return True, "ok"


async def mark_invite_sent(session_name: str):
    async with OUTBOUND_LOCK:
        s = _session_stats(session_name)
        s["invite_day"][_day_key()] = int(s["invite_day"].get(_day_key(), 0)) + 1
        save_json(OUTBOUND_FILE, OUTBOUND_STATS)


# =============================================================================
# LEADS
# =============================================================================

async def remember_lead(lead: Dict[str, Any]):
    async with PERSIST_LOCK:
        LEADS[lead["id"]] = lead
        save_json(LEADS_FILE, LEADS)


async def remember_favorite(lead_id: str):
    async with PERSIST_LOCK:
        lead = LEADS.get(lead_id)
        if not lead:
            return False
        FAVORITES[lead_id] = {
            "saved_at": now_iso(),
            "sender_username": lead.get("sender_username"),
            "sender_name": lead.get("sender_name"),
            "category": lead.get("category"),
            "source_link": lead.get("message_link"),
            "text": lead.get("text"),
        }
        save_json(FAVORITES_FILE, FAVORITES)
        return True


def render_lead_card(lead: Dict[str, Any]) -> str:
    ai = lead.get("ai", {}) or {}
    action = ai.get("action", "n/a")
    confidence = ai.get("confidence", 0.0)
    return (
        f"🆕 LEAD {lead['id']}\n"
        f"Категория: {lead['category']} | AI: {action} ({confidence:.2f})\n"
        f"Причина: {lead.get('rule_reason', '-')}\n"
        f"Сессия: {lead['session_name']}\n"
        f"Группа: {lead['chat_title']}\n"
        f"Ссылка: {lead['message_link']}\n"
        f"Отправитель: {lead.get('sender_name') or '-'} "
        f"{('@' + lead['sender_username']) if lead.get('sender_username') else ''}\n"
        f"Текст:\n{truncate(lead['text'], 1200)}\n\n"
        f"Draft:\n{truncate(ai.get('reply_text', '') or '', 1200)}\n\n"
        f"Команды:\n"
        f"/show {lead['id']}\n"
        f"/regen {lead['id']}\n"
        f"/dm {lead['id']}\n"
        f"/pitch {lead['id']}\n"
        f"/invite {lead['id']}\n"
        f"/fav {lead['id']}\n"
        f"/ignore {lead['id']}"
    )


async def send_admin_notice(client: TelegramClient, text: str):
    if not text:
        return
    try:
        await client.send_message(ADMIN_NOTIFY_USERNAME, text)
    except Exception as e:
        logging.error("Failed admin notice: %s", e)


# =============================================================================
# RESOLVE / ACTIONS
# =============================================================================

async def resolve_user_entity(client: TelegramClient, lead: Dict[str, Any]):
    username = (lead.get("sender_username") or "").strip().lstrip("@")
    user_id = lead.get("sender_id")
    access_hash = lead.get("sender_access_hash")

    if username:
        return await client.get_input_entity(username)
    if user_id and access_hash:
        return InputPeerUser(user_id=user_id, access_hash=access_hash)
    if user_id:
        return await client.get_input_entity(user_id)

    raise ValueError("No sender entity data")


async def send_dm_for_lead(client: TelegramClient, lead_id: str, force_regen: bool = False) -> str:
    lead = LEADS.get(lead_id)
    if not lead:
        return f"❌ Lead {lead_id} not found"

    allowed, reason = await can_send_dm(lead["session_name"])
    if not allowed:
        return f"⛔ DM blocked: {reason}"

    ai = lead.get("ai", {}) or {}
    if force_regen or not (ai.get("reply_text") or "").strip():
        ai = await ai_generate_reply(
            scenario_hint=lead["category"],
            message_text=lead["text"],
            group_title=lead["chat_title"],
            sender_name=lead.get("sender_name") or lead.get("sender_username") or "unknown",
        )
        if not ai.get("reply_text"):
            ai["reply_text"] = fallback_reply(lead["category"], detect_language(lead["text"]))
        lead["ai"] = ai
        await remember_lead(lead)

    text = (lead["ai"].get("reply_text") or "").strip()
    if not text:
        return "⛔ Empty reply_text"

    try:
        entity = await resolve_user_entity(client, lead)
        await client.send_message(entity, text)
        await mark_dm_sent(lead["session_name"])
        lead["last_dm_at"] = now_iso()
        lead["status"] = "dm_sent"
        await remember_lead(lead)
        return f"✅ DM sent for {lead_id}"
    except UserPrivacyRestrictedError:
        return "⚠️ User privacy restricted"
    except UserNotMutualContactError:
        return "⚠️ User is not mutual contact"
    except PeerFloodError:
        return "⚠️ PeerFlood"
    except FloodWaitError as e:
        return f"⚠️ FloodWait {e.seconds}s"
    except ChatWriteForbiddenError:
        return "⚠️ ChatWriteForbidden"
    except RPCError as e:
        return f"⚠️ RPC error: {type(e).__name__}: {e}"
    except Exception as e:
        return f"⚠️ Failed to send DM: {type(e).__name__}: {e}"


async def invite_lead_to_group(client: TelegramClient, lead_id: str) -> str:
    lead = LEADS.get(lead_id)
    if not lead:
        return f"❌ Lead {lead_id} not found"

    allowed, reason = await can_invite(lead["session_name"])
    if not allowed:
        return f"⛔ Invite blocked: {reason}"

    try:
        user_entity = await resolve_user_entity(client, lead)
        group_entity = await client.get_input_entity(TARGET_INVITE_GROUP)
        await client(InviteToChannelRequest(channel=group_entity, users=[user_entity]))
        await mark_invite_sent(lead["session_name"])
        lead["last_invite_at"] = now_iso()
        lead["status"] = "invited"
        await remember_lead(lead)
        return f"✅ Invited {lead_id} to {TARGET_INVITE_GROUP}"
    except UserNotMutualContactError:
        return "⚠️ UserNotMutualContact"
    except UserPrivacyRestrictedError:
        return "⚠️ UserPrivacyRestricted"
    except PeerFloodError:
        return "⚠️ PeerFlood"
    except FloodWaitError as e:
        return f"⚠️ FloodWait {e.seconds}s"
    except RPCError as e:
        return f"⚠️ RPC error: {type(e).__name__}: {e}"
    except Exception as e:
        return f"⚠️ Invite failed: {type(e).__name__}: {e}"


# =============================================================================
# MESSAGE PROCESSING
# =============================================================================

async def handle_candidate_message(client: TelegramClient, config: Dict[str, Any], event):
    if not event.raw_text:
        return

    me_id = ME_IDS.get(config["session_name"])
    sender = await event.get_sender()
    if me_id and getattr(sender, "id", None) == me_id:
        return

    event_key = f"msg:{event.chat_id}:{event.id}"
    async with PERSIST_LOCK:
        purge_seen()
        if event_key in SEEN:
            return
        SEEN[event_key] = time.time()
        save_json(SEEN_FILE, SEEN)

    text = event.raw_text.strip()
    category, rule_reason = classify_message(text)
    if category in ("ignore", "reject_spam"):
        return

    sender_username = getattr(sender, "username", None)
    sender_name = (
        f"{(getattr(sender, 'first_name', '') or '').strip()} {(getattr(sender, 'last_name', '') or '').strip()}".strip()
        or sender_username
        or str(getattr(sender, "id", "unknown"))
    )

    fp = hash_fingerprint(sender_username or "", text)
    dup_key = f"fp:{fp}"
    async with PERSIST_LOCK:
        ts = float(SEEN.get(dup_key, 0.0) or 0.0)
        if time.time() - ts < 12 * 3600:
            return
        SEEN[dup_key] = time.time()
        save_json(SEEN_FILE, SEEN)

    ai = await ai_generate_reply(
        scenario_hint=category,
        message_text=text,
        group_title=getattr(event.chat, "title", "Unknown"),
        sender_name=sender_name,
    )
    if not ai.get("reply_text") and category in ("lead_search", "lead_question", "partner_services"):
        ai["reply_text"] = fallback_reply(category, detect_language(text))
        ai["language"] = detect_language(text)

    lead = {
        "id": make_lead_id(),
        "created_at": now_iso(),
        "session_name": config["session_name"],
        "chat_id": event.chat_id,
        "chat_title": getattr(event.chat, "title", "Unknown"),
        "message_id": event.id,
        "message_link": build_message_link(event.chat, event.id),
        "sender_id": getattr(sender, "id", None),
        "sender_access_hash": getattr(sender, "access_hash", None),
        "sender_username": sender_username,
        "sender_name": sender_name,
        "text": text,
        "category": category,
        "rule_reason": rule_reason,
        "ai": ai,
        "status": "new",
    }

    await remember_lead(lead)

    async with PERSIST_LOCK:
        update_analytics_bucket(lead["chat_title"], category)
        save_json(ANALYTICS_FILE, ANALYTICS)

    card = render_lead_card(lead)
    await send_admin_notice(client, card)

    if AUTO_SEND_HIGH_CONFIDENCE and ai.get("action") != "skip" and float(ai.get("confidence", 0.0) or 0.0) >= AUTO_SEND_THRESHOLD:
        result = await send_dm_for_lead(client, lead["id"])
        await send_admin_notice(client, f"🤖 AUTO_SEND {lead['id']}: {result}")
        if AUTO_INVITE_AFTER_DM and result.startswith("✅"):
            inv = await invite_lead_to_group(client, lead["id"])
            await send_admin_notice(client, f"🤖 AUTO_INVITE {lead['id']}: {inv}")


async def handle_private_inbound(client: TelegramClient, config: Dict[str, Any], event):
    if not event.is_private or not event.raw_text:
        return

    sender = await event.get_sender()
    me_id = ME_IDS.get(config["session_name"])
    if getattr(sender, "id", None) == me_id:
        return

    if known_internal_sender(sender):
        return

    if is_service_message_text(event.raw_text):
        return

    text = (
        f"📩 PRIVATE INBOUND [{config['session_name']}]\n"
        f"From: {getattr(sender, 'first_name', '')} {getattr(sender, 'last_name', '')} "
        f"{('@' + sender.username) if getattr(sender, 'username', None) else ''}\n"
        f"id={getattr(sender, 'id', None)}\n\n"
        f"{truncate(event.raw_text, 3500)}"
    )
    await send_admin_notice(client, text)


# =============================================================================
# COMMANDS
# =============================================================================

HELP_TEXT = (
    "Команды Юстина:\n"
    "/help\n"
    "/show LEAD_ID\n"
    "/regen LEAD_ID\n"
    "/dm LEAD_ID\n"
    "/pitch LEAD_ID\n"
    "/invite LEAD_ID\n"
    "/fav LEAD_ID\n"
    "/ignore LEAD_ID\n"
    "/stats"
)


async def is_authorized_command_sender(event, session_name: str) -> bool:
    sender = await event.get_sender()
    me_id = ME_IDS.get(session_name)

    if getattr(sender, "id", None) == me_id:
        return True

    sender_username = (getattr(sender, "username", "") or "").strip().lstrip("@").lower()
    if sender_username == ADMIN_NOTIFY_USERNAME.lower():
        return True

    if getattr(sender, "id", None) in set(ME_IDS.values()):
        return True

    return False


async def handle_command(client: TelegramClient, config: Dict[str, Any], event):
    if not event.raw_text:
        return

    text = event.raw_text.strip()
    if not text.startswith("/"):
        return

    if not event.is_private:
        return

    if not await is_authorized_command_sender(event, config["session_name"]):
        return

    parts = text.split(maxsplit=1)
    cmd = parts[0].lower()
    arg = parts[1].strip() if len(parts) > 1 else ""

    if cmd == "/help":
        await event.reply(HELP_TEXT)
        return

    if cmd == "/stats":
        s = OUTBOUND_STATS.get(config["session_name"], {})
        msg = (
            f"Stats [{config['session_name']}]\n"
            f"DM day {_day_key()}: {int((s.get('dm_day') or {}).get(_day_key(), 0))}/{OUTBOUND_DM_PER_DAY}\n"
            f"DM hour {_hour_key()}: {int((s.get('dm_hour') or {}).get(_hour_key(), 0))}/{OUTBOUND_DM_PER_HOUR}\n"
            f"Invite day {_day_key()}: {int((s.get('invite_day') or {}).get(_day_key(), 0))}/{INVITE_PER_DAY}"
        )
        await event.reply(msg)
        return

    if not arg:
        await event.reply("Нужен LEAD_ID")
        return

    lead = LEADS.get(arg)
    if not lead:
        await event.reply(f"Lead {arg} not found")
        return

    target_client = CLIENTS.get(lead["session_name"])
    if not target_client:
        await event.reply(f"Client for {lead['session_name']} not found")
        return

    if cmd == "/show":
        await event.reply(render_lead_card(lead))
        return

    if cmd == "/regen":
        ai = await ai_generate_reply(
            scenario_hint=lead["category"],
            message_text=lead["text"],
            group_title=lead["chat_title"],
            sender_name=lead.get("sender_name") or lead.get("sender_username") or "unknown",
        )
        if not ai.get("reply_text"):
            ai["reply_text"] = fallback_reply(lead["category"], detect_language(lead["text"]))
        lead["ai"] = ai
        await remember_lead(lead)
        await event.reply(f"✅ Regenerated for {arg}\n\n{truncate(ai.get('reply_text', ''), 3500)}")
        return

    if cmd in ("/dm", "/pitch"):
        res = await send_dm_for_lead(target_client, arg, force_regen=False)
        await event.reply(res)
        return

    if cmd == "/invite":
        res = await invite_lead_to_group(target_client, arg)
        await event.reply(res)
        return

    if cmd == "/fav":
        ok = await remember_favorite(arg)
        if ok:
            lead = LEADS[arg]
            fav_text = (
                f"⭐ FAVORITE {arg}\n"
                f"{lead.get('sender_name', '')} {('@' + lead['sender_username']) if lead.get('sender_username') else ''}\n"
                f"{lead.get('message_link', '')}\n\n"
                f"{truncate(lead.get('text', ''), 3000)}"
            )
            await send_admin_notice(target_client, fav_text)
            await event.reply(f"✅ Saved to favorites: {arg}")
        else:
            await event.reply(f"❌ Failed to save favorite: {arg}")
        return

    if cmd == "/ignore":
        lead["status"] = "ignored"
        await remember_lead(lead)
        await event.reply(f"✅ Ignored {arg}")
        return


# =============================================================================
# LIFECYCLE
# =============================================================================

def _handle_signal():
    shutdown.set()


async def run_client_forever(config: Dict[str, Any]):
    session_name = config["session_name"]

    if not config["api_id"]:
        logging.error("[%s] Missing api_id. Set TG_API_ID_1 / TG_API_ID_2 env vars.", session_name)
        return
    if not config["api_hash"]:
        logging.error("[%s] Missing api_hash. Set TG_API_HASH_1 / TG_API_HASH_2 env vars.", session_name)
        return

    backoff = 5

    while not shutdown.is_set():
    client = None
    try:
        session_path = os.path.join(SESSION_DIR, session_name)

        legacy_candidates = [
            f"{session_name}.session",
            os.path.join(DATA_DIR, f"{session_name}.session"),
        ]

        new_session_file = f"{session_path}.session"
        new_journal_file = f"{session_path}.session-journal"

        if not os.path.exists(new_session_file):
            for old_file in legacy_candidates:
                old_journal = f"{old_file}-journal"
                if os.path.exists(old_file):
                    try:
                        os.replace(old_file, new_session_file)
                        if os.path.exists(old_journal):
                            os.replace(old_journal, new_journal_file)
                        logging.info("[%s] Migrated legacy session: %s -> %s", session_name, old_file, new_session_file)
                        break
                    except Exception as e:
                        logging.warning("[%s] Failed to migrate legacy session from %s: %s", session_name, old_file, e)

        client = TelegramClient(session_path, config["api_id"], config["api_hash"])
        await client.connect()

            if not await client.is_user_authorized():
                logging.error("[%s] Session is not authorized. Authorize locally first.", session_name)
                await asyncio.sleep(backoff)
                continue

            me = await client.get_me()
            ME_IDS[session_name] = me.id
            CLIENTS[session_name] = client
            logging.info("[%s] Connected as @%s", session_name, getattr(me, "username", None))

            entities = await load_or_fetch_entities(client, GROUPS_TO_MONITOR)
            logging.info("[%s] Monitoring %s chats", session_name, len(entities))

            @client.on(events.NewMessage(chats=entities, incoming=True))
            async def group_handler(event):
                try:
                    await handle_candidate_message(client, config, event)
                except Exception:
                    logging.exception("[%s] group_handler failed", session_name)

            @client.on(events.NewMessage(incoming=True))
            async def private_inbound_handler(event):
                try:
                    await handle_private_inbound(client, config, event)
                except Exception:
                    logging.exception("[%s] private_inbound_handler failed", session_name)

            @client.on(events.NewMessage())
            async def command_handler(event):
                try:
                    await handle_command(client, config, event)
                except Exception:
                    logging.exception("[%s] command_handler failed", session_name)

            backoff = 5
            await client.run_until_disconnected()

        except Exception as e:
            logging.critical("[%s] Critical error: %s", session_name, e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        finally:
            CLIENTS.pop(session_name, None)
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass


async def main():
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(loop_exc_handler)

    import signal
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass

    valid_accounts = [
        cfg for cfg in ACCOUNTS
        if cfg.get("api_id") and cfg.get("api_hash")
    ]

    if not valid_accounts:
        logging.critical("No valid Telegram accounts configured")
        return

    tasks = [asyncio.create_task(run_client_forever(cfg)) for cfg in valid_accounts]
    await shutdown.wait()

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
