import asyncio
import json
import logging
import os
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from openai import AsyncOpenAI
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.errors.common import TypeNotFoundError
from telethon.sessions import StringSession
from telethon.tl.types import PeerUser, User

# =========================
# Logging
# =========================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

try:
    import telethon
    logger.info("Telethon version: %s", telethon.__version__)
except Exception:
    pass

# =========================
# Paths / storage
# =========================
BASE_DIR = Path(os.getenv("DATA_DIR", ".")).resolve()
BASE_DIR.mkdir(parents=True, exist_ok=True)
ANALYTICS_PATH = BASE_DIR / os.getenv("ANALYTICS_FILE", "analytics.json")
SEEN_MESSAGES_PATH = BASE_DIR / os.getenv("SEEN_MESSAGES_FILE", "seen_messages.json")

# =========================
# Env helpers
# =========================
def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if not value:
        return default
    try:
        return int(value)
    except Exception:
        return default


# =========================
# Config
# =========================
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-mini").strip()
OPENAI_TIMEOUT_SEC = env_int("OPENAI_TIMEOUT_SEC", 45)
ENABLE_OPENAI = env_bool("ENABLE_OPENAI", True)

ALERT_CHAT = os.getenv("ALERT_CHAT", "me").strip() or "me"
AUTO_REPLY_TO_GROUP = env_bool("AUTO_REPLY_TO_GROUP", False)
SEND_DRAFT_TO_ALERT = env_bool("SEND_DRAFT_TO_ALERT", True)
MAX_TEXT_LEN_FOR_MODEL = env_int("MAX_TEXT_LEN_FOR_MODEL", 3500)
LEAD_SCORE_THRESHOLD = env_int("LEAD_SCORE_THRESHOLD", 45)
REPLY_MIN_SCORE = env_int("REPLY_MIN_SCORE", 60)
SEEN_CACHE_LIMIT = env_int("SEEN_CACHE_LIMIT", 5000)
MAX_ALERT_TEXT = env_int("MAX_ALERT_TEXT", 1200)
IGNORE_OWN_MESSAGES = env_bool("IGNORE_OWN_MESSAGES", True)

RAW_MONITORED_CHATS = os.getenv("MONITORED_CHATS", "")
MONITORED_CHATS = [c.strip() for c in RAW_MONITORED_CHATS.split(",") if c.strip()]

RAW_REPLY_TARGET_CATEGORIES = os.getenv(
    "REPLY_TARGET_CATEGORIES",
    "LEGAL_HELP,MIGRATION_HELP,FAMILY_LAW,STATUS_PROBLEMS,DOCUMENTS,COURT,RESIDENCE",
)
REPLY_TARGET_CATEGORIES = {x.strip().upper() for x in RAW_REPLY_TARGET_CATEGORIES.split(",") if x.strip()}

LAWYER_PROFILE = {
    "name": "Andrii Bilytskyi",
    "title": "Ukrainian lawyer / Rechtsanwalt-style legal consultant for Ukrainian-speaking clients in Germany and EU",
    "site": "https://www.andriibilytskyi.com",
    "anwalt_profile": "https://www.anwalt.de/andrii-bilytskyi",
    "telegram": "https://t.me/advocate_ua_1",
    "languages": ["Ukrainian", "Russian", "German"],
    "focus": [
        "immigration and residence issues",
        "family law and children-related disputes",
        "recognition and enforcement of foreign decisions",
        "administrative matters in Germany",
        "cross-border legal issues for Ukrainians in Germany and EU",
    ],
    "positioning": [
        "calm, respectful, professional",
        "never promise a result",
        "offer a short orientation and invite private contact",
        "do not sound spammy or aggressive",
        "reply like a real lawyer, not a generic sales bot",
    ],
}

SYSTEM_STYLE = (
    "You are assisting lawyer Andrii Bilytskyi. "
    "Write concise, human, credible replies for Ukrainian/Russian-speaking Telegram audiences in Germany. "
    "Tone: calm, professional, empathetic, non-pushy. "
    "Never invent facts, never promise an outcome, never claim representation has already started. "
    "Prefer Ukrainian if the incoming message is Ukrainian, Russian if Russian, otherwise simple German or Ukrainian."
)

CATEGORY_RULES: Dict[str, Dict[str, Any]] = {
    "LEGAL_HELP": {
        "score": 30,
        "patterns": [
            r"\bадвокат\b", r"\bюрист\b", r"\brechtsanwalt\b", r"\banwalt\b",
            r"\bсуд\b", r"\bиск\b", r"\bпозов\b", r"\bklage\b", r"\bgericht\b",
            r"\bжалоб[аы]\b", r"\bbeschwerde\b", r"\bbeh[oö]rde\b", r"\bamtsgericht\b",
            r"\bausl[aä]nderbeh[oö]rde\b", r"\bjobcenter\b", r"\bjugendamt\b",
            r"\bunterhalt\b", r"\baliment", r"\bkindergeld\b", r"\buvg\b",
            r"\bвнж\b", r"\bпмж\b", r"\baufenthalt\b", r"\baufenthaltstitel\b",
            r"\bparagraph 24\b", r"\b§\s*24\b", r"\bдокумент[ыа]?\b", r"\bразвод\b",
            r"\bопек[ауы]?\b", r"\bалименты\b", r"\bнаследств", r"\bдоверенност",
        ],
    },
    "MIGRATION_HELP": {
        "score": 24,
        "patterns": [
            r"\bвиз[аы]\b", r"\bвнж\b", r"\bпараграф\s*24\b", r"\bубежищ", r"\bбежен",
            r"\baufenthalt\b", r"\baufenthaltserlaubnis\b", r"\bfiktionsbescheinigung\b",
            r"\btermin\b.*\bausl[aä]nderbeh[oö]rde\b", r"\beinb[uü]rgerung\b",
            r"\bblu\s*card\b", r"\bсняли\s+с\s+регистрац", r"\bрегистрац[ияи]\b",
        ],
    },
    "FAMILY_LAW": {
        "score": 24,
        "patterns": [
            r"\bдет(и|ей|ям|ьми)\b", r"\bребен", r"\bопек", r"\bотец\b", r"\bмать\b",
            r"\bразвод\b", r"\bумг\b", r"\bумганг\b", r"\bumgang\b", r"\bsorgerecht\b",
            r"\bkindeswohl\b", r"\bjugendamt\b", r"\bалименты\b", r"\bunterhalt\b",
            r"\bконтакт\s+с\s+ребен", r"\bлишили\s+прав", r"\bвывезл[аи]\s+ребен",
        ],
    },
    "STATUS_PROBLEMS": {
        "score": 18,
        "patterns": [
            r"\bотказ\b", r"\bотказали\b", r"\bпроблем[аы]\b.*\bдокумент", r"\bне\s+дают\b",
            r"\bне\s+отвечают\b", r"\bне\s+принимают\b", r"\bblockiert\b", r"\babgelehnt\b",
            r"\bkeine\s+antwort\b", r"\btermin\s+не\s+дают\b", r"\bwiderspruch\b",
        ],
    },
    "DOCUMENTS": {
        "score": 16,
        "patterns": [
            r"\bдоверенность\b", r"\bперевод\b", r"\bапостил", r"\bнотари", r"\bдокумент",
            r"\bсправк", r"\bсвидетельств", r"\bpassport\b", r"\breisepass\b",
            r"\bрегистрац", r"\banmeldung\b", r"\bummeldung\b",
        ],
    },
    "COURT": {
        "score": 18,
        "patterns": [
            r"\bсуд\b", r"\bgericht\b", r"\bklage\b", r"\bprozess\b", r"\bапелляц",
            r"\bbeschluss\b", r"\burteil\b", r"\bисполнительн", r"\bvollstreck",
        ],
    },
    "PARTNER_SERVICES": {
        "score": 10,
        "patterns": [
            r"\bстрахов", r"\bversicherung\b", r"\bimmobil", r"\bmakler\b",
            r"\bпереводчик\b", r"\bsteuerberater\b", r"\bbuchhalter\b",
            r"\bконсультац(ия|ии)\s+бесплатн", r"\bпомогу\s+оформить\b",
        ],
    },
    "CTA": {
        "score": 8,
        "patterns": [
            r"\bкто\s+сталкивал", r"\bподскажите\b", r"\bкто\s+знает\b",
            r"\bнужен\b.*\bадвокат\b", r"\bищу\b.*\bюрист\b", r"\bhelp\b",
            r"\bпосоветуйте\b", r"\bможно\s+ли\b", r"\bчто\s+делать\b",
        ],
    },
}

NEGATIVE_RULES: List[Tuple[re.Pattern[str], int, str]] = [
    (re.compile(r"\bпродам\b|\bverkaufe\b|\bпродаю\b", re.I), 20, "sales"),
    (re.compile(r"\bработа\b|\bvakanz|\bваканси|\bищем\s+сотрудник", re.I), 15, "job_ad"),
    (re.compile(r"\bдоставка\b|\bпосылк|\bперевозк|\bbus\b|\bбус\b", re.I), 15, "delivery"),
    (re.compile(r"\bскидк|\bакци|\bреклама\b|\breklam", re.I), 12, "promo"),
]


# =========================
# Data helpers
# =========================
def load_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("Failed loading %s: %s", path.name, e)
        return default


def save_json_file(path: Path, data: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


ANALYTICS: Dict[str, Any] = load_json_file(ANALYTICS_PATH, {})
SEEN_MESSAGES: Dict[str, float] = load_json_file(SEEN_MESSAGES_PATH, {})


def normalize_analytics_structure() -> None:
    global ANALYTICS
    if not isinstance(ANALYTICS, dict):
        ANALYTICS = {}

    migrated = False
    for group_name, group_data in list(ANALYTICS.items()):
        if not isinstance(group_data, dict):
            ANALYTICS[group_name] = {
                "total": 0,
                "categories": {},
                "last_seen": None,
                "last_text": "",
            }
            migrated = True
            continue

        if "total" not in group_data or not isinstance(group_data.get("total"), int):
            group_data["total"] = int(group_data.get("total") or 0)
            migrated = True

        if "categories" not in group_data or not isinstance(group_data.get("categories"), dict):
            old_cats = group_data.get("category_counts") or {}
            group_data["categories"] = old_cats if isinstance(old_cats, dict) else {}
            migrated = True

        if "last_seen" not in group_data:
            group_data["last_seen"] = None
            migrated = True

        if "last_text" not in group_data:
            group_data["last_text"] = ""
            migrated = True

    if migrated:
        save_json_file(ANALYTICS_PATH, ANALYTICS)
        logger.info("analytics.json migrated to current schema")


normalize_analytics_structure()


def ensure_analytics_bucket(group_name: str) -> Dict[str, Any]:
    if group_name not in ANALYTICS or not isinstance(ANALYTICS[group_name], dict):
        ANALYTICS[group_name] = {}

    bucket = ANALYTICS[group_name]
    if not isinstance(bucket.get("total"), int):
        bucket["total"] = int(bucket.get("total") or 0)
    if not isinstance(bucket.get("categories"), dict):
        old_cats = bucket.get("category_counts") or {}
        bucket["categories"] = old_cats if isinstance(old_cats, dict) else {}
    if "last_seen" not in bucket:
        bucket["last_seen"] = None
    if "last_text" not in bucket:
        bucket["last_text"] = ""
    return bucket


def update_analytics_bucket(group_name: str, category: str, text: str = "") -> None:
    bucket = ensure_analytics_bucket(group_name)
    bucket["total"] += 1
    bucket["categories"][category] = bucket["categories"].get(category, 0) + 1
    bucket["last_seen"] = int(time.time())
    if text:
        bucket["last_text"] = text[:300]
    save_json_file(ANALYTICS_PATH, ANALYTICS)


def make_seen_key(chat_id: int, message_id: int) -> str:
    return f"{chat_id}:{message_id}"


def is_seen(chat_id: int, message_id: int) -> bool:
    return make_seen_key(chat_id, message_id) in SEEN_MESSAGES


def mark_seen(chat_id: int, message_id: int) -> None:
    SEEN_MESSAGES[make_seen_key(chat_id, message_id)] = time.time()
    if len(SEEN_MESSAGES) > SEEN_CACHE_LIMIT:
        items = sorted(SEEN_MESSAGES.items(), key=lambda x: x[1], reverse=True)[:SEEN_CACHE_LIMIT]
        SEEN_MESSAGES.clear()
        SEEN_MESSAGES.update(dict(items))
    save_json_file(SEEN_MESSAGES_PATH, SEEN_MESSAGES)


# =========================
# OpenAI client
# =========================
OPENAI_CLIENT: Optional[AsyncOpenAI] = None
if OPENAI_API_KEY and ENABLE_OPENAI:
    try:
        OPENAI_CLIENT = AsyncOpenAI(api_key=OPENAI_API_KEY, timeout=OPENAI_TIMEOUT_SEC)
    except Exception as e:
        logger.warning("Failed to initialize OpenAI client: %s", e)
        OPENAI_CLIENT = None


# =========================
# Session config
# =========================
@dataclass
class SessionConfig:
    name: str
    api_id: int
    api_hash: str
    session_value: str
    monitored_chats: List[str]


@dataclass
class SenderInfo:
    sender_id: Optional[int]
    username: Optional[str]
    display_name: str
    is_bot: bool


@dataclass
class LeadResult:
    category: str
    score: int
    reasons: List[str]
    keywords: List[str]
    needs_reply: bool
    draft_reply: str
    source: str


# =========================
# Helpers
# =========================
def parse_session_config(index: int) -> Optional[SessionConfig]:
    api_id = os.getenv(f"TG_API_ID_{index}", "").strip()
    api_hash = os.getenv(f"TG_API_HASH_{index}", "").strip()
    session_string = os.getenv(f"TG_SESSION_STRING_{index}", "").strip()

    if not api_id and not api_hash and not session_string:
        return None

    if not api_hash:
        logger.error("[session%s] Missing api_hash. Set TG_API_HASH_%s env var.", index, index)
        return None
    if not api_id:
        logger.error("[session%s] Missing api_id. Set TG_API_ID_%s env var.", index, index)
        return None
    if not session_string:
        logger.error("[session%s] Missing session string. Set TG_SESSION_STRING_%s env var.", index, index)
        return None

    try:
        api_id_int = int(api_id)
    except ValueError:
        logger.error("[session%s] Invalid api_id: %s", index, api_id)
        return None

    return SessionConfig(
        name=f"session{index}",
        api_id=api_id_int,
        api_hash=api_hash,
        session_value=session_string,
        monitored_chats=MONITORED_CHATS,
    )


def build_clients() -> List[Tuple[TelegramClient, SessionConfig]]:
    clients: List[Tuple[TelegramClient, SessionConfig]] = []
    for i in range(1, 10):
        config = parse_session_config(i)
        if not config:
            continue
        client = TelegramClient(StringSession(config.session_value), config.api_id, config.api_hash)
        clients.append((client, config))
    return clients


CLIENTS = build_clients()
if not CLIENTS:
    logger.warning("No Telegram sessions configured")


def compact_text(text: str, limit: int = MAX_ALERT_TEXT) -> str:
    text = re.sub(r"\s+", " ", (text or "").strip())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def trim_for_model(text: str) -> str:
    text = (text or "").strip()
    return text if len(text) <= MAX_TEXT_LEN_FOR_MODEL else text[:MAX_TEXT_LEN_FOR_MODEL]


def detect_language(text: str) -> str:
    if re.search(r"[іїєґІЇЄҐ]", text):
        return "uk"
    if re.search(r"[а-яА-ЯёЁ]", text):
        return "ru"
    if re.search(r"[äöüß]", text.lower()):
        return "de"
    return "auto"


def build_message_link(chat_username: Optional[str], chat_id: Optional[int], message_id: int) -> str:
    if chat_username:
        chat_username = chat_username.lstrip("@")
        return f"https://t.me/{chat_username}/{message_id}"
    if chat_id:
        raw = str(chat_id)
        if raw.startswith("-100"):
            raw = raw[4:]
        else:
            raw = raw.lstrip("-")
        return f"https://t.me/c/{raw}/{message_id}"
    return "(no link)"


def normalize_category(raw: str) -> str:
    value = (raw or "LEGAL_HELP").strip().upper()
    return value if value else "LEGAL_HELP"


def is_target_category(category: str) -> bool:
    return normalize_category(category) in REPLY_TARGET_CATEGORIES


# =========================
# Telethon safe sender/chat extraction
# =========================
async def safe_get_sender_info(client: TelegramClient, event: events.NewMessage.Event) -> SenderInfo:
    sender_id = getattr(event, "sender_id", None)

    sender_obj = getattr(event, "sender", None)
    if isinstance(sender_obj, User):
        username = getattr(sender_obj, "username", None)
        name_parts = [getattr(sender_obj, "first_name", None), getattr(sender_obj, "last_name", None)]
        display_name = " ".join([x for x in name_parts if x]).strip() or username or f"id={sender_id}"
        return SenderInfo(
            sender_id=sender_id,
            username=(f"@{username}" if username else None),
            display_name=display_name,
            is_bot=bool(getattr(sender_obj, "bot", False)),
        )

    if sender_id:
        try:
            entity = await client.get_entity(PeerUser(sender_id))
            username = getattr(entity, "username", None)
            name_parts = [getattr(entity, "first_name", None), getattr(entity, "last_name", None)]
            display_name = " ".join([x for x in name_parts if x]).strip() or username or f"id={sender_id}"
            return SenderInfo(
                sender_id=sender_id,
                username=(f"@{username}" if username else None),
                display_name=display_name,
                is_bot=bool(getattr(entity, "bot", False)),
            )
        except TypeNotFoundError as e:
            logger.warning("safe_get_sender_info TypeNotFoundError for sender_id=%s: %s", sender_id, e)
        except Exception as e:
            logger.warning("safe_get_sender_info failed for sender_id=%s: %s", sender_id, e)

    return SenderInfo(
        sender_id=sender_id,
        username=None,
        display_name=f"id={sender_id}" if sender_id else "unknown",
        is_bot=False,
    )


async def safe_get_chat_meta(event: events.NewMessage.Event) -> Tuple[Optional[int], Optional[str], str]:
    chat = await event.get_chat()
    chat_id = getattr(event, "chat_id", None)
    chat_username = getattr(chat, "username", None)
    chat_title = getattr(chat, "title", None) or chat_username or str(chat_id)
    if chat_username and not str(chat_username).startswith("@"):
        chat_username = f"@{chat_username}"
    return chat_id, chat_username, chat_title


# =========================
# Rules-based classifier
# =========================
def rules_classify(text: str) -> LeadResult:
    hay = (text or "").strip()
    score = 0
    reasons: List[str] = []
    matched_keywords: List[str] = []
    category_scores: Dict[str, int] = defaultdict(int)

    for category, cfg in CATEGORY_RULES.items():
        category_base = cfg.get("score", 0)
        local_matches = 0
        for pattern in cfg.get("patterns", []):
            if re.search(pattern, hay, flags=re.I):
                local_matches += 1
                matched_keywords.append(pattern)
        if local_matches:
            gained = category_base + max(0, local_matches - 1) * 4
            category_scores[category] += gained
            score += gained
            reasons.append(f"{category}:{local_matches}")

    for pattern, penalty, reason in NEGATIVE_RULES:
        if pattern.search(hay):
            score -= penalty
            reasons.append(f"-{reason}")

    if len(hay) < 15:
        score -= 8
        reasons.append("-too_short")

    if re.search(r"\b(подскажите|кто знает|можно ли|что делать|посоветуйте)\b", hay, flags=re.I):
        score += 6
        reasons.append("question_intent")

    best_category = "OTHER"
    if category_scores:
        best_category = max(category_scores.items(), key=lambda x: x[1])[0]

    if best_category == "CTA" and score >= LEAD_SCORE_THRESHOLD:
        best_category = "LEGAL_HELP"

    needs_reply = score >= REPLY_MIN_SCORE and is_target_category(best_category)

    return LeadResult(
        category=best_category,
        score=max(0, min(100, score)),
        reasons=reasons[:10],
        keywords=matched_keywords[:12],
        needs_reply=needs_reply,
        draft_reply="",
        source="rules",
    )


# =========================
# OpenAI enrichment
# =========================
async def generate_openai_json(text: str, rules_result: LeadResult) -> Optional[Dict[str, Any]]:
    if not OPENAI_CLIENT:
        return None

    lang = detect_language(text)
    prompt_text = trim_for_model(text)
    user_prompt = (
        "Analyze the Telegram message and return valid JSON only. "
        "The output must be JSON. No markdown, no commentary, no code fences.\n\n"
        "Required JSON schema:\n"
        "{\n"
        '  "category": "LEGAL_HELP|MIGRATION_HELP|FAMILY_LAW|STATUS_PROBLEMS|DOCUMENTS|COURT|PARTNER_SERVICES|OTHER",\n'
        '  "score": 0-100,\n'
        '  "reasons": ["short reason"],\n'
        '  "needs_reply": true_or_false,\n'
        '  "draft_reply": "short realistic human reply",\n'
        '  "language": "uk|ru|de|auto"\n'
        "}\n\n"
        f"Lawyer profile context: {json.dumps(LAWYER_PROFILE, ensure_ascii=False)}\n"
        f"Rules pre-analysis: {json.dumps({ 'category': rules_result.category, 'score': rules_result.score, 'reasons': rules_result.reasons }, ensure_ascii=False)}\n"
        f"Message language guess: {lang}\n"
        "Reply constraints for draft_reply:\n"
        "- sound natural and credible\n"
        "- 2-5 sentences\n"
        "- mention the lawyer can look at the situation and suggest next steps\n"
        "- invite private contact only if appropriate\n"
        "- no guarantees, no aggressive sales tone\n\n"
        f"Message:\n{prompt_text}"
    )

    try:
        resp = await OPENAI_CLIENT.responses.create(
            model=OPENAI_MODEL,
            input=[
                {"role": "system", "content": SYSTEM_STYLE},
                {"role": "user", "content": user_prompt},
            ],
            text={"format": {"type": "json_object"}},
        )
        output = getattr(resp, "output_text", None)
        if not output:
            return None
        return json.loads(output)
    except Exception as e:
        logger.warning("OpenAI json_object failed: %s", e)
        return None


async def enrich_lead_with_openai(text: str, rules_result: LeadResult) -> LeadResult:
    data = await generate_openai_json(text, rules_result)
    if not data:
        return rules_result

    category = normalize_category(str(data.get("category") or rules_result.category))
    score = data.get("score", rules_result.score)
    try:
        score = int(score)
    except Exception:
        score = rules_result.score
    score = max(0, min(100, score))

    reasons = data.get("reasons")
    if not isinstance(reasons, list):
        reasons = rules_result.reasons
    reasons = [str(x) for x in reasons][:10]

    draft_reply = str(data.get("draft_reply") or "").strip()
    needs_reply = bool(data.get("needs_reply", rules_result.needs_reply))

    if not is_target_category(category):
        needs_reply = False

    return LeadResult(
        category=category,
        score=score,
        reasons=reasons,
        keywords=rules_result.keywords,
        needs_reply=needs_reply,
        draft_reply=draft_reply,
        source="openai",
    )


# =========================
# Reply fallback
# =========================
def fallback_reply(text: str, category: str) -> str:
    lang = detect_language(text)
    if lang == "uk":
        return (
            "Вітаю. Ситуація схожа на ту, де важливі документи й конкретні обставини. "
            "Можу коротко подивитися, які саме кроки тут доцільні в Німеччині або у транскордонному контексті. "
            "Якщо зручно, напишіть у приватні повідомлення та надішліть короткий опис і наявні документи: "
            f"{LAWYER_PROFILE['telegram']}"
        )
    if lang == "de":
        return (
            "Guten Tag. Das klingt nach einer Frage, bei der die Unterlagen und die genaue Situation entscheidend sind. "
            "Ich kann mir das kurz ansehen und einschätzen, welche nächsten Schritte in Deutschland sinnvoll sind. "
            f"Schreiben Sie mir bei Bedarf privat: {LAWYER_PROFILE['telegram']}"
        )
    return (
        "Здравствуйте. Похоже, здесь важны детали и документы, потому что от них зависит правильный следующий шаг. "
        "Могу кратко посмотреть ситуацию и подсказать, как лучше действовать в Германии или в трансграничном вопросе. "
        f"Если удобно, напишите в личные сообщения: {LAWYER_PROFILE['telegram']}"
    )


# =========================
# Alert formatting
# =========================
def format_alert(
    session_name: str,
    chat_title: str,
    link: str,
    sender: SenderInfo,
    lead: LeadResult,
    text: str,
) -> str:
    keywords_line = ", ".join(lead.keywords[:6]) if lead.keywords else "-"
    reasons_line = ", ".join(lead.reasons[:8]) if lead.reasons else "-"
    sender_label = sender.username or sender.display_name
    draft = lead.draft_reply.strip() if lead.draft_reply.strip() else fallback_reply(text, lead.category)
    return (
        f"🧠 Lead | {lead.category} (score={lead.score})\n"
        f"📡 {session_name} | {chat_title}\n"
        f"🔗 {link}\n"
        f"👤 {sender_label} id={sender.sender_id}\n"
        f"🏷 keywords: {keywords_line}\n"
        f"🧩 reasons: {reasons_line}\n"
        f"📝 text:\n{compact_text(text, MAX_ALERT_TEXT)}\n\n"
        f"💬 draft:\n{draft}"
    )


# =========================
# Message processing
# =========================
async def send_alert(client: TelegramClient, text: str) -> None:
    try:
        await client.send_message(ALERT_CHAT, text)
    except FloodWaitError as e:
        logger.warning("FloodWait while sending alert: sleep %ss", e.seconds)
        await asyncio.sleep(e.seconds + 1)
        await client.send_message(ALERT_CHAT, text)
    except Exception as e:
        logger.error("Failed to send alert: %s", e)


async def maybe_send_group_reply(
    event: events.NewMessage.Event,
    lead: LeadResult,
    original_text: str,
) -> None:
    if not AUTO_REPLY_TO_GROUP:
        return
    if lead.score < REPLY_MIN_SCORE or not lead.needs_reply:
        return

    draft = lead.draft_reply.strip() or fallback_reply(original_text, lead.category)
    try:
        await event.reply(draft)
    except FloodWaitError as e:
        logger.warning("FloodWait while replying to group: sleep %ss", e.seconds)
        await asyncio.sleep(e.seconds + 1)
        await event.reply(draft)
    except Exception as e:
        logger.error("Failed group reply: %s", e)


async def classify_message(text: str) -> LeadResult:
    rules_result = rules_classify(text)

    if rules_result.score < max(20, LEAD_SCORE_THRESHOLD - 15):
        return rules_result

    enriched = await enrich_lead_with_openai(text, rules_result)
    if not enriched.draft_reply and enriched.needs_reply:
        enriched.draft_reply = fallback_reply(text, enriched.category)
    return enriched


async def handle_candidate_message(
    client: TelegramClient,
    config: SessionConfig,
    event: events.NewMessage.Event,
) -> None:
    if not event.message:
        return
    if getattr(event.message, "message", None) is None:
        return
    if not getattr(event, "is_group", False) and not getattr(event, "is_channel", False):
        return

    text = (event.raw_text or "").strip()
    if not text:
        return

    chat_id, chat_username, chat_title = await safe_get_chat_meta(event)
    message_id = int(event.message.id)

    if chat_id is not None and is_seen(chat_id, message_id):
        return

    sender = await safe_get_sender_info(client, event)
    if sender.is_bot:
        return

    me = await client.get_me()
    if IGNORE_OWN_MESSAGES and sender.sender_id and getattr(me, "id", None) == sender.sender_id:
        return

    lead = await classify_message(text)

    if lead.score < LEAD_SCORE_THRESHOLD:
        if chat_id is not None:
            mark_seen(chat_id, message_id)
        return

    update_analytics_bucket(chat_title, lead.category, text)

    link = build_message_link(chat_username, chat_id, message_id)
    alert_text = format_alert(config.name, chat_title, link, sender, lead, text)

    if SEND_DRAFT_TO_ALERT:
        await send_alert(client, alert_text)

    await maybe_send_group_reply(event, lead, text)

    if chat_id is not None:
        mark_seen(chat_id, message_id)


# =========================
# Startup / caching
# =========================
async def cache_monitored_entities(client: TelegramClient, config: SessionConfig) -> None:
    ok = 0
    for chat_ref in config.monitored_chats:
        try:
            entity = await client.get_entity(chat_ref)
            username = getattr(entity, "username", None)
            logger.info("✅ cache entity: @%s", username or chat_ref)
            ok += 1
        except Exception as e:
            logger.warning("⚠️ failed cache entity %s: %s", chat_ref, e)
    logger.info("[%s] Monitoring %s chats", config.name, ok)


async def register_handlers(client: TelegramClient, config: SessionConfig) -> None:
    @client.on(events.NewMessage(chats=config.monitored_chats))
    async def group_handler(event: events.NewMessage.Event) -> None:
        try:
            await handle_candidate_message(client, config, event)
        except TypeNotFoundError as e:
            logger.warning("[%s] group_handler TypeNotFoundError skipped: %s", config.name, e)
        except Exception:
            logger.exception("[%s] group_handler failed", config.name)


# =========================
# Main
# =========================
async def start_one(client: TelegramClient, config: SessionConfig) -> None:
    await client.connect()
    if not await client.is_user_authorized():
        logger.error("[%s] Session is not authorized. Recreate TG_SESSION_STRING_%s.", config.name, config.name[-1])
        return

    me = await client.get_me()
    username = getattr(me, "username", None)
    logger.info("[%s] Connected as @%s", config.name, username or getattr(me, "id", "unknown"))

    await cache_monitored_entities(client, config)
    await register_handlers(client, config)


async def main() -> None:
    if not MONITORED_CHATS:
        logger.warning("MONITORED_CHATS is empty. Set usernames separated by commas.")

    if not CLIENTS:
        logger.error("No valid Telegram clients configured. Exiting.")
        return

    for client, config in CLIENTS:
        try:
            await start_one(client, config)
        except Exception:
            logger.exception("Failed to start %s", config.name)

    await asyncio.gather(*(client.run_until_disconnected() for client, _ in CLIENTS))


if __name__ == "__main__":
    asyncio.run(main())
