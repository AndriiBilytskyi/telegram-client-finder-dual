#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telethon monitor + "Юстин" (semi-automatic workflow, manual actions via /dm /pitch /invite /fav)

What it does:
- Monitors many Telegram groups/channels for new messages.
- Classifies each matched message into:
    LEAD_SEARCH / LEAD_QUESTION / PARTNER_SERVICES / COMPETITOR_LAWYER / SPAM_SCAM / OFFTOP_JOBS_SELL / OTHER
- Sends YOU a lead card with:
    - link, sender, text, class, reasons
    - draft message (Justin reply or partner pitch)
    - commands to execute (manual): /dm <id>, /pitch <id>, /invite <id>, /fav <id>, /ignore <id>, /show <id>
- On /dm: sends Justin’s prepared reply to that user in private.
- On /pitch: sends partner pitch to that user in private.
- On /invite: tries to invite the user to TARGET_INVITE_GROUP.
- On /fav: adds user to favorites.json.
- On private inbound messages: forwards to admin + optional one-time auto-ack with contacts.

IMPORTANT:
- No auto mass DM. All outreach is via explicit command by you (semi-automatic, safer).
"""

import os
import sys
import re
import json
import time
import uuid
import pickle
import signal
import asyncio
import logging
import traceback
from dataclasses import dataclass
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional, Tuple

import telethon
from telethon import TelegramClient, events, functions
from telethon.errors import (
    PeerFloodError,
    UserPrivacyRestrictedError,
    UserAlreadyParticipantError,
    ChatAdminRequiredError,
    FloodWaitError,
    RPCError,
)

# =============================================================================
# LOGGING
# =============================================================================
LOG_PATH = os.getenv("LOG_PATH", "/data/justin_monitor.log")
try:
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
except Exception:
    pass

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

fh = RotatingFileHandler(LOG_PATH, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

root_logger.handlers = [fh, sh]


def excepthook(exc_type, exc, tb):
    logging.critical("UNCAUGHT EXCEPTION:\n%s", "".join(traceback.format_exception(exc_type, exc, tb)))


sys.excepthook = excepthook


def loop_exc_handler(loop, context):
    msg = context.get("exception") or context.get("message")
    logging.critical("ASYNC EXCEPTION: %s", msg, exc_info=context.get("exception"))


print("Telethon version:", getattr(telethon, "__version__", "unknown"))

# =============================================================================
# DATA / FILES
# =============================================================================
DEFAULT_DATA_DIR = "/data" if os.path.isdir("/data") else "."
DATA_DIR = os.getenv("DATA_DIR", DEFAULT_DATA_DIR)

CACHE_DIR = os.getenv("CACHE_DIR", os.path.join(DATA_DIR, "group_cache"))
LEADS_FILE = os.getenv("LEADS_FILE", os.path.join(DATA_DIR, "leads.json"))
FAV_FILE = os.getenv("FAV_FILE", os.path.join(DATA_DIR, "favorites.json"))
SESSIONS_FILE = os.getenv("SESSIONS_FILE", os.path.join(DATA_DIR, "sessions.json"))
SEEN_FILE = os.getenv("SEEN_FILE", os.path.join(DATA_DIR, "seen.json"))
ANALYTICS_FILE = os.getenv("ANALYTICS_FILE", os.path.join(DATA_DIR, "analytics.json"))

os.makedirs(CACHE_DIR, exist_ok=True)

# =============================================================================
# LAWYER CARD (your links)
# =============================================================================
LAWYER_CARD = {
    "name_ua": "Андрій Білицький",
    "name_de": "Andrii Bilytskyi",
    "site": "https://www.andriibilytskyi.com",
    "profile": "https://www.anwalt.de/andrii-bilytskyi",
    "tg_group": "https://t.me/advocate_ua_1",
}

TARGET_INVITE_GROUP = os.getenv("TARGET_INVITE_GROUP", "@advocate_ua_1").strip()  # where to invite
ADMIN_NOTIFY_USERNAME = os.getenv("ADMIN_NOTIFY_USERNAME", "Andrii_Bilytskyi").strip().lstrip("@")

# Who can send control commands (/dm, /pitch, ...) to the running clients
raw_admins = os.getenv("CONTROL_ADMINS", ADMIN_NOTIFY_USERNAME).strip()
CONTROL_ADMINS = {x.strip().lstrip("@").lower() for x in raw_admins.split(",") if x.strip()}

# Optional: send a short auto-ack to users who DM back (once per day per user)
AUTO_ACK_PRIVATE = os.getenv("AUTO_ACK_PRIVATE", "1").strip() == "1"

# =============================================================================
# TELEGRAM ACCOUNTS
# =============================================================================
# Recommended: store api_id/api_hash in env vars; below you can still hardcode if needed.
ACCOUNTS = [
    {
        "api_id": int(os.getenv("TG_API_ID_1", "26735008")),
        "api_hash": os.getenv("TG_API_HASH_1", "").strip(),  # <-- set in env
        "session_name": os.getenv("TG_SESSION_1", "session1"),
        "label": "session1",
    },
    {
        "api_id": int(os.getenv("TG_API_ID_2", "20903513")),
        "api_hash": os.getenv("TG_API_HASH_2", "").strip(),  # <-- set in env
        "session_name": os.getenv("TG_SESSION_2", "session2"),
        "label": "session2",
    },
]

# =============================================================================
# GROUPS TO MONITOR (your list; can be overridden by env file if you want)
# =============================================================================
GROUPS_TO_MONITOR = sorted(
    set(
        [
            "@NRWanzeigen",
            "@ukraineingermany1",
            "@ukrainians_in_germany1",
            "@berlin_ukrainians",
            "@deutscheukraine",
            "@ukraincifrankfurt",
            "@jobinde",
            "@hamburg_ukrainians",
            "@UkraineinMunich",
            "@workeuropeplus",
            "@UA_in_Germany",
            "@dusseldorfukrain",
            "@TruckingNordrheinWestfalen",
            "@Berlin_UA2025",
            "@bonn_help",
            "@GermanyTop1",
            "@germany_chatik",
            "@nrw_anzeige",
            "@bochum_ua",
            "@POZITYV_PUTESHESTVIYA",
            "@uahelpkoelnanzeigen",
            "@cologne_help",
            "@TheGermany1",
            "@germania_migranty",
            "@GLOBUSEXPRESS",
            "@nashipomogut",
            "@ukr_de_essen",
            "@save_ukraine_de_essen",
            "@solingen_UA",
            "@keln_baraholka",
            "@ukraine_dortmund",
            "@UADuesseldorf",
            "@beauty_dusseldorf",
            "@pomoshukraineaachen",
            "@AhlenNRW",
            "@alsdorfua",
            "@aschafenburg",
            "@NA6R_hilft",
            "@bad4ua",
            "@badenbaden_lkr",
            "@kreiskleve",
            "@Bernkastel_Wittlich",
            "@bielefeldhelps",
            "@ukraine_bochum_support",
            "@uahelp_ruhrgebiet",
            "@DeutschlandBottrop",
            "@BS_UA_HELP",
            "@refugeesbremen",
            "@Bruchsal_Chat",
            "@Ukrainians_in_Calw",
            "@hilfe_ukraine_chemnitz",
            "@cottbus_ua",
            "@hamburg_ukraine_chat",
            "@Magdeburg_ukrainian",
            "@Fainy_Kiel",
            "@ukraine_in_Hanover",
            "@uahelfen_arbeit",
            "@bremen_hannover_dresden",
            "@ukraine_in_dresden",
            "@BavariaLife",
            "@ErfurtUA",
            "@MunchenBavaria",
            "@ua_ka_help",
            "@Ukrainians_in_Berlin_ua",
            "@refugeesinAustria",
            "@KaiserslauternUA",
            "@Karlsruhe_Ukraine",
            "@MunchenGessenBremen",
            "@chatFreiburg",
            "@Pfaffenhofen",
            "@deutschland_diaspora",
            "@Manner_ClubNRW",
            "@Ukrainer_in_Deutschland",
            "@Ukrainer_in_Wuppertal",
            "@ukrainians_in_hamburg_ua",
            "@ukrainians_berlin",
            "@berlinhelpsukrainians",
            "@Bayreuth_Bamberg",
            # Austria
            "@austriaobiavlenia",
            "@ukraineat",
            "@ukraineaustriaat",
            "@Ukrainians_in_Wien",
            "@Vienna_Linz",
            "@TheAustria1",
            "@Salzburg_Vena",
            "@qXGhIDwK00A4MWM0",
            "@austria_ua",
            "@Ukrainians_Wels_Linz",
            "@cafe_kyiv_linz",
            "@usteiermark",
            # replacement for Koblenz
            "@koblenz_ta_navkolo",
        ]
    )
)

# =============================================================================
# KEYWORDS (initial trigger list; classification does the final routing)
# =============================================================================
KEYWORDS = sorted(
    set(
        [
            # RU/UA
            "адвокат",
            "адвоката",
            "адвокатом",
            "адвокату",
            "юрист",
            "юриста",
            "юристу",
            "юристом",
            "помощь адвоката",
            "полиция",
            "прокуратура",
            "поліція",
            "прокурор",
            "страховка",
            "страховка",
            "финансы",
            "jobcenter",
            "sozialamt",
            "widerspruch",
            "sozialgericht",
            "beratungshilfe",
            "prozesskostenhilfe",
            "§24",
            # DE/EN
            "anwalt",
            "rechtsanwalt",
            "polizei",
            "staatsanwalt",
            "gericht",
            "versicherung",
            "lawyer",
            "attorney",
            "police",
            "prosecutor",
            "court",
            "advokat",
            "advocate",
        ]
    )
)

# =============================================================================
# UTILITIES
# =============================================================================
def normalize(text: str) -> str:
    t = (text or "").lower()
    t = re.sub(r"[^\w\s§€]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def load_json(path: str, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default
    return default


def save_json(path: str, data) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


LEADS: Dict[str, Any] = load_json(LEADS_FILE, {})
FAVS: Dict[str, Any] = load_json(FAV_FILE, {"items": []})
SESSIONS: Dict[str, Any] = load_json(SESSIONS_FILE, {})  # per user_id: last_ack_date, history(optional)
SEEN: Dict[str, Any] = load_json(SEEN_FILE, {"keys": []})  # processed message keys
SEEN_SET = set(SEEN.get("keys", []))


def persist_seen():
    SEEN["keys"] = list(SEEN_SET)[-20000:]  # cap
    save_json(SEEN_FILE, SEEN)


def update_analytics(group_title: str, label: str):
    try:
        data = load_json(ANALYTICS_FILE, {})
        g = data.get(group_title, {"total": 0, "labels": {}})
        g["total"] += 1
        g["labels"][label] = g["labels"].get(label, 0) + 1
        data[group_title] = g
        save_json(ANALYTICS_FILE, data)
    except Exception as e:
        logging.error("Analytics error: %s", e)


def build_message_link(chat, message_id: int) -> str:
    if getattr(chat, "username", None):
        return f"https://t.me/{chat.username}/{message_id}"
    cid = str(getattr(chat, "id", ""))
    if cid.startswith("-100"):
        return f"https://t.me/c/{cid[4:]}/{message_id}"
    return "(link unavailable)"


async def load_or_fetch_entities(client, group_usernames: List[str]):
    entities = []
    for username in sorted(set(group_usernames)):
        try:
            filename = f"{username.strip('@')}.pkl"
            path = os.path.join(CACHE_DIR, filename)
            if os.path.exists(path):
                with open(path, "rb") as f:
                    entities.append(pickle.load(f))
                logging.info("✅ cache entity: %s", username)
            else:
                entity = await client.get_entity(username)
                with open(path, "wb") as f:
                    pickle.dump(entity, f)
                entities.append(entity)
                logging.info("📥 fetched entity: %s", username)
        except Exception as e:
            logging.warning("❌ entity %s: %s", username, e)
    return entities


# =============================================================================
# CLASSIFICATION RULES
# =============================================================================
@dataclass
class ClassResult:
    label: str
    score: int
    reasons: List[str]


def hits(patterns: List[str], text: str) -> List[str]:
    return [p for p in patterns if re.search(p, text, flags=re.IGNORECASE)]


P_CTA = [
    r"пишите( мне)? (в )?(лич|лс|директ)",
    r"\bв лс\b",
    r"\bwhatsapp\b",
    r"\bзвоните\b",
    r"\btelegram:\b",
    r"консультац(ия|ии) бесплатн",
    r"\bскидк",
    r"\bоформлен",
    r"\bцена\b",
    r"€|\beur\b",
    r"\+?\d[\d\s\-\(\)]{8,}\d",  # phone-ish
]

P_SPAM = [
    r"\bcasino\b",
    r"\bказино\b",
    r"\bпромокод\b",
    r"\bcrypto\b",
    r"продажа аккаунтов",
    r"личн(ые)? кабинеты банков",
    r"\bvip\b",
]

P_JOBS_SELL = [
    r"\bработа\b",
    r"\bваканси",
    r"\bсклад\b",
    r"\bставк",
    r"\bжиль",
    r"\bпроживан",
    r"\bаванс",
    r"\bamazon\b",
    r"\bпродаю\b",
    r"\bпробег\b",
    r"\bдизель\b",
    r"\bкоробк",
]

P_PARTNER = [
    r"\bстрахов",
    r"\bversicherung\b",
    r"\bkfz\b",
    r"\brechtsschutz\b",
    r"\bzahn",
    r"\bhaftpflicht\b",
    r"\bhausrat\b",
    r"\bunfall\b",
    r"\bмаклер\b",
    r"\bконсультант\b",
    r"\bberater\b",
    r"\bdvag\b",
    r"§34[dfic]\b",
    r"\bипотек",
    r"\bbaufinanz",
    r"\bкредит\b",
    r"\bпереведу\b.*\bписьм",
    r"\bпереводчик\b",
    r"\bсопровожд",
    r"\bанмельдунг\b|\banmeldung\b",
    r"\bтермин\b|\btermin\b",
    r"\bот 5€\b",
]

P_COMPETITOR = [
    r"\bя (адвокат|юрист)\b",
    r"\bя є адвокат",
    r"\bсвідоцтв(о)?\b",
    r"\bправнича допомог",
    r"\bюридичн(і|а) послуг",
]

P_LEAD_SEARCH = [
    r"ищу (адвокат|юрист)",
    r"нужен (адвокат|юрист)",
    r"контакт(ы)? (адвокат|юрист)",
    r"порадьте .*адвокат",
    r"посоветуйте .*адвокат",
    r"хто .*адвокат",
    r"потрібн(а|о) .* (адвокат|юрист)",
    r"anwalt gesucht",
    r"brauche anwalt",
    r"rechtsanwalt gesucht",
    r"looking for (a )?lawyer",
    r"need (an )?attorney",
]

P_FALSE_LEAD = [
    r"адвокатск(ая|ую) страхов",
    r"\brechtsschutz\b",
    r"\bversicherung\b",
    r"\bkfz\b",
    r"\bzahn",
]

P_Q = [
    r"\?",
    r"\bкак\b",
    r"\bчто\b",
    r"\bпочему\b",
    r"\bсколько\b",
    r"\bможно ли\b",
    r"\bподскажите\b",
    r"\bпідкажіть\b",
    r"\bде\b",
    r"\bколи\b",
]

P_TOPIC = [
    # migration
    r"§\s*24",
    r"\baufenthalt",
    r"\bausländerbeh",
    r"\bfiktion",
    r"\babschieb",
    r"\bdeport",
    r"\bablehnung\b",
    # social
    r"\bjobcenter\b",
    r"\bsozialamt\b",
    r"\bwiderspruch\b",
    r"\bsozialgericht\b",
    r"\bberatungshilfe\b",
    r"\bprozesskostenhilfe\b",
    r"\bbürgergeld\b",
    # criminal
    r"\bуголов",
    r"\bstraf",
    r"\bpolizei\b",
    r"\bstaatsanwalt\b",
    # family
    r"\bразвод\b",
    r"\bопек",
    r"\bалим",
    r"\bumgang\b",
    r"\bsorgerecht\b",
    # labor
    r"\barbeitsrecht\b",
    r"\bkündig",
    r"\bувольнен",
    r"\bпереработ",
    # medical bills / inkasso
    r"\barzt\b",
    r"\bzahnarzt\b",
    r"\brechnung\b",
    r"\binkasso\b",
    r"\bmahnbescheid\b",
    r"\bсчет\b",
    r"\bдоплат",
    r"\bстраховк.*не покрыв",
    # utilities
    r"\bstrom\b",
    r"\bgas\b",
    r"\bэлектрич",
]

def classify(text_norm: str) -> ClassResult:
    # Hard blocks first
    h = hits(P_SPAM, text_norm)
    if h:
        return ClassResult("SPAM_SCAM", 100, [f"spam:{x}" for x in h])

    h = hits(P_JOBS_SELL, text_norm)
    if h:
        return ClassResult("OFFTOP_JOBS_SELL", 90, [f"offtop:{x}" for x in h])

    scores: Dict[str, Tuple[int, List[str]]] = {}

    def add(label: str, pts: int, rs: List[str]):
        cur = scores.get(label, (0, []))
        scores[label] = (cur[0] + pts, cur[1] + rs)

    # Partner
    hp = hits(P_PARTNER, text_norm)
    hc = hits(P_CTA, text_norm)
    if hp:
        add("PARTNER_SERVICES", 40 + 10 * min(len(hp), 3), [f"partner:{x}" for x in hp])
    if hc:
        add("PARTNER_SERVICES", 15 + 5 * min(len(hc), 3), [f"cta:{x}" for x in hc])

    # Competitor
    hcomp = hits(P_COMPETITOR, text_norm)
    if hcomp:
        add("COMPETITOR_LAWYER", 55, [f"comp:{x}" for x in hcomp])

    # Lead search (direct)
    hls = hits(P_LEAD_SEARCH, text_norm)
    hfalse = hits(P_FALSE_LEAD, text_norm)
    if hls and not hfalse:
        add("LEAD_SEARCH", 70, [f"lead_search:{x}" for x in hls])

    # Lead question (topic + question signs)
    hq = hits(P_Q, text_norm)
    ht = hits(P_TOPIC, text_norm)
    if ht and hq:
        add("LEAD_QUESTION", 50 + 5 * min(len(ht), 5), [f"topic:{x}" for x in ht] + [f"q:{x}" for x in hq])

    if not scores:
        return ClassResult("OTHER", 0, [])

    # Choose best by score; if tie, enforce priority
    priority = ["PARTNER_SERVICES", "COMPETITOR_LAWYER", "LEAD_SEARCH", "LEAD_QUESTION", "OTHER"]
    best_label, (best_score, best_reasons) = sorted(
        scores.items(), key=lambda kv: (kv[1][0], -priority.index(kv[0])), reverse=True
    )[0]
    return ClassResult(best_label, best_score, best_reasons)


# =============================================================================
# TOPIC DETECTION + LANGUAGE HEURISTIC (templates)
# =============================================================================
TOPIC_PATTERNS = {
    "migration_24": [r"§\s*24", r"\baufenthalt", r"\bausländerbeh", r"\bfiktion", r"\bablehnung", r"\babschieb", r"\bdeport"],
    "social_jobcenter": [r"\bjobcenter\b", r"\bsozialamt\b", r"\bwiderspruch\b", r"\bsozialgericht\b", r"\bberatungshilfe\b", r"\bprozesskostenhilfe\b", r"\bbürgergeld\b"],
    "criminal": [r"\bуголов", r"\bstraf", r"\bpolizei\b", r"\bstaatsanwalt\b", r"\banzeige\b"],
    "family": [r"\bразвод\b", r"\bопек", r"\bалим", r"\bumgang\b", r"\bsorgerecht\b"],
    "labor": [r"\barbeitsrecht\b", r"\bkündig", r"\bувольнен", r"\bпереработ"],
    "medical_bills": [r"\barzt\b", r"\bzahnarzt\b", r"\brechnung\b", r"\binkasso\b", r"\bmahnbescheid\b", r"\bсчет\b", r"\bдоплат"],
    "utilities": [r"\bstrom\b", r"\bgas\b", r"\bэлектрич", r"\bсвет\b"],
}

def detect_topic(text_norm: str) -> str:
    for topic, pats in TOPIC_PATTERNS.items():
        if any(re.search(p, text_norm, flags=re.IGNORECASE) for p in pats):
            return topic
    return "general"

def detect_lang(text: str) -> str:
    t = text or ""
    # Ukrainian markers
    if re.search(r"[іїєґІЇЄҐ]", t):
        return "ua"
    # Cyrillic -> RU/UA (ua handled above)
    if re.search(r"[А-Яа-я]", t):
        return "ru"
    # German-ish
    if re.search(r"\b(und|ich|bitte|anwalt|rechtsanwalt|aufenthalt|sozialamt|jobcenter)\b", t, flags=re.IGNORECASE):
        return "de"
    return "en"

def lawyer_links_block(lang: str) -> str:
    if lang == "de":
        return (
            f"\n\nKontakt:\n"
            f"• Website: {LAWYER_CARD['site']}\n"
            f"• Profil: {LAWYER_CARD['profile']}\n"
            f"• Telegram-Gruppe: {LAWYER_CARD['tg_group']}"
        )
    if lang == "ua":
        return (
            f"\n\nКонтакти адвоката:\n"
            f"• Сайт: {LAWYER_CARD['site']}\n"
            f"• Профіль: {LAWYER_CARD['profile']}\n"
            f"• Група: {LAWYER_CARD['tg_group']}"
        )
    if lang == "en":
        return (
            f"\n\nContacts:\n"
            f"• Website: {LAWYER_CARD['site']}\n"
            f"• Profile: {LAWYER_CARD['profile']}\n"
            f"• Telegram group: {LAWYER_CARD['tg_group']}"
        )
    # ru
    return (
        f"\n\nКонтакты адвоката:\n"
        f"• Сайт: {LAWYER_CARD['site']}\n"
        f"• Профиль: {LAWYER_CARD['profile']}\n"
        f"• Группа: {LAWYER_CARD['tg_group']}"
    )

def justin_template(topic: str, lang: str) -> str:
    if lang == "de":
        base = "Hallo! Ich bin Justin – Assistent von Rechtsanwalt Andrii Bilytskyi. Ich gebe eine erste Orientierung (allgemeine Infos, keine Rechtsberatung)."
        if topic == "migration_24":
            body = (
                "\n\nBei §24/Ausländerbehörde ist oft die Frist entscheidend. Bitte kurz (ohne sensible Daten):"
                "\n1) Bundesland/Stadt (Ausländerbehörde)?"
                "\n2) Was steht im Schreiben (Ablehnung/Anhörung/Frist/Rechtsbehelfsbelehrung)?"
                "\n3) Gibt es eine konkrete Frist?"
                "\n\nWenn eine Frist läuft oder Abschiebung erwähnt wird, лучше sofort Anwalt einschalten."
            )
        elif topic == "social_jobcenter":
            body = (
                "\n\nBei Jobcenter/Sozialamt prüfen wir meist Zuständigkeit + Fristen (Widerspruch/Klage). Bitte kurz:"
                "\n1) Bundesland/Stadt?"
                "\n2) Worum geht es (Bürgergeld/Sozialhilfe) und was wurde abgelehnt?"
                "\n3) Gibt es schon einen Widerspruchsbescheid / Frist fürs Sozialgericht?"
            )
        elif topic == "medical_bills":
            body = (
                "\n\nBei Arztrechnungen ist wichtig: Rechnung vs. Mahnung vs. Inkasso vs. Mahnbescheid (Gericht). Bitte kurz:"
                "\n1) Welches Dokument genau?"
                "\n2) Wofür (Arzt/Zahnarzt/Klinik) und Versicherung?"
                "\n3) Gibt es eine Frist?"
            )
        elif topic == "criminal":
            body = (
                "\n\nBei Strafsachen лучше Details nicht öffentlich diskutieren. Bitte kurz (ohne sensible Daten):"
                "\n1) Stadium: Vorladung/Vernehmung/Beschuldigung/Haussuchung?"
                "\n2) Stadt/Bundesland?"
                "\n3) Gibt es eine Frist?"
            )
        elif topic == "family":
            body = (
                "\n\nFamilienrecht hängt stark vom Verfahrensstand ab. Bitte kurz:"
                "\n1) Bundesland/Stadt, läuft schon ein Verfahren?"
                "\n2) Thema: Scheidung/Umgang/Sorgerecht/Unterhalt?"
                "\n3) Gibt es Fristen oder Termine?"
            )
        elif topic == "labor":
            body = (
                "\n\nArbeitsrecht: Fristen (Kündigungsschutzklage) sind oft sehr kurz. Bitte kurz:"
                "\n1) Bundesland/Stadt?"
                "\n2) Thema: Kündigung/Überstunden/Arbeitsvertrag?"
                "\n3) Datum des Schreibens / Frist erwähnt?"
            )
        else:
            body = (
                "\n\nKurz zur Einordnung (ohne sensible Daten):"
                "\n1) Bundesland/Stadt?"
                "\n2) Rechtsgebiet (Aufenthalt/Soziales/Familie/Strafrecht/Arbeit/Medizin-Rechnungen)?"
                "\n3) Gibt es Fristen?"
            )
        return base + body + lawyer_links_block(lang)

    # UA/RU/EN
    is_ua = (lang == "ua")
    if is_ua:
        base = "Вітаю! Я Юстин — помічник адвоката Андрія Білицького. Даю первинну орієнтацію (загальна інформація, не юридична консультація)."
        if topic == "migration_24":
            body = (
                "\n\nПо §24/міграції критично не пропустити строки. Уточніть, будь ласка (без персональних даних):"
                "\n1) Земля/місто (яка Ausländerbehörde)?"
                "\n2) Що саме написано в листі (відмова/запит/строк/оскарження)?"
                "\n3) Чи є конкретний строк відповіді/оскарження?"
            )
        elif topic == "social_jobcenter":
            body = (
                "\n\nУ спорах Jobcenter/Sozialamt важливі компетенція та строки (Widerspruch/Sozialgericht). Уточніть:"
                "\n1) Земля/місто?"
                "\n2) Про яку допомогу йдеться?"
                "\n3) Чи є Widerspruchsbescheid / строк до Sozialgericht?"
            )
        elif topic == "medical_bills":
            body = (
                "\n\nПо медичних рахунках важливо відрізнити Rechnung/Mahnung/Inkasso/Mahnbescheid. Уточніть:"
                "\n1) Який саме документ прийшов?"
                "\n2) За яку послугу і чи була страховка?"
                "\n3) Чи є строк?"
            )
        elif topic == "criminal":
            body = (
                "\n\nЯкщо це кримінальна справа/поліція — краще не обговорювати деталі публічно. Уточніть (без персональних даних):"
                "\n1) Стадія: виклик/допит/обвинувачення/обшук?"
                "\n2) Земля/місто?"
                "\n3) Чи є строк?"
            )
        elif topic == "family":
            body = (
                "\n\nУ сімейних справах (розлучення/опіка/контакти) важливий процесуальний стан. Уточніть:"
                "\n1) Земля/місто, чи є вже справа в суді?"
                "\n2) Тема: розлучення/аліменти/опіка/Umgang?"
                "\n3) Чи є строки/терміновість?"
            )
        elif topic == "labor":
            body = (
                "\n\nУ трудових спорах строки можуть бути дуже короткими. Уточніть:"
                "\n1) Земля/місто?"
                "\n2) Тема: звільнення/переробітки/договір?"
                "\n3) Чи є строк у листі/документі?"
            )
        else:
            body = (
                "\n\nЩоб зорієнтуватися (без персональних даних):"
                "\n1) Земля/місто?"
                "\n2) Сфера: міграція/соцвиплати/сімейне/кримінальне/трудове/рахунки?"
                "\n3) Чи є строки?"
            )
        return base + body + lawyer_links_block(lang)

    if lang == "en":
        base = "Hi! I’m Justin — assistant to lawyer Andrii Bilytskyi. I can provide first orientation (general info, not legal advice)."
        body = "\n\nTo guide you (no sensitive personal data):\n1) Which city/federal state?\n2) What area (immigration/social/family/criminal/labor/medical bills)?\n3) Any deadlines mentioned in letters?"
        return base + body + lawyer_links_block(lang)

    # RU
    base = "Здравствуйте! Я Юстин — помощник адвоката Андрея Билицкого. Даю первичную ориентацию (общая информация, не юридическая консультация)."
    if topic == "migration_24":
        body = (
            "\n\nПо §24/миграции критично не пропустить сроки. Уточните, пожалуйста (без персональных данных):"
            "\n1) Земля/город (какая Ausländerbehörde)?"
            "\n2) Что написано в письме (отказ/запрос/срок/порядок обжалования)?"
            "\n3) Есть ли конкретный срок ответа/обжалования?"
        )
    elif topic == "social_jobcenter":
        body = (
            "\n\nВ спорах Jobcenter/Sozialamt важны компетенция и сроки (Widerspruch/Sozialgericht). Уточните:"
            "\n1) Земля/город?"
            "\n2) О какой выплате речь?"
            "\n3) Есть ли Widerspruchsbescheid / срок для Sozialgericht?"
        )
    elif topic == "medical_bills":
        body = (
            "\n\nПо мед. счетам важно отличить Rechnung/Mahnung/Inkasso/Mahnbescheid. Уточните:"
            "\n1) Что именно пришло?"
            "\n2) За какую услугу и была ли страховка?"
            "\n3) Есть ли срок?"
        )
    elif topic == "criminal":
        body = (
            "\n\nЕсли это уголовное дело/полиция — лучше не обсуждать детали публично. Уточните (без персональных данных):"
            "\n1) Стадия: вызов/допрос/обвинение/обыск?"
            "\n2) Земля/город?"
            "\n3) Есть ли сроки?"
        )
    elif topic == "family":
        body = (
            "\n\nВ семейных делах (развод/опека/контакты) важен процессуальный статус. Уточните:"
            "\n1) Земля/город, есть ли уже дело в суде?"
            "\n2) Тема: развод/алименты/опека/Umgang?"
            "\n3) Есть ли сроки/срочность?"
        )
    elif topic == "labor":
        body = (
            "\n\nВ трудовых спорах сроки могут быть очень короткими. Уточните:"
            "\n1) Земля/город?"
            "\n2) Тема: увольнение/переработки/договор?"
            "\n3) Есть ли срок в письме/документе?"
        )
    else:
        body = (
            "\n\nЧтобы сориентироваться (без персональных данных):"
            "\n1) Земля/город?"
            "\n2) Сфера: миграция/соцвыплаты/семейное/уголовное/трудовое/счета?"
            "\n3) Есть ли сроки?"
        )
    return base + body + lawyer_links_block(lang)

def partner_pitch_template(lang: str) -> str:
    if lang == "de":
        return (
            "Hallo! Ich bin Justin – Assistent von Rechtsanwalt Andrii Bilytskyi.\n"
            "Ich sehe, dass Sie Menschen mit Versicherungen/Finanzen unterstützen. "
            "Wenn bei Ihren Kunden juristische Themen auftauchen (Streit mit Versicherung, Aufenthalt/§24, Jobcenter/Sozialamt, Inkasso, Gericht), "
            "können wir gern einen schnellen Handoff / gegenseitige Empfehlungen organisieren.\n"
            f"\n• Website: {LAWYER_CARD['site']}\n• Profil: {LAWYER_CARD['profile']}\n• Telegram-Gruppe: {LAWYER_CARD['tg_group']}"
        )
    if lang == "ua":
        return (
            "Вітаю! Я Юстин — помічник адвоката Андрія Білицького.\n"
            "Бачу, що ви допомагаєте людям зі страховками/фінансами. "
            "Якщо у ваших клієнтів виникають юридичні питання (спори зі страховою, Aufenthalt/§24, Jobcenter/Sozialamt, інкасо, суд), "
            "можемо налаштувати швидкі рекомендації/передачу звернень.\n"
            f"\n• Сайт: {LAWYER_CARD['site']}\n• Профіль: {LAWYER_CARD['profile']}\n• Група: {LAWYER_CARD['tg_group']}"
        )
    if lang == "en":
        return (
            "Hi! I’m Justin — assistant to lawyer Andrii Bilytskyi.\n"
            "I see you help people with insurance/finance. If your clients sometimes need legal support "
            "(insurance disputes, residence/§24, social benefits, debt collection, court), we can set up a quick handoff/referrals.\n"
            f"\n• Website: {LAWYER_CARD['site']}\n• Profile: {LAWYER_CARD['profile']}\n• Telegram group: {LAWYER_CARD['tg_group']}"
        )
    return (
        "Здравствуйте! Я Юстин — помощник адвоката Андрея Билицкого.\n"
        "Вижу, что вы помогаете людям со страховками/финансами. "
        "Если у ваших клиентов возникают юридические вопросы (споры со страховкой, Aufenthalt/§24, Jobcenter/Sozialamt, инкассо, суд), "
        "можем настроить быстрые рекомендации/передачу обращений.\n"
        f"\n• Сайт: {LAWYER_CARD['site']}\n• Профиль: {LAWYER_CARD['profile']}\n• Группа: {LAWYER_CARD['tg_group']}"
    )


# =============================================================================
# LEADS / FAVORITES
# =============================================================================
def new_lead_id() -> str:
    return uuid.uuid4().hex[:10]

def mk_event_key(event) -> str:
    return f"{event.chat_id}:{event.id}"

def save_lead(lead: Dict[str, Any]) -> None:
    LEADS[lead["id"]] = lead
    save_json(LEADS_FILE, LEADS)

def get_lead(lead_id: str) -> Optional[Dict[str, Any]]:
    return LEADS.get(lead_id)

def add_to_favorites(lead: Dict[str, Any]) -> None:
    item = {
        "ts": lead.get("ts"),
        "sender_id": lead.get("sender_id"),
        "sender_username": lead.get("sender_username"),
        "sender_name": lead.get("sender_name"),
        "msg_link": lead.get("msg_link"),
        "label": lead.get("label"),
        "text": (lead.get("text") or "")[:800],
    }
    FAVS["items"].append(item)
    # dedup last 5k
    FAVS["items"] = FAVS["items"][-5000:]
    save_json(FAV_FILE, FAVS)

def build_admin_card(lead: Dict[str, Any]) -> str:
    draft = lead.get("draft") or ""
    reasons = ", ".join(lead.get("reasons") or [])
    kws = ", ".join(lead.get("keywords") or [])
    return (
        f"🧾 Lead #{lead['id']} | {lead.get('label')} (score={lead.get('score')})\n"
        f"📢 {lead.get('chat_title')}\n"
        f"🔗 {lead.get('msg_link')}\n"
        f"👤 @{lead.get('sender_username') or ''} ({lead.get('sender_name')}) id={lead.get('sender_id')}\n"
        f"🏷 keywords: {kws}\n"
        f"🧠 reasons: {reasons}\n"
        f"💬 text:\n{lead.get('text')}\n\n"
        f"✍️ draft:\n{draft}\n\n"
        f"Commands:\n"
        f"/dm {lead['id']}\n"
        f"/pitch {lead['id']}\n"
        f"/invite {lead['id']}\n"
        f"/fav {lead['id']}\n"
        f"/ignore {lead['id']}\n"
        f"/show {lead['id']}\n"
        f"/help"
    )


# =============================================================================
# COMMAND PARSING
# =============================================================================
CMD_RE = re.compile(r"^/(dm|pitch|invite|fav|ignore|show|help)\b(?:\s+([0-9a-f]{6,20}))?", re.IGNORECASE)

def is_control_admin(sender) -> bool:
    u = (getattr(sender, "username", "") or "").lower()
    return u in CONTROL_ADMINS

async def safe_send(client: TelegramClient, peer, text: str):
    # Telegram msg length safety
    chunk = 3800
    for i in range(0, len(text), chunk):
        await client.send_message(peer, text[i : i + chunk])

async def send_admin(client: TelegramClient, text: str):
    await safe_send(client, ADMIN_NOTIFY_USERNAME, text)


# =============================================================================
# LIFECYCLE / SIGNALS
# =============================================================================
shutdown = asyncio.Event()

def _handle_signal():
    shutdown.set()

# =============================================================================
# PER-CLIENT RUNNER
# =============================================================================
async def run_client_forever(cfg: Dict[str, Any]):
    backoff = 5
    label = cfg.get("label") or cfg.get("session_name")

    if not cfg.get("api_hash"):
        logging.error("[%s] Missing api_hash. Set TG_API_HASH_1 / TG_API_HASH_2 env vars.", label)
        return

    while not shutdown.is_set():
        client: Optional[TelegramClient] = None
        try:
            client = TelegramClient(cfg["session_name"], cfg["api_id"], cfg["api_hash"])
            await client.connect()

            if not await client.is_user_authorized():
                logging.warning("[%s] Not authorized. Please authorize session: %s", label, cfg["session_name"])
                await asyncio.sleep(backoff)
                continue

            me = await client.get_me()
            my_username = (getattr(me, "username", "") or "").lower()
            logging.info("[%s] Connected as @%s", label, my_username)

            entities = await load_or_fetch_entities(client, GROUPS_TO_MONITOR)
            logging.info("[%s] Monitoring %d chats", label, len(entities))

            # -------------------------
            # 1) GROUP MONITOR HANDLER
            # -------------------------
            @client.on(events.NewMessage(chats=entities))
            async def group_handler(event):
                try:
                    if not event.raw_text:
                        return

                    key = mk_event_key(event)
                    if key in SEEN_SET:
                        return

                    text = event.raw_text
                    text_norm = normalize(text)

                    matched = [kw for kw in KEYWORDS if kw in text_norm]
                    # If no keywords at all, skip early (you can relax this if you want)
                    if not matched:
                        return

                    cls = classify(text_norm)

                    # Skip noisy classes (but keep analytics if you want)
                    if cls.label in ("SPAM_SCAM", "OFFTOP_JOBS_SELL"):
                        SEEN_SET.add(key)
                        persist_seen()
                        update_analytics(getattr(event.chat, "title", "") or "?", cls.label)
                        return

                    sender = await event.get_sender()
                    sender_username = getattr(sender, "username", None)
                    sender_name = f"{getattr(sender,'first_name','') or ''} {getattr(sender,'last_name','') or ''}".strip()

                    link = build_message_link(event.chat, event.id)
                    lead_id = new_lead_id()

                    # Draft
                    lang = detect_lang(text)
                    topic = detect_topic(text_norm)
                    draft = ""
                    if cls.label in ("LEAD_SEARCH", "LEAD_QUESTION"):
                        draft = justin_template(topic, lang)
                    elif cls.label == "PARTNER_SERVICES":
                        draft = partner_pitch_template(lang)
                    elif cls.label == "COMPETITOR_LAWYER":
                        draft = ""  # usually no action
                    else:
                        draft = ""

                    lead = {
                        "id": lead_id,
                        "ts": datetime.now().isoformat(timespec="seconds"),
                        "session": label,
                        "label": cls.label,
                        "score": cls.score,
                        "reasons": cls.reasons,
                        "chat_title": getattr(event.chat, "title", "") or "",
                        "chat_username": getattr(event.chat, "username", None),
                        "chat_id": event.chat_id,
                        "msg_id": event.id,
                        "msg_link": link,
                        "sender_id": getattr(sender, "id", None),
                        "sender_username": sender_username,
                        "sender_name": sender_name,
                        "keywords": matched,
                        "text": text,
                        "lang": lang,
                        "topic": topic,
                        "draft": draft,
                        "status": "new",
                    }

                    # Dedup by (chat_id,msg_id) globally:
                    SEEN_SET.add(key)
                    persist_seen()

                    save_lead(lead)
                    update_analytics(lead["chat_title"] or "?", cls.label)

                    # Notify admin only for relevant classes
                    if cls.label in ("LEAD_SEARCH", "LEAD_QUESTION", "PARTNER_SERVICES"):
                        await send_admin(client, build_admin_card(lead))
                    else:
                        # optional: uncomment if you want to see competitor cards too
                        # await send_admin(client, build_admin_card(lead))
                        pass

                except Exception as e:
                    logging.error("[%s] group handler error: %s", label, e)

            # -------------------------
            # 2) COMMAND HANDLER (incoming from control admins)
            # -------------------------
            @client.on(events.NewMessage(incoming=True))
            async def incoming_cmd_handler(event):
                try:
                    if not event.raw_text:
                        return
                    sender = await event.get_sender()
                    if not is_control_admin(sender):
                        return

                    m = CMD_RE.match(event.raw_text.strip())
                    if not m:
                        return

                    cmd = (m.group(1) or "").lower()
                    lead_id = (m.group(2) or "").lower().strip()

                    if cmd == "help":
                        await event.reply(
                            "Commands:\n"
                            "/dm <lead_id>     - send Justin reply to user\n"
                            "/pitch <lead_id>  - send partner pitch to user\n"
                            "/invite <lead_id> - invite user to TARGET_INVITE_GROUP\n"
                            "/fav <lead_id>    - add to favorites.json\n"
                            "/ignore <lead_id> - mark lead ignored\n"
                            "/show <lead_id>   - show stored lead\n"
                        )
                        return

                    if cmd == "show":
                        lead = get_lead(lead_id)
                        if not lead:
                            await event.reply("lead not found")
                            return
                        await event.reply(build_admin_card(lead))
                        return

                    if not lead_id:
                        await event.reply("Missing lead_id. Example: /dm a1b2c3d4e5")
                        return

                    lead = get_lead(lead_id)
                    if not lead:
                        await event.reply("lead not found")
                        return

                    # Execute
                    if cmd == "ignore":
                        lead["status"] = "ignored"
                        save_lead(lead)
                        await event.reply("ok: ignored")
                        return

                    if cmd == "fav":
                        add_to_favorites(lead)
                        lead["status"] = "fav"
                        save_lead(lead)
                        await event.reply("ok: added to favorites")
                        return

                    if cmd in ("dm", "pitch"):
                        if not lead.get("sender_id"):
                            await event.reply("lead has no sender_id")
                            return

                        target = await client.get_entity(int(lead["sender_id"]))
                        if cmd == "dm":
                            text_to_send = lead.get("draft") or justin_template(lead.get("topic") or "general", lead.get("lang") or "ru")
                            await safe_send(client, target, text_to_send)
                            lead["status"] = "dm_sent"
                            save_lead(lead)
                            await event.reply("ok: dm sent")
                            return

                        if cmd == "pitch":
                            text_to_send = lead.get("draft") or partner_pitch_template(lead.get("lang") or "ru")
                            await safe_send(client, target, text_to_send)
                            lead["status"] = "pitch_sent"
                            save_lead(lead)
                            await event.reply("ok: pitch sent")
                            return

                    if cmd == "invite":
                        if not lead.get("sender_id"):
                            await event.reply("lead has no sender_id")
                            return
                        user_ent = await client.get_entity(int(lead["sender_id"]))
                        group_ent = await client.get_entity(TARGET_INVITE_GROUP)
                        await client(functions.channels.InviteToChannelRequest(channel=group_ent, users=[user_ent]))
                        lead["status"] = "invited"
                        save_lead(lead)
                        await event.reply(f"ok: invited to {TARGET_INVITE_GROUP}")
                        return

                except UserAlreadyParticipantError:
                    await event.reply("ℹ️ user already participant")
                except UserPrivacyRestrictedError:
                    await event.reply("❗UserPrivacyRestricted: cannot DM/invite this user")
                except ChatAdminRequiredError:
                    await event.reply("❗ChatAdminRequired: account has no rights to invite in target group")
                except PeerFloodError:
                    await event.reply("❗PeerFlood: Telegram limited actions. Stop/slow down.")
                except FloodWaitError as e:
                    await event.reply(f"⏳ FloodWait: wait {getattr(e, 'seconds', '?')}s")
                except RPCError as e:
                    await event.reply(f"RPCError: {type(e).__name__}: {e}")
                except Exception as e:
                    await event.reply(f"Error: {type(e).__name__}: {e}")

            # -------------------------
            # 3) COMMAND HANDLER (outgoing to self "Saved Messages")
            #    Useful if you run/operate from the same account.
            # -------------------------
            @client.on(events.NewMessage(outgoing=True))
            async def outgoing_cmd_handler(event):
                try:
                    if not event.raw_text:
                        return
                    # only react to commands you type to yourself (Saved Messages) or to the bot chat with ADMIN_NOTIFY
                    # (safe filter: command must match)
                    m = CMD_RE.match(event.raw_text.strip())
                    if not m:
                        return

                    # Optional: restrict to "Saved Messages" only
                    # if not event.is_private: return

                    cmd = (m.group(1) or "").lower()
                    lead_id = (m.group(2) or "").lower().strip()

                    if cmd == "help":
                        await event.reply(
                            "Commands:\n"
                            "/dm <lead_id>\n/pitch <lead_id>\n/invite <lead_id>\n/fav <lead_id>\n/ignore <lead_id>\n/show <lead_id>\n/help"
                        )
                        return

                    if cmd == "show":
                        lead = get_lead(lead_id)
                        if not lead:
                            await event.reply("lead not found")
                            return
                        await event.reply(build_admin_card(lead))
                        return

                    if not lead_id:
                        await event.reply("Missing lead_id. Example: /dm a1b2c3d4e5")
                        return

                    lead = get_lead(lead_id)
                    if not lead:
                        await event.reply("lead not found")
                        return

                    # Reuse the incoming handler logic by simulating minimal dispatch:
                    if cmd == "ignore":
                        lead["status"] = "ignored"
                        save_lead(lead)
                        await event.reply("ok: ignored")
                        return

                    if cmd == "fav":
                        add_to_favorites(lead)
                        lead["status"] = "fav"
                        save_lead(lead)
                        await event.reply("ok: added to favorites")
                        return

                    if cmd in ("dm", "pitch"):
                        if not lead.get("sender_id"):
                            await event.reply("lead has no sender_id")
                            return
                        target = await client.get_entity(int(lead["sender_id"]))
                        if cmd == "dm":
                            text_to_send = lead.get("draft") or justin_template(lead.get("topic") or "general", lead.get("lang") or "ru")
                            await safe_send(client, target, text_to_send)
                            lead["status"] = "dm_sent"
                            save_lead(lead)
                            await event.reply("ok: dm sent")
                            return
                        if cmd == "pitch":
                            text_to_send = lead.get("draft") or partner_pitch_template(lead.get("lang") or "ru")
                            await safe_send(client, target, text_to_send)
                            lead["status"] = "pitch_sent"
                            save_lead(lead)
                            await event.reply("ok: pitch sent")
                            return

                    if cmd == "invite":
                        if not lead.get("sender_id"):
                            await event.reply("lead has no sender_id")
                            return
                        user_ent = await client.get_entity(int(lead["sender_id"]))
                        group_ent = await client.get_entity(TARGET_INVITE_GROUP)
                        await client(functions.channels.InviteToChannelRequest(channel=group_ent, users=[user_ent]))
                        lead["status"] = "invited"
                        save_lead(lead)
                        await event.reply(f"ok: invited to {TARGET_INVITE_GROUP}")
                        return

                except UserAlreadyParticipantError:
                    await event.reply("ℹ️ user already participant")
                except UserPrivacyRestrictedError:
                    await event.reply("❗UserPrivacyRestricted: cannot DM/invite this user")
                except ChatAdminRequiredError:
                    await event.reply("❗ChatAdminRequired: account has no rights to invite in target group")
                except PeerFloodError:
                    await event.reply("❗PeerFlood: Telegram limited actions. Stop/slow down.")
                except FloodWaitError as e:
                    await event.reply(f"⏳ FloodWait: wait {getattr(e, 'seconds', '?')}s")
                except Exception as e:
                    await event.reply(f"Error: {type(e).__name__}: {e}")

            # -------------------------
            # 4) PRIVATE INBOUND (user replies)
            # -------------------------
            @client.on(events.NewMessage(incoming=True))
            async def private_inbound_handler(event):
                try:
                    if not event.is_private:
                        return
                    if not event.raw_text:
                        return

                    sender = await event.get_sender()
                    if is_control_admin(sender):
                        return  # admin commands handled elsewhere

                    uid = str(getattr(sender, "id", ""))
                    if not uid:
                        return

                    # Forward message to admin
                    uname = getattr(sender, "username", None)
                    sname = f"{getattr(sender,'first_name','') or ''} {getattr(sender,'last_name','') or ''}".strip()
                    msg = (
                        f"📩 Incoming private message ({label})\n"
                        f"👤 @{uname or ''} ({sname}) id={uid}\n"
                        f"💬 {event.raw_text}"
                    )
                    await send_admin(client, msg)

                    if not AUTO_ACK_PRIVATE:
                        return

                    # One-time per day ack
                    today = datetime.now().strftime("%Y-%m-%d")
                    st = SESSIONS.get(uid, {})
                    if st.get("last_ack") == today:
                        return

                    lang = detect_lang(event.raw_text)
                    ack = ""
                    if lang == "de":
                        ack = (
                            "Danke! Ich habe Ihre Nachricht weitergeleitet.\n"
                            "Bitte senden Sie keine sensiblen Daten (Pass/Adresse/Nummern).\n"
                            "Für eine ausführliche Beratung kontaktieren Sie den Anwalt direkt:"
                            + lawyer_links_block(lang)
                        )
                    elif lang == "ua":
                        ack = (
                            "Дякую! Я передав повідомлення.\n"
                            "Будь ласка, не надсилайте чутливі дані (паспорт/адреса/номери документів).\n"
                            "Для детальної консультації звʼяжіться з адвокатом напряму:"
                            + lawyer_links_block(lang)
                        )
                    elif lang == "en":
                        ack = (
                            "Thanks! I forwarded your message.\n"
                            "Please do not send sensitive data (passport/address/document numbers).\n"
                            "For detailed legal advice, contact the lawyer directly:"
                            + lawyer_links_block(lang)
                        )
                    else:
                        ack = (
                            "Спасибо! Я передал сообщение.\n"
                            "Пожалуйста, не отправляйте чувствительные данные (паспорт/адрес/номера документов).\n"
                            "Для детальной консультации свяжитесь с адвокатом напрямую:"
                            + lawyer_links_block(lang)
                        )

                    await safe_send(client, sender, ack)
                    st["last_ack"] = today
                    SESSIONS[uid] = st
                    save_json(SESSIONS_FILE, SESSIONS)

                except Exception as e:
                    logging.error("[%s] private inbound error: %s", label, e)

            backoff = 5
            await client.run_until_disconnected()

        except Exception as e:
            logging.critical("[%s] critical error: %s", label, e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        finally:
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass


# =============================================================================
# MAIN
# =============================================================================
async def main():
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(loop_exc_handler)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass

    tasks = [asyncio.create_task(run_client_forever(cfg)) for cfg in ACCOUNTS]
    await shutdown.wait()

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
