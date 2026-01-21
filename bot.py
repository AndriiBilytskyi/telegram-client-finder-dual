import os
import sys
import re
import json
import pickle
import signal
import asyncio
import logging
import traceback
from logging.handlers import RotatingFileHandler
from datetime import datetime

import telethon
from telethon import TelegramClient, events

# ====== НАСТРОЙКА ЛОГОВ =======================================================
LOG_PATH = os.getenv("LOG_PATH", "/data/bot.log")  # если нет диска /data, файл создастся рядом
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

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

# ====== КОНФИГ ================================================================
print("Telethon version:", getattr(telethon, "__version__", "unknown"))

ACCOUNTS = [
    {
        "api_id": 26735008,
        "api_hash": "6c35a6247e6b6502e5b79173b22af871",
        "session_name": "session1",
        "your_username": "Andrii_Bilytskyi",
    },
    {
        "api_id": 20903513,
        "api_hash": "0eb01bf47aeac4cbfd89fff140a4e06d",
        "session_name": "session2",
        "your_username": "Anwalt_Bilytskyi",
    },
]

KEYWORDS = sorted(set([
    # RU/UA
    'адвокат', 'адвоката', 'адвокатом', 'адвокату',
    'юрист', 'юриста', 'юристу', 'юристом',
    'помощь адвоката', 'полиция', 'прокуратура',
    'поліція', 'прокурор', 'страховка', 'финансы',
    # DE
    'anwalt', 'rechtsanwalt', 'polizei', 'staatsanwalt', 'gericht', 'versicherung',
    # EN + варианты
    'lawyer', 'attorney', 'police', 'prosecutor', 'court',
    'advokat', 'advocate'
]))

GROUPS_TO_MONITOR = sorted(set([
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
    # Австрия
    '@austriaobiavlenia', '@ukraineat', '@ukraineaustriaat',
    '@Ukrainians_in_Wien', '@Vienna_Linz', '@TheAustria1',
    '@Salzburg_Vena', '@qXGhIDwK00A4MWM0', '@austria_ua',
    '@Ukrainians_Wels_Linz', '@cafe_kyiv_linz', '@usteiermark',
    # Ты добавил замену для Koblenz:
    '@koblenz_ta_navkolo',
]))

# Директории/файлы
DEFAULT_DATA_DIR = "/data" if os.path.isdir("/data") else "."
CACHE_DIR = os.getenv("CACHE_DIR", os.path.join(DEFAULT_DATA_DIR, "group_cache"))
ANALYTICS_FILE = os.getenv("ANALYTICS_FILE", os.path.join(DEFAULT_DATA_DIR, "analytics.json"))

os.makedirs(CACHE_DIR, exist_ok=True)

# ====== УТИЛИТЫ ==============================================================
def normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def update_analytics(group_title: str, matched_keywords):
    try:
        data = {}
        if os.path.exists(ANALYTICS_FILE):
            with open(ANALYTICS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)

        group_data = data.get(group_title, {"total": 0, "keywords": {}})
        group_data["total"] += 1
        for kw in matched_keywords:
            group_data["keywords"][kw] = group_data["keywords"].get(kw, 0) + 1

        data[group_title] = group_data
        with open(ANALYTICS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"Ошибка аналитики: {e}")

async def load_or_fetch_entities(client, group_usernames):
    entities = []
    for username in sorted(set(group_usernames)):
        try:
            filename = f"{username.strip('@')}.pkl"
            path = os.path.join(CACHE_DIR, filename)
            if os.path.exists(path):
                with open(path, "rb") as f:
                    entities.append(pickle.load(f))
                print(f"✅ Кеш: {username}")
            else:
                entity = await client.get_entity(username)
                with open(path, "wb") as f:
                    pickle.dump(entity, f)
                entities.append(entity)
                print(f"📥 Из сети: {username}")
        except Exception as e:
            print(f"❌ {username}: {e}")
    return entities

# ====== ЖИЗНЕЦИКЛ КЛИЕНТА ====================================================
shutdown = asyncio.Event()

def _handle_signal():
    shutdown.set()

async def run_client_forever(config):
    """Запуск клиента с авто-перезапуском при критических ошибках."""
    backoff = 5
    while not shutdown.is_set():
        client = None
        try:
            client = TelegramClient(config["session_name"], config["api_id"], config["api_hash"])
            await client.connect()
            if not await client.is_user_authorized():
                print(f"⚠️ Авторизуйте вручную: {config['session_name']}")
                await asyncio.sleep(backoff)
                continue

            print(f"✅ Подключено: {config['session_name']}")
            entities = await load_or_fetch_entities(client, GROUPS_TO_MONITOR)
            print(f"📡 {config['session_name']}: следит за {len(entities)} группами")

            @client.on(events.NewMessage(chats=entities))
            async def handler(event):
                if not event.raw_text:
                    return
                text_norm = normalize(event.raw_text)
                matched = [kw for kw in KEYWORDS if kw in text_norm]
                if matched:
                    try:
                        sender = await event.get_sender()
                        if getattr(sender, "username", None):
                            sender_name = f"@{sender.username}"
                        else:
                            fn = (sender.first_name or "").strip()
                            ln = (sender.last_name or "").strip()
                            sender_name = f"{fn} {ln}".strip()

                        link = f"https://t.me/{getattr(event.chat,'username',None)}/{event.id}" \
                               if getattr(event.chat, "username", None) else "🔒 Приватная группа"
                        now = datetime.now().strftime("%d.%m.%Y %H:%M")
                        message = (
                            f"[{now}] 📢 {event.chat.title}\n"
                            f"🔗 {link}\n"
                            f"👤 {sender_name}\n"
                            f"💬 {event.raw_text}"
                        )
                        await client.send_message(config["your_username"], message)
                        print(f"📬 {config['session_name']}: {event.chat.title} — {matched}")
                        update_analytics(event.chat.title, matched)
                    except Exception as e:
                        logging.error(f"Ошибка обработки: {e}")

            backoff = 5  # успешный старт — сбрасываем бэкофф
            await client.run_until_disconnected()

        except Exception as e:
            logging.critical(f"Критическая ошибка {config['session_name']}: {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        finally:
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass

# ====== MAIN =================================================================
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
