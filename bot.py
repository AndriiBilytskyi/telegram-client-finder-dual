import os
import asyncio
import pickle
import logging
import json
import re
from datetime import datetime
from telethon import TelegramClient, events

# === Конфигурация аккаунтов ===
ACCOUNTS = [
    {
        "api_id": 26735008,
        "api_hash": '6c35a6247e6b6502e5b79173b22af871',
        "session_name": 'session1',
        "your_username": 'Andrii_Bilytskyi',
    },
    {
        "api_id": 20903513,
        "api_hash": '0eb01bf47aeac4cbfd89fff140a4e06d',
        "session_name": 'session2',
        "your_username": 'UA_DE_22',
    }
]

KEYWORDS = [
    'адвокат', 'адвоката', 'адвокатом', 'адвокату',
    'юрист', 'юриста', 'юристу', 'юристом',
    'помощь адвоката', 'полиция', 'прокуратура',
    'поліція', 'прокурор',
    'lawyer', 'attorney', 'police', 'prosecutor', 'court',
    'anwalt', 'rechtsanwalt', 'polizei', 'staatsanwalt', 'gericht'
]

GROUPS_TO_MONITOR = [
    '@NRWanzeigen', '@ukraineingermany1', '@ukrainians_in_germany1',
    '@berlin_ukrainians', '@deutscheukraine', '@ukraincifrankfurt',
    '@jobinde', '@hamburg_ukrainians', '@UkraineinMunich',
    '@workeuropeplus', '@UA_in_Germany', '@dusseldorfukrain',
    '@TruckingNordrheinWestfalen', '@Berlin_UA2025', '@bonn_help',
    '@GermanyTop1', '@germany_chatik', '@nrw_anzeige', '@bochum_ua',
    '@POZITYV_PUTESHESTVIYA', '@uahelpkoelnanzeigen', '@cologne_help',
    '@TheGermany1', '@germania_migranty', '@GLOBUSEXPRESS',
    '@nashipomogut', '@reklamnaia_ploshadka', '@ukr_de_essen',
    '@solingen_UA', '@keln_baraholka', '@baraholkaNRW',
    '@ukraine_dortmund', '@ukrainischinDortmund', '@UADuesseldorf',
    '@beauty_dusseldorf', '@pomoshukraineaachen', '@AhlenNRW',
    '@alsdorfua', '@aschafenburg', '@NA6R_hilft', '@bad4ua',
    '@badenbaden_lkr', '@kreiskleve', '@Bernkastel_Wittlich',
    '@bielefeldhelps', '@ukraine_bochum_support', '@uahelp_ruhrgebiet',
    '@DeutschlandBottrop', '@BS_UA_HELP', '@refugeesbremen',
    '@Bruchsal_Chat', '@Ukrainians_in_Calw', '@hilfe_ukraine_chemnitz',
    '@cottbus_ua', '@hamburg_ukraine_chat', '@Magdeburg_ukrainian',
    '@Fainy_Kiel', '@ukraine_in_Hanover', '@uahelfen_arbeit',
    '@bremen_hannover_dresden', '@ukraine_in_dresden', '@BavariaLife',
    '@ErfurtUA', '@save_ukraine_de_essen', '@MunchenBavaria',
    '@refugees_help_Koblenz', '@KaiserslauternUA', '@Karlsruhe_Ukraine',
    '@MunchenGessenBremen', '@chatFreiburg', '@Pfaffenhofen',
    '@deutschland_diaspora', '@Manner_ClubNRW', '@Ukrainer_in_Deutschland',
    '@Ukrainer_in_Wuppertal', '@ukrainians_in_hamburg_ua', '@ukrainians_berlin',
    '@berlinhelpsukrainians'
]

CACHE_DIR = "group_cache"
ANALYTICS_FILE = "analytics.json"

logging.basicConfig(filename="log.txt", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# === Нормализация текста ===
def normalize(text):
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def update_analytics(group_title, matched_keywords):
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
    os.makedirs(CACHE_DIR, exist_ok=True)
    entities = []
    for username in set(group_usernames):
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

async def setup_client(config):
    client = TelegramClient(config["session_name"], config["api_id"], config["api_hash"])
    await client.connect()

    if not await client.is_user_authorized():
        print(f"⚠️ Авторизуйте вручную: {config['session_name']}")
        return None

    print(f"✅ Подключено: {config['session_name']}")
    entities = await load_or_fetch_entities(client, GROUPS_TO_MONITOR)
    print(f"📡 {config['session_name']}: следит за {len(entities)} группами")

    @client.on(events.NewMessage(chats=entities))
    async def handler(event):
        text = normalize(event.raw_text)
        matched = [kw for kw in KEYWORDS if kw in text]
        if matched:
            try:
                sender = await event.get_sender()
                sender_name = f"@{sender.username}" if sender.username else f"{sender.first_name} {sender.last_name}".strip()
                link = f"https://t.me/{event.chat.username}/{event.id}" if event.chat.username else "🔒 Приватная группа"
                now = datetime.now().strftime("%d.%m.%Y %H:%M")
                message = f"[{now}] 📢 {event.chat.title}\n🔗 {link}\n👤 {sender_name}\n💬 {event.raw_text}"
                await client.send_message(config['your_username'], message)
                print(f"📬 {config['session_name']}: {event.chat.title} — {matched}")
                update_analytics(event.chat.title, matched)
            except Exception as e:
                logging.error(f"Ошибка обработки: {e}")
    return client

async def main():
    clients = []
    for config in ACCOUNTS:
        try:
            client = await setup_client(config)
            if client:
                clients.append(client)
        except Exception as e:
            logging.critical(f"Ошибка клиента {config['session_name']}: {e}")
    if clients:
        await asyncio.gather(*(client.run_until_disconnected() for client in clients))
    else:
        print("❌ Ни один клиент не работает")

if __name__ == "__main__":
    asyncio.run(main())
