import os
import asyncio
import pickle
import logging
import json
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

# === Ключевые слова ===
KEYWORDS = [
    'адвокат', 'адвоката', 'адвокатом', 'адвокату',
    'юрист', 'юриста', 'юристу', 'юристом',
    'помощь адвоката', 'полиция', 'прокуратура',
    'поліція', 'прокурор', 'суд',
    'lawyer', 'attorney', 'police', 'prosecutor', 'court',
    'anwalt', 'rechtsanwalt', 'polizei', 'staatsanwalt', 'gericht'
]

# === Группы ===
GROUPS_TO_MONITOR = list(set([
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
    '@deutschland_diaspora', '@Manner_ClubNRW', '@Ukrainer_in_Deutschland', '@Ukrainer_in_Wuppertal',
    '@ukrainians_in_hamburg_ua', '@ukrainians_berlin', '@berlinhelpsukrainians'
]))

# === Пути к файлам ===
CACHE_DIR = "group_cache"
ANALYTICS_FILE = "analytics.json"

# === Логирование ===
logging.basicConfig(filename="log.txt", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# === Аналитика ===
def update_analytics(group_title, matched_keywords):
    if os.path.exists(ANALYTICS_FILE):
        with open(ANALYTICS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {}

    group_data = data.get(group_title, {"total": 0, "keywords": {}})
    group_data["total"] += 1
    for kw in matched_keywords:
        group_data["keywords"][kw] = group_data["keywords"].get(kw, 0) + 1

    data[group_title] = group_data

    with open(ANALYTICS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# === Получение чатов с индивидуальным кешем ===
async def load_or_fetch_entities(client, group_usernames):
    os.makedirs(CACHE_DIR, exist_ok=True)
    entities = []
    seen = set()

    for username in group_usernames:
        if username in seen:
            continue
        seen.add(username)

        filename = f"{username.strip('@')}.pkl"
        cache_path = os.path.join(CACHE_DIR, filename)

        if os.path.exists(cache_path):
            try:
                with open(cache_path, "rb") as f:
                    entity = pickle.load(f)
                    entities.append(entity)
                    print(f"✅ Кешированная группа загружена: {username}")
                continue
            except Exception as e:
                print(f"⚠️ Ошибка чтения кеша {username}: {e}")

        try:
            entity = await client.get_entity(username)
            entities.append(entity)
            with open(cache_path, "wb") as f:
                pickle.dump(entity, f)
            print(f"📥 Группа загружена из сети и сохранена: {username}")
            await asyncio.sleep(1.5)
        except Exception as e:
            print(f"❌ Не удалось получить {username}: {e}")
            continue

    return entities


# === Настройка клиента ===
async def setup_client(api_id, api_hash, session_name, your_username, group_usernames):
    client = TelegramClient(session_name, api_id, api_hash)

    try:
        await client.connect()
        if not await client.is_user_authorized():
            print(f"⚠️ Сессия {session_name} не авторизована. Залогиньтесь вручную.")
            return None
    except Exception as e:
        print(f"❌ Ошибка подключения {session_name}: {e}")
        return None

    for _ in range(10):
        if client.is_connected():
            break
        print(f"⏳ Ожидание подключения {session_name}...")
        await asyncio.sleep(1)

    if not client.is_connected():
        print(f"❌ Клиент {session_name} не подключён.")
        return None

    print(f"✅ Клиент {session_name} подключён.")

    entities = await load_or_fetch_entities(client, group_usernames)

    @client.on(events.NewMessage(chats=entities))
    async def handler(event):
        message_text = event.raw_text.lower()
        matched = [kw for kw in KEYWORDS if kw in message_text]

        if matched:
            try:
                now = datetime.now().strftime("%d.%m.%Y %H:%M")
                sender = await event.get_sender()
                sender_name = (
                    f"@{sender.username}" if sender.username else
                    f"{sender.first_name or ''} {sender.last_name or ''}".strip() or
                    "Неизвестный пользователь"
                )

                link = f"https://t.me/{event.chat.username}/{event.id}" if event.chat.username else "🔒 Приватная группа"

                final_message = (
                    f"[{now}] 📢 Новое сообщение в группе: {event.chat.title}\n"
                    f"🔗 {link}\n\n"
                    f"👤 Отправитель: {sender_name}\n\n"
                    f"💬 Сообщение:\n{event.raw_text}"
                )

                await client.send_message(your_username, final_message)
                print(f"📬 Переслано в {your_username} | Ключевые слова: {', '.join(matched)}")
                update_analytics(event.chat.title, matched)

            except Exception as e:
                logging.error(f"❌ Ошибка при обработке сообщения: {e}")

    return client


# === Запуск клиентов ===
async def main():
    clients = []
    for acc in ACCOUNTS:
        client = await setup_client(
            acc["api_id"], acc["api_hash"],
            acc["session_name"], acc["your_username"],
            GROUPS_TO_MONITOR
        )
        if client:
            clients.append(client)

    if clients:
        await asyncio.gather(*(client.run_until_disconnected() for client in clients))
    else:
        print("❌ Ни один клиент не подключён.")


# === Защита от сбоев ===
async def safe_main():
    while True:
        try:
            await main()
        except Exception as e:
            logging.critical(f"🔥 Ошибка в основном цикле: {e}")
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(safe_main())
