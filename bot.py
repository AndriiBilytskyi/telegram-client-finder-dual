
from telethon import TelegramClient, events
import os
import asyncio

# Данные первого аккаунта
api_id_1 = int(os.getenv('API_ID_1'))
api_hash_1 = os.getenv('API_HASH_1')
phone_number_1 = os.getenv('PHONE_NUMBER_1')
your_username_1 = os.getenv('YOUR_USERNAME_1')

# Данные второго аккаунта
api_id_2 = int(os.getenv('API_ID_2'))
api_hash_2 = os.getenv('API_HASH_2')
phone_number_2 = os.getenv('PHONE_NUMBER_2')
your_username_2 = os.getenv('YOUR_USERNAME_2')

# Список групп
groups = [
    'NRWanzeigen', 'wuppertal_ua', 'ukraincifrankfurt',
    'Manner_ClubNRW', 'uahelp_nrw_auto', 'avtoUAeuro', 'deutscheukraine',
    'ukraineingermany1', 'jobinde', 'hamburg_ukrainians', 'ukrainians_in_germany1',
    'berlin_ukrainians', 'UkraineinMunich', 'workeuropeplus',
    'UA_in_Germany', 'dusseldorfukrain', 'TruckingNordrheinWestfalen', 'Berlin_UA2025',
    'bonn_help', 'Ukrainer_in_Deutschland', 'GermanyTop1', 'germany_chatik',
    'lifeinde', 'line_DE', 'nrw_anzeige', 'bochum_ua', 'POZITYV_PUTESHESTVIYA',
    'uahelpkoelnanzeigen', 'cologne_help', 'TheGermany1', 'germania_migranty', 'GLOBUSEXPRESS'
]

# Ключевые слова
keywords = ['адвокат']

# Инициализация клиентов
client1 = TelegramClient('session1', api_id_1, api_hash_1)
client2 = TelegramClient('session2', api_id_2, api_hash_2)

async def setup_client(client, phone_number, your_username, client_name):
    await client.start(phone=phone_number)

    @client.on(events.NewMessage(chats=groups))
    async def handler(event):
        message_text = event.raw_text.lower()
        for keyword in keywords:
            if keyword in message_text:
                print(f'[{client_name}] Найдено сообщение: {event.raw_text}')
                await client.send_message(your_username, f"[{client_name}] Новое сообщение в {event.chat.title}:\n\n{event.raw_text}")

                break

async def main():
    await asyncio.gather(
        setup_client(client1, phone_number_1, your_username_1, 'Аккаунт 1'),
        setup_client(client2, phone_number_2, your_username_2, 'Аккаунт 2'),
    )
    print("Оба бота запущены и следят за группами...")
    await asyncio.gather(
        client1.run_until_disconnected(),
        client2.run_until_disconnected()
    )

asyncio.run(main())
