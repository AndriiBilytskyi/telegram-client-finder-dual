import asyncio
from telethon import TelegramClient, events

# Данные первого аккаунта
api_id = 26735008
api_hash = '6c35a6247e6b6502e5b79173b22af871'
session_name = 'session1'
your_username = 'Andrii_Bilytskyi'  # твой username без @

# Данные второго аккаунта
api_id_2 = 20903513
api_hash_2 = '0eb01bf47aeac4cbfd89fff140a4e06d'
session_name_2 = 'session2'
your_username_2 = 'UA_DE_22'

# Группы для отслеживания
groups_to_monitor = [
    '@NRWanzeigen', '@wuppertal_ua', '@ukraincifrankfurt',
    '@Manner_ClubNRW', '@uahelp_nrw_auto', '@avtoUAeuro', '@deutscheukraine',
    '@ukraineingermany1', '@jobinde', '@hamburg_ukrainians', '@ukrainians_in_germany1',
    '@berlin_ukrainians', '@UkraineinMunich', '@workeuropeplus',
    '@UA_in_Germany', '@dusseldorfukrain', '@TruckingNordrheinWestfalen', '@Berlin_UA2025',
    '@bonn_help', '@Ukrainer_in_Deutschland', '@GermanyTop1', '@germany_chatik',
    '@lifeinde', '@line_DE', '@nrw_anzeige', '@bochum_ua', '@POZITYV_PUTESHESTVIYA',
    '@uahelpkoelnanzeigen', '@cologne_help', '@TheGermany1', '@germania_migranty', '@GLOBUSEXPRESS'
    # Добавляй сюда группы, где ты реально состоишь
]

# Ключевые слова для поиска
keywords = ['адвокат', 'юрист', 'право', 'помощь адвоката', 'полиция','прокуратура', 'суд']

# Функция для подключения клиента
async def setup_client(api_id, api_hash, session_name, your_username):
    client = TelegramClient(session_name, api_id, api_hash)
    await client.start()

    @client.on(events.NewMessage(chats=groups_to_monitor))
    async def handler(event):
        message_text = event.raw_text.lower()
        if any(keyword in message_text for keyword in keywords):
            try:
                await client.send_message(your_username, f"🔔 Новое сообщение в {event.chat.title}:\n\n{event.raw_text}")
                print(f"✅ Сообщение отправлено от {session_name}")
            except Exception as e:
                print(f"⚠️ Ошибка отправки сообщения: {e}")

    return client

# Основная функция запуска
async def main():
    client1 = await setup_client(api_id, api_hash, session_name, your_username)
    client2 = await setup_client(api_id_2, api_hash_2, session_name_2, your_username_2)

    await asyncio.gather(client1.run_until_disconnected(), client2.run_until_disconnected())

# Запуск
if __name__ == "__main__":
    asyncio.run(main())
