import os
import asyncio
from datetime import datetime
from telethon import TelegramClient, events

# Данные первого аккаунта
api_id = 26735008
api_hash = '6c35a6247e6b6502e5b79173b22af871'
session_name = 'session1'
your_username = 'Andrii_Bilytskyi'

# Данные второго аккаунта
api_id_2 = 20903513
api_hash_2 = '0eb01bf47aeac4cbfd89fff140a4e06d'
session_name_2 = 'session2'
your_username_2 = 'UA_DE_22'

# Список групп
groups_to_monitor = [
    '@NRWanzeigen',
    '@wuppertal_ua',
    '@ukraineingermany1',
    '@ukrainians_in_germany1',
    '@berlin_ukrainians',
    '@deutscheukraine',
    '@ukraincifrankfurt',
    '@Manner_ClubNRW',
    '@uahelp_nrw_auto',
    '@avtoUAeuro',
    '@jobinde',
    '@hamburg_ukrainians',
    '@UkraineinMunich',
    '@workeuropeplus',
    '@UA_in_Germany',
    '@dusseldorfukrain',
    '@TruckingNordrheinWestfalen',
    '@Berlin_UA2025',
    '@bonn_help',
    '@Ukrainer_in_Deutschland',
    '@GermanyTop1',
    '@germany_chatik',
    '@lifeinde',
    '@line_DE',
    '@nrw_anzeige',
    '@bochum_ua',
    '@POZITYV_PUTESHESTVIYA',
    '@uahelpkoelnanzeigen',
    '@cologne_help',
    '@TheGermany1',
    '@germania_migranty',
    '@GLOBUSEXPRESS', 
    '@nashipomogut',
    '@reklamnaia_ploshadka',
    '@cologne_market',
    '@ukr_de_essen',
    '@solingen_UA',
    '@keln_baraholka',
    '@baraholkaNRW',
    '@ukraine_dortmund',
    '@ukrainischinDortmund',
    '@UADuesseldorf',
    '@beauty_dusseldorf',
    '@pomoshukraineaachen',
    '@AhlenNRW',
    '@alsdorfua',
    '@aschafenburg',
    '@NA6R_hilft',
    '@bad4ua',
    '@badenbaden_lkr',
    '@kreiskleve',
    '@Bernkastel_Wittlich',
    '@bielefeldhelps',
    '@ukraine_bochum_support',
    '@uahelp_ruhrgebiet',
    '@DeutschlandBottrop',
    '@BS_UA_HELP',
    '@refugeesbremen',
    '@Bruchsal_Chat',
    '@ukrainiansinboeblengengermany',
    '@Ukrainians_in_Calw',
    '@hilfe_ukraine_chemnitz',
    '@cottbus_ua', '@hamburg_ukraine_chat',
    '@Magdeburg_ukrainian', '@Fainy_Kiel', '@refugeesbremen',
    '@ukraine_in_Hanover', '@uahelfen_arbeit', '@bremen_hannover_dresden',
    '@ukraine_in_dresden', '@BavariaLife', '@ErfurtUA', '@save_ukraine_de_essen', '@MunchenBavaria', '@levukrgermany', 
    '@refugees_help_Koblenz','@KaiserslauternUA', '@Karlsruhe_Ukraine', '@MunchenGessenBremen', '@chatFreiburg', '@+hrLS9t5A3L0wNTk6', '@Pfaffenhofen']

# Ключевые слова
keywords = ['адвокат', 'адвоката', 'адвокатом', 'адвокату', 'юрист', 'юриста', 'юристу', 'юристом', 'помощь адвоката', 'полиция','прокуратура']

async def setup_client(api_id, api_hash, session_name, your_username):
    client = TelegramClient(session_name, api_id, api_hash)
    await client.start()

    groups_entities = []
    for username in groups_to_monitor:
        try:
            entity = await client.get_entity(username)
            groups_entities.append(entity)
            print(f"✅ Группа найдена: {username}")
        except Exception as e:
            print(f"⚠️ Ошибка получения группы {username}: {e}")

    @client.on(events.NewMessage(chats=groups_entities))
    async def handler(event):
        message_text = event.raw_text.lower()

        if any(keyword in message_text for keyword in keywords):
            try:
                now = datetime.now().strftime("%d.%m.%Y %H:%M")

                sender = await event.get_sender()

                sender_name = ""
                if sender.username:
                    sender_name = f"@{sender.username}"
                elif sender.first_name or sender.last_name:
                    sender_name = f"{sender.first_name or ''} {sender.last_name or ''}".strip()
                else:
                    sender_name = "Неизвестный пользователь"

                final_message = (
                    f"[{now}] 📢 Новое сообщение в группе {event.chat.title}:\n\n"
                    f"👤 Отправитель: {sender_name}\n\n"
                    f"💬 Сообщение:\n{event.raw_text}"
                )

                await client.send_message(your_username, final_message)
                print(f"✅ Сообщение переслано от {session_name}")
            except Exception as e:
                print(f"⚠️ Ошибка отправки сообщения: {e}")

    return client

async def main():
    client1 = await setup_client(api_id, api_hash, session_name, your_username)
    client2 = await setup_client(api_id_2, api_hash_2, session_name_2, your_username_2)

    await asyncio.gather(client1.run_until_disconnected(), client2.run_until_disconnected())

if __name__ == "__main__":
    asyncio.run(main())
