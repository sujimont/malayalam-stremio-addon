import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession
import config

async def main():
    print("=" * 60)
    print(" Export Telegram Session String for Online / Cloud Deployment")
    print("=" * 60)
    
    client = TelegramClient(config.SESSION_NAME, config.API_ID, config.API_HASH)
    await client.connect()
    
    if not await client.is_user_authorized():
        print("[ERROR] Session is not authorized. Please run login.bat first.")
        await client.disconnect()
        return

    me = await client.get_me()
    session_str = StringSession.save(client.session)
    await client.disconnect()

    print(f"\nLogged in as: {me.first_name} (@{me.username})")
    print("\nCopy your TG_SESSION_STRING below (set this in your Cloud Environment Variables):\n")
    print("-" * 60)
    print(session_str)
    print("-" * 60)
    print("\nThis single string allows your cloud server to access Telegram without")
    print("needing OTP codes or copying .session files!\n")

if __name__ == "__main__":
    asyncio.run(main())
