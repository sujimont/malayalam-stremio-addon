import asyncio
from telethon import TelegramClient
import config

async def main():
    print("=" * 60)
    print(" Telegram Login Setup for Malayalam Serials Stremio Addon")
    print("=" * 60)
    print("This will log into your Telegram account once and save")
    print(f"your session to '{config.SESSION_NAME}.session'.")
    print("=" * 60)
    
    client = TelegramClient(config.SESSION_NAME, config.API_ID, config.API_HASH)
    await client.start()
    
    me = await client.get_me()
    print("\n[SUCCESS] Successfully logged in as:", me.first_name, f"(@{me.username})")
    print(f"Session saved as '{config.SESSION_NAME}.session'.")
    print("You can now run 'python server.py' to start the Stremio Addon!")
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
