import os

# Telegram API Credentials
API_ID = int(os.environ.get("API_ID", 38374919))
API_HASH = os.environ.get("API_HASH", "8f837fa18676cbab0c0c58a2b1cc7987")
SESSION_NAME = os.environ.get("SESSION_NAME", "malayalam_stremio")
TG_SESSION_STRING = os.environ.get("TG_SESSION_STRING", "")

# Target Channel & Bot
CHANNEL_USERNAME = os.environ.get("CHANNEL_USERNAME", "mallutvserials1")
DEFAULT_BOT_USERNAME = os.environ.get("DEFAULT_BOT_USERNAME", "mallu3serial_bot")

# Server Port
PORT = int(os.environ.get("PORT", 7000))
