import asyncio
import os
import requests
from telethon import TelegramClient
import config

POSTERS_DIR = os.path.join(os.path.dirname(__file__), "posters")
os.makedirs(POSTERS_DIR, exist_ok=True)

# Curated online posters
CURATED_POSTERS = {
    "mls_bigg_boss": "https://upload.wikimedia.org/wikipedia/en/7/72/BIGG_Boss_malayalam_season_8_logo.jpg",
    "mls_patharamattu": "https://upload.wikimedia.org/wikipedia/en/thumb/a/ad/Patharamattu.jpeg/400px-Patharamattu.jpeg",
    "mls_chempaneer_poovu": "https://upload.wikimedia.org/wikipedia/en/thumb/c/c4/Chempaneer_Poovu.jpeg/400px-Chempaneer_Poovu.jpeg",
    "mls_chembarathy": "https://upload.wikimedia.org/wikipedia/en/5/58/Chembaruthi.jpg",
    "mls_mazha_thorum_munpe": "https://upload.wikimedia.org/wikipedia/en/thumb/d/d7/Mazha_Thorum_Munpe.jpg/400px-Mazha_Thorum_Munpe.jpg",
    "mls_kattathe_kilikoodu": "https://upload.wikimedia.org/wikipedia/en/thumb/6/69/Kattathe_Kilikkoodu.jpeg/400px-Kattathe_Kilikkoodu.jpeg",
}

def download_curated():
    headers = {"User-Agent": "Mozilla/5.0"}
    for slug, url in CURATED_POSTERS.items():
        dest = os.path.join(POSTERS_DIR, f"{slug}.jpg")
        if not os.path.exists(dest):
            try:
                r = requests.get(url, headers=headers, timeout=8)
                if r.status_code == 200 and len(r.content) > 1000:
                    with open(dest, "wb") as f:
                        f.write(r.content)
                    print(f"Downloaded curated poster for {slug}: {len(r.content)} bytes")
            except Exception as e:
                print(f"Failed to download {slug}: {e}")

async def extract_telegram_thumbs():
    try:
        client = TelegramClient(config.SESSION_NAME, config.API_ID, config.API_HASH)
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            return
            
        msgs = await client.get_messages("me", limit=30)
        for m in msgs:
            if m.document and m.document.thumbs and m.file.name:
                name_clean = m.file.name.lower()
                for slug in ["bigg_boss", "mazha_thorum_munpe", "kattathe_kilikoodu", "karnan", "patharamattu", "chembarathy", "chempaneer_poovu", "valyettan"]:
                    key = f"mls_{slug}"
                    dest = os.path.join(POSTERS_DIR, f"{key}.jpg")
                    if slug.replace("_", "") in name_clean.replace("_", "") and not os.path.exists(dest):
                        await client.download_media(m.media, thumb=-1, file=dest)
                        print(f"Extracted real video thumb for {key} -> {dest}")
        await client.disconnect()
    except Exception as e:
        print(f"Extract error: {e}")

if __name__ == "__main__":
    download_curated()
    asyncio.run(extract_telegram_thumbs())
