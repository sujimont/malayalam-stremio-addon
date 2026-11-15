import asyncio
from contextlib import asynccontextmanager
import os
import re
import time
import unicodedata
from typing import Dict, List, Optional
import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from telethon import TelegramClient
from telethon.tl.functions.contacts import UnblockRequest
from telethon.tl.types import MessageEntityTextUrl

import config

# Global Telethon Client & Bot Interaction Lock
tg_client: Optional[TelegramClient] = None
bot_lock = asyncio.Lock()

# Cache directory on local drive (non-OneDrive for high-speed I/O)
CACHE_DIR = os.path.join(
    os.environ.get("LOCALAPPDATA", os.environ.get("TEMP", ".")), "stremio_tg_cache"
)
os.makedirs(CACHE_DIR, exist_ok=True)

# Local directory for posters & banners
POSTERS_DIR = os.path.join(os.path.dirname(__file__), "posters")
os.makedirs(POSTERS_DIR, exist_ok=True)

# Curated online posters for high-definition cards
CURATED_POSTERS = {
    "mls_bigg_boss": "https://upload.wikimedia.org/wikipedia/en/7/72/BIGG_Boss_malayalam_season_8_logo.jpg",
    "mls_patharamattu": "https://upload.wikimedia.org/wikipedia/en/a/ad/Patharamattu.jpeg",
    "mls_chempaneer_poovu": "https://upload.wikimedia.org/wikipedia/en/c/c4/Chempaneer_Poovu.jpeg",
    "mls_chembarathy": "https://upload.wikimedia.org/wikipedia/en/5/58/Chembaruthi.jpg",
    "mls_mazha_thorum_munpe": "https://upload.wikimedia.org/wikipedia/en/thumb/d/d7/Mazha_Thorum_Munpe.jpg/400px-Mazha_Thorum_Munpe.jpg",
    "mls_kattathe_kilikoodu": "https://upload.wikimedia.org/wikipedia/en/thumb/6/69/Kattathe_Kilikkoodu.jpeg/400px-Kattathe_Kilikkoodu.jpeg",
    "mls_valyettan": "https://www.keralatv.in/media/2026/01/Valyettan-Serial-Zee-Keralam.jpg",
    "mls_kanmashi": "https://www.keralatv.in/media/2026/09/Kanmashi-Serial-Zee-Keralam-Channel-640x360.jpg",
}

# Cache for Scraped Shows from Channel
CACHE_TTL = 300  # 5 minutes
last_scrape_time = 0
cached_serials: Dict[str, dict] = {}

# Show Episodes Cache: serial_id -> list of episode dicts
cached_show_episodes: Dict[str, List[dict]] = {}
cached_episode_streams: Dict[str, List[dict]] = {}
payload_to_serial: Dict[str, str] = {}

# Media cache for resolved Telegram messages: payload -> media_info dict
resolved_media: Dict[str, dict] = {}


# ==========================================
# Quality Scoring (720p Optimal Sweet Spot)
# ==========================================


def quality_priority(quality_str: str) -> int:
    """Prioritizes 720p as optimal for streaming performance and bandwidth"""
    ql = quality_str.lower()
    if "720" in ql:
        return 100  # 720p is optimal: crisp HD, fast buffering, smooth playback
    if "1080" in ql:
        return 80   # High quality fallback if 720p not available
    if "576" in ql:
        return 60
    if "480" in ql:
        return 40
    if "360" in ql:
        return 20
    return 10


def get_best_stream_option(options: List[dict]) -> dict:
    """Picks 720p if available, otherwise best alternative"""
    def score(opt):
        q_val = quality_priority(opt.get("quality", ""))
        sz_str = opt.get("size", "")
        sz_num = 0.0
        m = re.search(r"([\d\.]+)\s*([KMGT]B)", sz_str, re.IGNORECASE)
        if m:
            val = float(m.group(1))
            unit = m.group(2).upper()
            if "G" in unit:
                val *= 1024
            elif "K" in unit:
                val /= 1024
            sz_num = val
        return (q_val, sz_num)

    return max(options, key=score)


# ==========================================
# High-Speed Smooth Progressive Streamer
# ==========================================


class ProgressiveVideoFile:
    """Manages background download and progressive disk streaming for a video"""

    def __init__(self, payload: str, total_size: int, client: TelegramClient, media_msg):
        self.payload = payload
        self.total_size = total_size
        self.client = client
        self.media_msg = media_msg
        self.file_path = os.path.join(CACHE_DIR, f"{payload}.mkv")
        self.part_path = os.path.join(CACHE_DIR, f"{payload}.part")
        self.downloaded_bytes = 0
        self.is_complete = (
            os.path.exists(self.file_path)
            and os.path.getsize(self.file_path) == total_size
        )
        self.task: Optional[asyncio.Task] = None
        self.error: Optional[str] = None

        if self.is_complete:
            self.downloaded_bytes = total_size
        elif os.path.exists(self.part_path):
            self.downloaded_bytes = os.path.getsize(self.part_path)

    def start_download(self):
        if self.is_complete or (self.task and not self.task.done()):
            return
        self.task = asyncio.create_task(self._download_loop())

    async def _download_loop(self):
        try:
            if (
                os.path.exists(self.file_path)
                and os.path.getsize(self.file_path) == self.total_size
            ):
                self.is_complete = True
                self.downloaded_bytes = self.total_size
                return

            offset = 0
            if os.path.exists(self.part_path):
                offset = os.path.getsize(self.part_path)
            self.downloaded_bytes = offset

            print(f"[CACHE] High-speed single-socket download for {self.payload} from byte {offset}...")
            chunk_size = 1024 * 1024  # 1 MB chunk for maximum throughput

            with open(self.part_path, "ab" if offset > 0 else "wb") as f:
                async for chunk in self.client.iter_download(
                    self.media_msg.media,
                    offset=offset,
                    chunk_size=chunk_size,
                    request_size=chunk_size,
                ):
                    f.write(chunk)
                    f.flush()
                    self.downloaded_bytes += len(chunk)

            # Finalize completed file
            if os.path.exists(self.part_path):
                if os.path.exists(self.file_path):
                    os.remove(self.file_path)
                os.rename(self.part_path, self.file_path)

            self.is_complete = True
            print(f"[CACHE] Download finished: {self.payload} completely saved to disk.")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.error = str(e)
            print(f"[CACHE] Download loop notice: {e}")

    async def stream_range(self, start: int, length: int):
        """Streams bytes smoothly from local disk buffer with initial prebuffer and zero stutter"""
        self.start_download()

        # Handle tail probes (e.g. MKV seek index at the very end of file)
        # without forcing the player to wait for the whole file to download
        if (start > self.total_size - 2 * 1024 * 1024) and not self.is_complete:
            chunk_size = 256 * 1024
            remaining = length
            try:
                async for chunk in self.client.iter_download(
                    self.media_msg.media,
                    offset=start,
                    limit=length,
                    chunk_size=chunk_size,
                    request_size=chunk_size,
                ):
                    to_yield = min(len(chunk), remaining)
                    yield chunk[:to_yield]
                    remaining -= to_yield
                    if remaining <= 0:
                        break
            except Exception as e:
                print(f"[STREAM] Tail probe notice: {e}")
            return

        # Prebuffer 3.5 MB (~15-20 seconds of video) when starting playback from byte 0.
        # This gives a massive head start so the player buffer never empties!
        if start == 0 and not self.is_complete:
            prebuffer_target = min(3500000, self.total_size)
            wait_count = 0
            while self.downloaded_bytes < prebuffer_target and not self.is_complete:
                await asyncio.sleep(0.04)
                wait_count += 1
                if wait_count > 120:  # 5 seconds max initial cushion
                    break

        curr_pos = start
        remaining = length

        while remaining > 0:
            # Poll disk buffer every 35ms until enough bytes are written
            wait_spins = 0
            while self.downloaded_bytes <= curr_pos and not self.is_complete:
                if self.error:
                    break
                await asyncio.sleep(0.035)
                wait_spins += 1
                if wait_spins > 300:  # 10.5 seconds timeout
                    print("[STREAM] Buffer read timeout")
                    break

            target_file = self.file_path if self.is_complete else self.part_path
            if not os.path.exists(target_file):
                await asyncio.sleep(0.04)
                continue

            available_on_disk = os.path.getsize(target_file)
            readable = min(remaining, available_on_disk - curr_pos)
            if readable <= 0:
                if self.is_complete:
                    break
                await asyncio.sleep(0.04)
                continue

            to_read_now = min(readable, 512 * 1024)
            with open(target_file, "rb") as f:
                f.seek(curr_pos)
                data = f.read(to_read_now)

            if data:
                yield data
                curr_pos += len(data)
                remaining -= len(data)
            else:
                await asyncio.sleep(0.04)


active_video_buffers: Dict[str, ProgressiveVideoFile] = {}


def get_base_url(request: Request) -> str:
    """Accurately computes the base URL supporting both local HTTP and cloud reverse-proxy HTTPS"""
    host = request.headers.get("host", f"127.0.0.1:{config.PORT}")
    proto = request.headers.get("x-forwarded-proto")
    if proto:
        scheme = proto.split(",")[0].strip()
    else:
        scheme = "https" if ("127.0.0.1" not in host and "localhost" not in host) else "http"
    return f"{scheme}://{host}"


def get_serial_artwork(serial_id: str, title: str, base_url: str = "") -> dict:
    """Returns local cached poster URL, curated poster URL, or stylized banner"""
    local_file = f"{serial_id}.jpg"
    local_path = os.path.join(POSTERS_DIR, local_file)
    if os.path.exists(local_path):
        poster_url = f"{base_url}/posters/{local_file}" if base_url else f"/posters/{local_file}"
        return {"poster": poster_url, "background": poster_url}

    if serial_id in CURATED_POSTERS:
        poster_url = CURATED_POSTERS[serial_id]
        return {"poster": poster_url, "background": poster_url}

    clean = re.sub(r"[^a-zA-Z0-9 ]+", "", title).strip().replace(" ", "+")
    return {
        "poster": f"https://placehold.co/400x600/18181b/f43f5e.png?text={clean}&font=roboto",
        "background": f"https://placehold.co/1280x720/18181b/f43f5e.png?text={clean}&font=roboto",
    }


def scrape_channel() -> Dict[str, dict]:
    """Scrapes the public Telegram channel for latest serial posts"""
    global last_scrape_time, cached_serials

    now = time.time()
    if cached_serials and (now - last_scrape_time < CACHE_TTL):
        return cached_serials

    url = f"https://t.me/s/{config.CHANNEL_USERNAME}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    try:
        resp = requests.get(url, headers=headers, timeout=12)
        resp.raise_for_status()
    except Exception as e:
        print(f"[ERROR] Failed to fetch channel {url}: {e}")
        return cached_serials

    soup = BeautifulSoup(resp.text, "html.parser")
    serials: Dict[str, dict] = {}

    for msg in soup.select(".tgme_widget_message"):
        text_el = msg.select_one(".tgme_widget_message_text")
        btn = msg.select_one(".tgme_widget_message_inline_button")
        if not text_el or not btn:
            continue

        raw_text = text_el.get_text("\n")
        text = unicodedata.normalize("NFKD", raw_text)
        btn_href = btn.get("href", "")

        fn = re.search(r"File\s*Name\s*:\s*([^\n\r]+)", text, re.IGNORECASE)
        ep = re.search(r"Episode\s*:\s*([^\n\r]+)", text, re.IGNORECASE)
        ss = re.search(r"Season\s*:\s*([^\n\r]+)", text, re.IGNORECASE)
        ql = re.search(r"Quality\s*:\s*([^\n\r]+)", text, re.IGNORECASE)

        if not fn:
            continue

        name = fn.group(1).strip()
        ep_str = ep.group(1).strip() if ep else "1"
        season = int(ss.group(1).strip()) if ss and ss.group(1).strip().isdigit() else 1
        quality = ql.group(1).strip() if ql else "720p"

        slug = re.sub(r"[^a-zA-Z0-9]+", "_", name.lower()).strip("_")
        serial_id = f"mls_{slug}"

        bot_user = config.DEFAULT_BOT_USERNAME
        bot_payload = ""
        if "t.me/" in btn_href and "?start=" in btn_href:
            parts = btn_href.split("?start=")
            bot_user = parts[0].split("t.me/")[-1].strip("/")
            bot_payload = parts[1]

        if serial_id not in serials:
            serials[serial_id] = {
                "id": serial_id,
                "name": name,
                "type": "series",
                "latest_ep": ep_str,
                "quality": quality,
                "bot_user": bot_user,
                "bot_payload": bot_payload,
            }

    cached_serials = serials
    last_scrape_time = now
    return serials


def parse_page_episodes(search_msg, episodes_map, serial_id):
    """Parses a search results page from the bot into episodes_map and cached_episode_streams"""
    global cached_episode_streams

    urls = [
        ent.url.split("start=")[-1]
        for ent in (search_msg.entities or [])
        if isinstance(ent, MessageEntityTextUrl) and "start=" in ent.url
    ]

    lines = [
        line.strip()
        for line in search_msg.raw_text.split("\n")
        if re.match(r"^\d+\.\s*\[", line.strip())
    ]

    for i in range(min(len(lines), len(urls))):
        line = lines[i]
        payload = urls[i]
        payload_to_serial[payload] = serial_id

        ep_m = re.search(r"S(\d+)E(\d+)", line, re.IGNORECASE)
        if ep_m:
            season = int(ep_m.group(1))
            ep_num = int(ep_m.group(2))
        else:
            single_ep_m = re.search(r"(\d+)", line)
            season = 1
            ep_num = int(single_ep_m.group(1)) if single_ep_m else 1

        sz_m = re.search(r"\[([\d\.]+\s*[KMGT]B)\]", line, re.IGNORECASE)
        size_str = sz_m.group(1) if sz_m else ""

        ql_m = re.search(r"(\d+p)", line, re.IGNORECASE)
        quality = ql_m.group(1) if ql_m else "720p"

        title_clean = re.sub(r"^\d+\.\s*", "", line)
        video_id = f"{serial_id}:{season}:{ep_num}"

        if video_id not in cached_episode_streams:
            cached_episode_streams[video_id] = []

        if not any(opt["payload"] == payload for opt in cached_episode_streams[video_id]):
            cached_episode_streams[video_id].append(
                {
                    "quality": quality,
                    "size": size_str,
                    "title": title_clean,
                    "payload": payload,
                }
            )

        if ep_num not in episodes_map:
            episodes_map[ep_num] = {
                "id": video_id,
                "season": season,
                "episode": ep_num,
            }


async def fetch_episodes_from_bot(serial_id: str, max_pages: int = 8) -> List[dict]:
    """Queries @mallu3serial_bot and paginates through multiple pages to fetch past episodes"""
    global tg_client, cached_show_episodes, cached_episode_streams, cached_serials, bot_lock

    if serial_id in cached_show_episodes and len(cached_show_episodes[serial_id]) > 5:
        return cached_show_episodes[serial_id]

    if tg_client is None or not tg_client.is_connected():
        print("[WARN] Telegram client not connected, returning fallback episode.")
        return []

    if serial_id not in cached_serials:
        scrape_channel()

    s_data = cached_serials.get(serial_id)
    if not s_data:
        return []

    bot_user = s_data["bot_user"]
    bot_payload = s_data["bot_payload"]
    episodes_map: Dict[int, dict] = {}

    async with bot_lock:
        print(f"[META] Fetching episode archive for '{s_data['name']}': /start {bot_payload}")
        sent_msg = await tg_client.send_message(bot_user, f"/start {bot_payload}")

        search_msg = None
        for _ in range(16):
            await asyncio.sleep(0.5)
            msgs = await tg_client.get_messages(bot_user, min_id=sent_msg.id, limit=5)
            for m in msgs:
                if m.id > sent_msg.id and m.text and ("ᴛᴏᴛᴀʟ ꜰɪʟᴇꜱ" in m.text or "ʀᴇꜱᴜʟᴛꜱ" in m.text):
                    search_msg = m
                    break
            if search_msg:
                break

        if not search_msg:
            print(f"[WARN] No search response from bot for {s_data['name']} in time.")
            return []

        # Paginate through pages to collect older episodes
        for page in range(1, max_pages + 1):
            parse_page_episodes(search_msg, episodes_map, serial_id)

            if page == max_pages:
                break

            next_btn = None
            for row in search_msg.buttons or []:
                for b in row:
                    if "ɴᴇxᴛ" in b.text or "⋟" in b.text:
                        next_btn = b
                        break
                if next_btn:
                    break

            if not next_btn:
                break

            try:
                await next_btn.click()
                await asyncio.sleep(0.4)
                search_msg = await tg_client.get_messages(bot_user, ids=search_msg.id)
            except Exception as e:
                print(f"[PAGINATE] Click next error on page {page}: {e}")
                break

    # Build final episode list WITHOUT dummy released dates
    final_episodes = []
    for ep_num, ep_data in episodes_map.items():
        v_id = ep_data["id"]
        opts = cached_episode_streams.get(v_id, [])
        if opts:
            best_opt = get_best_stream_option(opts)
            best_q = best_opt["quality"]
        else:
            best_q = s_data.get("quality", "720p")

        final_episodes.append(
            {
                "id": v_id,
                "title": f"Episode {ep_num} ({best_q})",
                "season": ep_data["season"],
                "episode": ep_num,
            }
        )

    sorted_episodes = sorted(
        final_episodes, key=lambda x: x["episode"], reverse=True
    )
    cached_show_episodes[serial_id] = sorted_episodes
    print(f"[OK] Cached {len(sorted_episodes)} episodes for '{s_data['name']}' (720p preferred, dates omitted)")
    return sorted_episodes


# ==========================================
# Lifespan Management (Startup & Shutdown)
# ==========================================


@asynccontextmanager
async def lifespan(app: FastAPI):
    global tg_client
    session_file = f"{config.SESSION_NAME}.session"

    print("\n" + "=" * 60)
    print(" Malayalam Serials Stremio Addon Server (Butter-Smooth 720p Stream)")
    print("=" * 60)

    scrape_channel()
    print(f"[OK] Scraped {len(cached_serials)} Malayalam serials into catalog.")

    if config.TG_SESSION_STRING:
        print("[OK] Found TG_SESSION_STRING environment variable. Connecting to Telegram...")
        from telethon.sessions import StringSession
        tg_client = TelegramClient(StringSession(config.TG_SESSION_STRING), config.API_ID, config.API_HASH)
        await tg_client.connect()
    elif os.path.exists(session_file):
        print(f"[OK] Found Telegram session '{session_file}'. Connecting...")
        tg_client = TelegramClient(config.SESSION_NAME, config.API_ID, config.API_HASH)
        await tg_client.connect()
        if await tg_client.is_user_authorized():
            me = await tg_client.get_me()
            print(f"[OK] Telegram authenticated as: {me.first_name} (@{me.username})")
            try:
                bot_entity = await tg_client.get_entity(config.DEFAULT_BOT_USERNAME)
                await tg_client(UnblockRequest(id=bot_entity))
                print(f"[OK] Verified @{config.DEFAULT_BOT_USERNAME} is unblocked.")
            except Exception as e:
                print(f"[NOTE] Bot check: {e}")

            # Scan Saved Messages to cache any existing show thumbnails
            try:
                saved_msgs = await tg_client.get_messages("me", limit=35)
                for m in saved_msgs:
                    if m.document and m.document.thumbs and m.file.name:
                        fn_clean = m.file.name.lower().replace("_", "")
                        for s_id in cached_serials.keys():
                            s_slug = s_id.replace("mls_", "").replace("_", "")
                            dest = os.path.join(POSTERS_DIR, f"{s_id}.jpg")
                            if s_slug in fn_clean and not os.path.exists(dest):
                                await tg_client.download_media(m.media, thumb=-1, file=dest)
                                print(f"[POSTER] Auto-saved poster for {s_id} from Saved Messages")
            except Exception as e:
                print(f"[NOTE] Saved messages thumbnail scan: {e}")
        else:
            print("[WARN] Session file exists but is not authorized. Run 'login.bat'.")
    else:
        print("[NOTICE] Telegram session not found yet.")
        print(" -> Double-click 'login.bat' to log into Telegram.")

    print(f"\n[READY] Addon running at: http://127.0.0.1:{config.PORT}/manifest.json")
    print(f"[CACHE] Fast disk cache located at: {CACHE_DIR}")
    print("=" * 60 + "\n")

    yield

    if tg_client and tg_client.is_connected():
        await tg_client.disconnect()


app = FastAPI(title="Malayalam Serials Stremio Addon", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/posters", StaticFiles(directory=POSTERS_DIR), name="posters")


# ==========================================
# Stremio Addon Protocol Endpoints
# ==========================================


@app.get("/manifest.json")
def get_manifest():
    return {
        "id": "org.malayalam.serials",
        "version": "1.0.0",
        "name": "Malayalam Serials Latest",
        "description": "Latest Malayalam serial episodes from Telegram (@mallutvserials1)",
        "logo": "https://placehold.co/256x256/1e1b4b/f43f5e.png?text=MLS&font=roboto",
        "resources": ["catalog", "meta", "stream"],
        "types": ["series"],
        "catalogs": [
            {
                "type": "series",
                "id": "malayalam_serials_latest",
                "name": "Malayalam Serials Latest",
                "extra": [{"name": "search", "isRequired": False}],
            }
        ],
    }


@app.get("/catalog/series/malayalam_serials_latest.json")
@app.get("/catalog/series/malayalam_serials_latest/{extra}.json")
def get_catalog(request: Request, extra: Optional[str] = None):
    serials = scrape_channel()
    query = ""
    if extra and "search=" in extra:
        query = extra.split("search=")[-1].lower()

    base_url = get_base_url(request)
    metas = []
    for s_id, s_data in serials.items():
        if query and query not in s_data["name"].lower():
            continue

        artwork = get_serial_artwork(s_id, s_data["name"], base_url)
        metas.append(
            {
                "id": s_id,
                "type": "series",
                "name": s_data["name"],
                "poster": artwork["poster"],
                "background": artwork["background"],
                "posterShape": "poster",
                "description": f"Latest: Episode {s_data['latest_ep']} (720p)",
            }
        )

    return {"metas": metas}


@app.get("/meta/series/{id}.json")
async def get_meta(id: str, request: Request):
    serials = scrape_channel()
    if id not in serials:
        raise HTTPException(status_code=404, detail="Serial not found")

    s_data = serials[id]
    base_url = get_base_url(request)
    artwork = get_serial_artwork(id, s_data["name"], base_url)
    episodes = await fetch_episodes_from_bot(id)

    if not episodes:
        ep_num = (
            int(re.search(r"\d+", s_data["latest_ep"]).group(0))
            if re.search(r"\d+", s_data["latest_ep"])
            else 1
        )
        episodes = [
            {
                "id": f"{id}:1:{ep_num}",
                "title": f"Episode {s_data['latest_ep']} (720p)",
                "season": 1,
                "episode": ep_num,
            }
        ]

    return {
        "meta": {
            "id": s_data["id"],
            "type": "series",
            "name": s_data["name"],
            "poster": artwork["poster"],
            "background": artwork["background"],
            "posterShape": "poster",
            "description": f"Malayalam TV Serial: {s_data['name']}\nEpisodes from @mallutvserials1",
            "videos": episodes,
        }
    }


@app.get("/stream/series/{video_id}.json")
async def get_stream(video_id: str, request: Request):
    """Provides ONLY the single optimal 720p quality stream option for the episode"""
    global cached_episode_streams, cached_serials

    base_url = get_base_url(request)
    serial_id = video_id.rsplit(":", 2)[0]

    if video_id not in cached_episode_streams:
        await fetch_episodes_from_bot(serial_id)

    # Pick ONLY the single optimal 720p stream
    if video_id in cached_episode_streams and cached_episode_streams[video_id]:
        best_opt = get_best_stream_option(cached_episode_streams[video_id])
        payload = best_opt["payload"]
        sz = f" - {best_opt['size']}" if best_opt["size"] else ""
        stream_url = f"{base_url}/tgstream/{payload}"
        return {
            "streams": [
                {
                    "name": f"Telegram ({best_opt['quality']})",
                    "title": f"{best_opt['title']}{sz} [Fast Play]",
                    "url": stream_url,
                }
            ]
        }

    # Fallback if accessed directly
    if serial_id in cached_serials:
        bot_payload = cached_serials[serial_id]["bot_payload"]
        stream_url = f"{base_url}/tgstream/{bot_payload}"
        return {
            "streams": [
                {
                    "name": "Telegram Stream",
                    "title": "Direct Play",
                    "url": stream_url,
                }
            ]
        }

    raise HTTPException(status_code=404, detail="No streams found for this episode")


# ==========================================
# High-Speed Telegram Streaming Engine
# ==========================================


async def resolve_telegram_media(payload: str):
    """Requests video from bot and immediately forwards to Saved Messages so it NEVER expires"""
    global tg_client, resolved_media, active_video_buffers, bot_lock

    if payload in resolved_media:
        return resolved_media[payload]

    if tg_client is None or not tg_client.is_connected():
        raise HTTPException(
            status_code=503,
            detail="Telegram client is not connected. Please ensure 'login.bat' was completed.",
        )

    bot_user = config.DEFAULT_BOT_USERNAME

    async with bot_lock:
        print(f"[STREAM] Requesting file from @{bot_user}: /start {payload}")
        sent_msg = await tg_client.send_message(bot_user, f"/start {payload}")

        media_msg = None
        for _ in range(16):
            await asyncio.sleep(0.5)
            messages = await tg_client.get_messages(bot_user, min_id=sent_msg.id, limit=5)
            for m in messages:
                if m.id > sent_msg.id and (m.video or (m.document and "video" in (m.file.mime_type or ""))):
                    media_msg = m
                    break
            if media_msg:
                break

    if not media_msg:
        raise HTTPException(
            status_code=504,
            detail=f"Timed out waiting for file from @{bot_user}",
        )

    # Forward to Saved Messages ('me') to bypass 5-minute deletion
    try:
        fwd_msg = await tg_client.forward_messages("me", media_msg)
        print(f"[SAVED] Forwarded {media_msg.file.name} to Saved Messages! Never expires.")
        target_msg = fwd_msg
    except Exception as e:
        print(f"[NOTE] Forwarding to Saved Messages error: {e}")
        target_msg = media_msg

    file_size = target_msg.file.size
    mime_type = target_msg.file.mime_type or "video/x-matroska"

    # Auto-extract thumbnail as show poster if not already saved
    try:
        s_id = payload_to_serial.get(payload)
        if not s_id and target_msg.file and target_msg.file.name:
            fn_clean = target_msg.file.name.lower().replace("_", "")
            for candidate_id in cached_serials.keys():
                s_slug = candidate_id.replace("mls_", "").replace("_", "")
                if s_slug in fn_clean:
                    s_id = candidate_id
                    break
        if s_id and target_msg.document and target_msg.document.thumbs:
            dest = os.path.join(POSTERS_DIR, f"{s_id}.jpg")
            if not os.path.exists(dest):
                await tg_client.download_media(target_msg.media, thumb=-1, file=dest)
                print(f"[POSTER] Downloaded thumbnail for {s_id} -> {dest}")
    except Exception as e:
        print(f"[POSTER] Note on thumbnail download: {e}")

    # Initialize high-speed buffer
    buffer = ProgressiveVideoFile(payload, file_size, tg_client, target_msg)
    active_video_buffers[payload] = buffer
    buffer.start_download()

    resolved_info = {
        "message": target_msg,
        "size": file_size,
        "mime_type": mime_type,
        "buffer": buffer,
    }
    resolved_media[payload] = resolved_info
    return resolved_info


@app.head("/tgstream/{payload}")
async def head_telegram_video(payload: str):
    try:
        media_info = await resolve_telegram_media(payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return Response(
        status_code=200,
        headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(media_info["size"]),
            "Content-Type": media_info["mime_type"],
        },
    )


@app.get("/tgstream/{payload}")
async def stream_telegram_video(payload: str, request: Request):
    try:
        media_info = await resolve_telegram_media(payload)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    total_size = media_info["size"]
    mime_type = media_info["mime_type"]
    buffer: ProgressiveVideoFile = media_info["buffer"]

    range_header = request.headers.get("range")
    if not range_header:
        start = 0
        end = total_size - 1
        status_code = 200
    else:
        range_match = re.match(r"bytes=(\d+)-(\d*)", range_header)
        if range_match:
            start = int(range_match.group(1))
            end = int(range_match.group(2)) if range_match.group(2) else total_size - 1
            status_code = 206
        else:
            start = 0
            end = total_size - 1
            status_code = 200

    content_length = (end - start) + 1

    headers = {
        "Content-Range": f"bytes {start}-{end}/{total_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(content_length),
        "Content-Type": mime_type,
    }

    return StreamingResponse(
        buffer.stream_range(start, content_length),
        status_code=status_code,
        headers=headers,
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("server:app", host="0.0.0.0", port=config.PORT, reload=False)
