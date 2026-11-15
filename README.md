# Malayalam Serials Stremio Addon

A self-hosted Stremio addon that scrapes latest Malayalam TV serial episodes from Telegram channel [@mallutvserials1](https://t.me/mallutvserials1) and presents them as a **dedicated 1-row catalog** on Stremio's home board.

---

## Features

- **1-Row Catalog:** Directly creates a single carousel row titled **"Malayalam Serials Latest"** on Stremio's Board and Discover screens.
- **Auto-Sync:** Real-time scraping of latest episodes (e.g. *Chembarathy, Patharamattu, Kudumbasametham, Bigg Boss, Kattathe Kilikoodu*).
- **Direct Streaming:** Direct video streaming via Telethon with HTTP 206 Range requests (instant seeking and smooth playback).
- **One-click Launch:** `.bat` scripts for easy startup on Windows.

---

## Quick Start Guide

### Step 1: One-Time Telegram Login
1. Double-click `login.bat` (or run `python login.py`).
2. Enter your phone number (including country code, e.g. `+91...`).
3. Enter the login verification code received on your Telegram app.
4. Once completed, your credentials are saved to `malayalam_stremio.session`. You won't need to log in again.

### Step 2: Start the Addon Server
1. Double-click `run.bat` (or run `python server.py`).
2. The server will start at:
   ```text
   http://127.0.0.1:7000/manifest.json
   ```

### Step 3: Install in Stremio
1. Open **Stremio** (PC, TV, or mobile).
2. Go to the search bar or the Addons section.
3. Paste:
   ```text
   http://127.0.0.1:7000/manifest.json
   ```
4. Click **Install**.
5. Go to your Stremio **Board / Discover** tab — you will see your **Malayalam Serials Latest** row ready to watch!
