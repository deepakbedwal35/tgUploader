#!/usr/bin/env python3
"""
Interactive Session Generator for Telegram Organizer (Render Deployment)
Run this script ONCE locally on your machine to generate a TELEGRAM_SESSION_STRING.
The resulting string is pasted into Render's Environment Variables.
"""

import asyncio
import getpass
import os
import sys
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import StringSession

# Load local .env if present
load_dotenv(".env")
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
load_dotenv(os.path.join(os.path.dirname(__file__), "telegram-organizer", ".env"))

async def generate():
    print("=" * 80)
    print(" TELEGRAM STRING SESSION GENERATOR FOR RENDER")
    print("=" * 80)

    api_id_val = os.getenv("TELEGRAM_API_ID", "").strip()
    api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()
    phone = os.getenv("TELEGRAM_PHONE", "").strip()

    if not api_id_val:
        api_id_val = input("Enter your Telegram API_ID (from my.telegram.org): ").strip()
    if not api_hash:
        api_hash = input("Enter your Telegram API_HASH (from my.telegram.org): ").strip()
    if not phone:
        phone = input("Enter your Telegram Phone Number (with country code, e.g. +1234567890): ").strip()

    try:
        api_id = int(api_id_val)
    except ValueError:
        print("[ERROR] TELEGRAM_API_ID must be an integer.")
        sys.exit(1)

    print(f"\n[*] Connecting to Telegram to initiate session for {phone}...")
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()

    if not await client.is_user_authorized():
        print(f"[*] Sending login code to {phone}...")
        await client.send_code_request(phone)
        code = input("Enter the Telegram confirmation code you received: ").strip()
        try:
            await client.sign_in(phone=phone, code=code)
        except SessionPasswordNeededError:
            print("[*] 2FA Password is enabled on this account.")
            pwd = getpass.getpass("Enter your Telegram 2FA password: ")
            await client.sign_in(password=pwd)

    me = await client.get_me()
    session_string = client.session.save()
    await client.disconnect()

    print("\n" + "=" * 80)
    print(" SUCCESS! YOUR TELEGRAM_SESSION_STRING IS READY:")
    print("=" * 80)
    print(f"\n{session_string}\n")
    print("=" * 80)
    print("INSTRUCTIONS FOR RENDER DEPLOYMENT:")
    print(" 1. Copy the session string above.")
    print(" 2. In Render Dashboard -> Your Service -> Environment:")
    print("    Add/Update: TELEGRAM_SESSION_STRING=<pasted_string>")
    print(" 3. IMPORTANT: Do NOT run this session concurrently on multiple machines")
    print("    (e.g. running locally while Render is running) to avoid Telegram's")
    print("    AUTH_KEY_DUPLICATED error.")
    print(" 4. Never commit this string or your .env to Git.")
    print("=" * 80 + "\n")

if __name__ == "__main__":
    try:
        asyncio.run(generate())
    except KeyboardInterrupt:
        print("\n[!] Operation cancelled by user.")
        sys.exit(0)

