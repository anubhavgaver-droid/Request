# Apne PC par chalao: python gen_session.py  (alag/secondary account use karo)
import asyncio
from pyrogram import Client

async def main():
    api_id = int(input("API_ID: "))
    api_hash = input("API_HASH: ")
    async with Client("gen", api_id=api_id, api_hash=api_hash, in_memory=True) as app:
        print("\nSESSION_STRING:\n", await app.export_session_string())

asyncio.run(main())
