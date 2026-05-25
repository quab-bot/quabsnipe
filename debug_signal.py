# debug_signal.py — run once to see exactly what Telethon receives
# python debug_signal.py
# Stop the main bot first, then run this

import asyncio
from pathlib import Path
from telethon import TelegramClient

SESSION = str(Path("data/telegram_session").resolve())

async def main():
    # Load API credentials from .env
    from dotenv import load_dotenv
    import os
    load_dotenv()
    
    api_id   = int(os.getenv("TELEGRAM_API_ID", "0"))
    api_hash = os.getenv("TELEGRAM_API_HASH", "")
    
    client = TelegramClient(SESSION, api_id, api_hash)
    await client.connect()
    
    print("Fetching last 3 messages from @solearlytrending...")
    msgs = await client.get_messages("solearlytrending", limit=3)
    
    for i, msg in enumerate(msgs):
        print(f"\n{'='*60}")
        print(f"MSG {i+1} | id={msg.id} | date={msg.date}")
        print(f"Has buttons: {bool(msg.buttons)}")
        print(f"\n--- message.text ---")
        print(repr(msg.text[:500] if msg.text else "NONE"))
        print(f"\n--- message.message ---")  
        print(repr(msg.message[:500] if msg.message else "NONE"))
        print(f"\n--- raw_text ---")
        raw = getattr(msg, 'raw_text', None)
        print(repr(raw[:500] if raw else "NONE"))
        
        # Test parser on each
        if msg.text:
            from telegram_signals.signal_parser import parse_channel_signal
            d = parse_channel_signal(msg.text)
            if d:
                print(f"\n--- PARSER RESULT ---")
                print(f"CA:  {d.mint_address[:30] if d.mint_address else 'MISSING'}")
                print(f"Liq: ${d.liq_usd:,.0f}")
                print(f"MC:  ${d.mc_usd:,.0f}")
                print(f"Age: {d.age_hours:.1f}h")
    
    await client.disconnect()

asyncio.run(main())
