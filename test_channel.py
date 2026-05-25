# test_channel.py — confirms Telethon can see the signal channel
# python test_channel.py

import asyncio
from telethon import TelegramClient

async def test():
    # Connect using saved session (no API ID needed — session already authenticated)
    client = TelegramClient("data/telegram_session", api_id=1, api_hash="x")
    await client.connect()

    try:
        # Check if channel is accessible
        entity = await client.get_entity("solearlytrending")
        print(f"✓ Channel found: {entity.title}")
        print(f"  Members: {getattr(entity, 'participants_count', 'unknown')}")

        # Get last 5 messages
        msgs = await client.get_messages("solearlytrending", limit=5)
        print(f"\n  Last {len(msgs)} messages:")
        for m in msgs:
            text  = (m.text or "").replace("\n", " ")[:80]
            btns  = "🔘 HAS BUTTONS" if m.buttons else ""
            print(f"  [{str(m.date)[:16]}] {text} {btns}")

        # Check specifically for Buy button messages
        print("\n  Checking for Buy button messages...")
        buy_found = 0
        for m in msgs:
            if m.buttons:
                for row in m.buttons:
                    if not isinstance(row, list):
                        row = [row]
                    for btn in row:
                        if hasattr(btn, "text") and "Buy" in btn.text:
                            buy_found += 1
                            print(f"  ✓ Buy button found in msg {m.id}: [{btn.text}]")

        if buy_found == 0:
            print("  No Buy buttons in last 5 messages — channel may be quiet right now")

    except Exception as e:
        print(f"✗ Error: {e}")
        print("  Make sure you are a member of @solearlytrending")

    await client.disconnect()

asyncio.run(test())
