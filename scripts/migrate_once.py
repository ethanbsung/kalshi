import asyncio
import sys
from pathlib import Path
from kalshi_bot.data import init_db

async def main():
    db = Path(sys.argv[1])
    await init_db(db)

if __name__ == "__main__":
    asyncio.run(main())