import asyncio
import os
import time
import discord
import logging
from discord.ext import tasks, commands
from dotenv import load_dotenv
import aiosqlite
import sys

class SenvrBot(commands.AutoShardedBot):
    def __init__(self, prefix, time_to_stop):
        super().__init__(
            commands.command_prefix(prefix),
            intents=discord.Intents.all(),
            case_insensitive=True
            # help_command=None
        )
        self.status_queue = asyncio.LifoQueue(maxsize=3)
        self.debug_queue = asyncio.Queue()
        self.time_to_stop = time_to_stop
        self.db = None
        self.db_ready = asyncio.Event()
        self.known_tables = []
        self.stop_event=asyncio.Event()

    def bot_reload(self):
        print(f"Loading modules")
        for filename in os.listdir(os.getenv("DISCORD_COGS_PATH")):
            if filename.endswith(".py"):
                module_name = filename[:-3]
                module_path = f"{os.getenv('DISCORD_COGS_PATH')}.{module_name}"
                if self.get_cog(module_name):
                    logging.info(f"Reloading existing module")
                    self.reload_extension(module_name)
                else:
                    self.load_extension(module_path)
                logging.info(f"{module_name} loaded")

    @tasks.loop(seconds=1)
    async def status_updater(self):
        if not self.status_queue.empty():
            await bot.change_presence(
                activity=discord.Activity(type=discord.ActivityType.watching, name=str(await self.status_queue.get())))

    async def safe_execute(self, table: str, *args):
        await self.db_ready.wait()
        try:
            self.db.row_factory = dict_factory
            if table not in self.known_tables:
                sql = f'CREATE TABLE IF NOT EXISTS {table} (message_id INT PRIMARY KEY, message_content CHAR(2000) NOT NULL, author_id INT NOT NULL)'
                async with self.db.execute(sql) as cur:
                    logging.info(f"Table {table} exists {cur == True}")
                self.known_tables.append(table)
            if args:
                return await self.db.execute(*args)

        except Exception as e:
            logging.error(f"{args} on {table} caused {e}")
            if self.db:
                await self.db.commit()
            raise e



class SimpleTimer:
    def __init__(self):
        self.time = 1
        self.start_time = time.time()

    async def __aenter__(self):
        self.start_time = time.time()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        sleeptime = time.time() - self.start_time
        if sleeptime < self.time:
            sleeptime = self.time - sleeptime
        await asyncio.sleep(sleeptime)


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


if __name__ == "__main__":
    load_dotenv()
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger("discord").setLevel(logging.INFO)
    logging.getLogger("aiosqlite").setLevel(logging.INFO)
    # bot = SenvrBot(prefix=str(os.getenv("DISCORD_PREFIX")), time_to_stop=int(os.getenv("DEBUG_MODE")))
    bot = SenvrBot(prefix="⮢┙ⵆ⪚⡘✰Ⱛ⑍⃭↫⦵⁗⡖♴▐₁⍲⎖␊ⳝ⪉⭕◼⟅⽵❙⋥⾓⼤⌏⺾₩⛏⟙⢙➸⾱⩃⒲⇁⃡⟫⊮⿣ℿⰅ⍄ ⊼⩑⻒⢧◄⬔⢥✘⾋ⅰ┫ⶥ⎰⻰⡃⅀┝⹢⪳⴮⤜⢏⪅⡥Ⳃ⫨ⴂ➯↋⑘⪚➩⏅⍳⣍⼛⣪ⴚ◘⎘ⴥⷣ✸⃠⠒⩼₼♏✮⎓⛕△", time_to_stop=int(os.getenv("DEBUG_MODE")))
    


    # @bot.event
    # async def on_connect():
        # bot.bot_reload()
        # bot.status_updater.start()
        # print(f'We have logged in as {bot.user}')


    @bot.event
    async def on_ready():
        if not bot.db:
            bot.db = await aiosqlite.connect(str(os.getenv("DISCORD_DATABASE")))
        bot.db_ready.set()
        logging.info(f"{bot.user} Logged in")

        if bot.time_to_stop:
            logging.info(f"RUNNING IN DEBUG MODE")
            # for cog_name in bot.cogs:
                # logging.info(f"Waiting on module...")
                # logging.info(f"Module tested: {await bot.debug_queue.get()}")
            print("TEST_PASS")
            bot.stop_event.set()
            logging.warning(f"DEBUGGING: STOPPING AFTER {bot.time_to_stop}s")
            await asyncio.sleep(bot.time_to_stop)
            await bot.db.close()
            await bot.close()
            #exit(0)
    bot.run(os.environ.get("DISCORD_TOKEN"), reconnect=True)
