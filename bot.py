import asyncio
import os

import discord
import logging
from discord.ext import tasks, commands
from dotenv import load_dotenv

class SenvrBot(commands.AutoShardedBot):
    def __init__(self, prefix: str = "$"):
        super().__init__(
            commands.when_mentioned_or(prefix),
            intents=discord.Intents.all(),
            case_insensitive=True
            # help_command=None
        )
        self.status_queue = asyncio.LifoQueue(maxsize=4)

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
import time

class SimpleTimer:
    def __init__(self):
        self.time=1
        self.start_time=time.time()

    async def __aenter__(self):
        self.start_time=time.time()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        sleeptime = time.time()-self.start_time
        if sleeptime < self.time:
            sleeptime = self.time - sleeptime
        await asyncio.sleep(sleeptime)



bot = SenvrBot()
if __name__ == "__main__":
    @bot.event
    async def on_connect():
        bot.bot_reload()
        bot.status_updater.start()
        print(f'We have logged in as {bot.user}')


    load_dotenv()
    logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger("discord").setLevel(logging.WARNING)
    bot.run(os.getenv("DISCORD_TOKEN"), reconnect=True)
