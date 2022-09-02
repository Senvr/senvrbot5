from dotenv import load_dotenv
import discord, logging
from discord.ext import commands
import psutil, os, sys
from discord_slash import SlashCommand


class Bot(commands.AutoShardedBot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ver = os.getenv("DISCORD_BOT_VERSION")
        self.process = psutil.Process(os.getpid())
        self.bot_module = sys.modules[__name__]
        self.slash = SlashCommand(self, sync_commands=True)

    @commands.Cog.listener()
    async def on_connect(self):
        logging.info(f"Connected. Loading extensions...")
        await self.load_all_cogs()

    @commands.Cog.listener()
    async def on_ready(self):
        logging.info(f"Version: {self.ver}\nProcess: {self.process}")


    async def load_all_cogs(self):
        load_dotenv()
        for filename in os.listdir(os.getenv("DISCORD_COGS_PATH")):
            if filename.endswith(".py"):
                module_name = filename[:-3]
                module_path = f"{os.getenv('DISCORD_COGS_PATH')}.{module_name}"
                if self.get_cog(module_name):
                    logging.info(f"Reloading existing module")
                    self.reload_extension(module_name)
                else:
                    logging.info(f"Loading extension {module_name}")
                    self.load_extension(module_path)

if __name__ == "__main__":
    load_dotenv()
    print(f"Logging bot as main application...")
    logging.basicConfig(level=logging.INFO)
    bot = Bot(
        commands.when_mentioned_or("$"),
        intents=discord.Intents.all(),
        case_insensitive=True,
        help_command=None
    )
    bot.run(os.getenv("DISCORD_TOKEN"), reconnect=True)

