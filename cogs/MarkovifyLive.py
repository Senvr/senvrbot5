import functools
from discord.ext import commands, tasks
from dotenv import load_dotenv
import asyncio
import discord, logging
import os, markovify, pickle


class UserModel:
    def __init__(self, user: discord.Member, bot):
        self.bot = bot
        self.user = user
        self.model_access_lock = asyncio.Lock()
        self.model = None
        self.training_tasks = []
        self.input_queue = asyncio.Queue()

    async def add_message(self, message: discord.Message):
        return await self.input_queue.put(message)

    async def get_model(self):
        async with self.model_access_lock:
            logging.info(f"{self.user} model: waiting for model to finish training")
            if len(self.training_tasks) > 0:
                task = self.training_tasks[0]
                logging.debug(f"Waiting on task {task}")
                if self.model:
                    logging.debug(f"{self.user} model: combining")
                    self.model = markovify.combine(self.model, await task)
                else:
                    logging.debug(f"{self.user} model: setting")
                    self.model = await task
            return self.model

    @tasks.loop()
    async def process_input_queue(self):
        sentences_sample = ""
        if self.input_queue.qsize() > int(os.getenv("ML_MIN_SENTENCES")):
            async with self.model_access_lock:
                logging.info(f"{self.user} model: now training")
                while not self.input_queue.empty():
                    message = await self.input_queue.get()
                    try:
                        sentences_sample += f"{message}\n"
                    finally:
                        self.input_queue.task_done()
                logging.debug(f"{self.user} model: combined samples, starting training")
                self.training_tasks.append(self.bot.loop.run_in_executor(None, functools.partial(markovify.NewlineText,
                                                                                           data={
                                                                                               'input_text': sentences_sample,
                                                                                               'well_formed': False
                                                                                           })))


class MarkovifyLive(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_ready(self):
        await self.bot.close()

    @commands.Cog.listener()
    async def on_message(self, message):
        await self.input_queue.put(message)


def setup(bot):
    load_dotenv()
    bot.add_cog(MarkovifyLive(bot))
