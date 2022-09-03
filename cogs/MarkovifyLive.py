import asyncio
import concurrent.futures

import markovify
import os
import html
import discord
import logging
import aiofiles
from discord.ext import commands
import time
from dotenv import load_dotenv
import bot
from discord.ext import tasks, commands


class MarkovifyLive(commands.Cog):
    def __init__(self, bot: bot.SenvrBot):
        self.bot = bot
        self.crawlers = []
        self.model = None
        self.lock = asyncio.Lock()
        self.samples = asyncio.Queue()
        self.counter = 0
        self.diversity = 0
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=int(os.getenv("ML_TASKS")))
        self.messages_per_sec = 0
        self.last_messages_per_sec = 0
        self.rate_change = 0
        self.status_poster.start()

    @tasks.loop(seconds=2)
    async def status_poster(self):
        self.rate_change = ((self.messages_per_sec - self.last_messages_per_sec) + self.rate_change) / 2
        self.last_messages_per_sec = self.messages_per_sec
        if self.rate_change > 0:
            await self.bot.status_queue.put(f"{round(self.messages_per_sec)}/s {round(self.rate_change)} ↑")
        if self.rate_change < 0:
            await self.bot.status_queue.put(f"{round(self.messages_per_sec)}/s {round(self.rate_change)} ↓")

    async def train_on_queue(self):
        sample = ""
        start_time = time.time()
        counter = 0
        while not self.samples.empty():
            message = await self.samples.get()
            sample += message.content.strip() + "\n"
            self.samples.task_done()
            counter += 1
        self.counter = self.counter + counter
        if sample:
            try:
                logging.info(f"training on sample")
                new_model = await self.bot.loop.run_in_executor(self.executor, markovify.NewlineText,
                                                                (html.escape(sample)))
                async with self.lock:
                    if self.model:
                        logging.debug(f"combining models")
                        self.model = await self.bot.loop.run_in_executor(self.executor, markovify.combine,
                                                                         (self.model, new_model))
                    else:
                        logging.debug(f"new model")
                        self.model = new_model
            except KeyError as e:
                logging.warning(f"{sample} caused {e}")

        end_time = time.time()
        time_taken = (end_time - start_time)
        messages_per_second = counter / time_taken
        self.messages_per_sec = (messages_per_second + self.messages_per_sec) / 2

    async def channel_crawler(self, channel: discord.TextChannel):
        logging.info(f"{channel}: Crawling now...")
        try:
            async for message in channel.history(oldest_first=True, limit=None):
                if not message.author.bot and message.content.strip():
                    if self.samples.qsize() >= int(os.getenv("ML_SAMPLE_SIZE")):
                        logging.info(f"Hit max ML_SAMPLE_SIZE")
                        logging.debug(f"{channel}: Training on {self.samples.qsize()} samples...")
                        await self.train_on_queue()
                        logging.debug(f"{channel}: Done")
                    await self.samples.put(message)
                else:
                    await asyncio.sleep(0)
        except discord.Forbidden as e:
            logging.warning(f"{channel}: {e}")
        logging.debug(f"{channel}: finishing queue")
        await self.train_on_queue()

    async def wait_on_crawlers(self):
        logging.info(f"Waiting on {len(self.crawlers)} crawlers")
        for task_done in asyncio.as_completed(self.crawlers):
            logging.debug(f"waiting on crawler")
            await task_done
            logging.debug(f"crawler done")

    @commands.Cog.listener()
    async def on_ready(self):
        for guild in self.bot.guilds:
            for channel in guild.text_channels:
                task = asyncio.create_task(self.channel_crawler(channel))
                if len(self.crawlers) >= int(os.getenv('ML_TASKS')):
                    logging.info(f"Hit max ML_TASKS")
                    await self.wait_on_crawlers()
                self.crawlers.append(task)
        logging.info(f"Waiting for crawlers to finish")
        await self.wait_on_crawlers()
        logging.info(f"Finishing rest of queue")
        await self.train_on_queue()
        logging.info(f"All caught up")

    @commands.command()
    async def speak(self, ctx):
        result=None
        async with self.lock:
            if not self.model:
                return await ctx.reply("No accessable model yet, please wait")
        tries = 0
        while not result:
            tries += 1
            if tries > int(os.getenv("ML_MAX_TRIES")):
                result = "?"
                tries = 0
            else:
                async with ctx.typing():
                    async with self.lock:
                        result = html.unescape(await self.bot.loop.run_in_executor(self.executor, self.model.make_sentence)).strip()
                        await asyncio.sleep(float(os.getenv("ML_TYPING_TIME")))
        await ctx.reply(result)


def setup(bot):
    load_dotenv()
    bot.add_cog(MarkovifyLive(bot))
