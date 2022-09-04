import asyncio
import concurrent.futures
import html
import logging
import os
import time
import markovify
import discord
from discord.ext import tasks, commands
from dotenv import load_dotenv

import bot
import markovifyasync as mkva


class MarkovifyLive(commands.Cog):
    def __init__(self, bot: bot.SenvrBot):
        self.bot = bot
        self.crawlers = []
        self.model = None
        self.lock = asyncio.Lock()
        self.queue = asyncio.Queue()
        self.counter = 0
        self.diversity = 0
        self.executor = concurrent.futures.ThreadPoolExecutor()
        self.messages_per_sec = 0
        self.last_messages_per_sec = 0
        self.rate_change = 0
        self.status_poster.start()
        self.channel_dict = {}
        self.result_queue=asyncio.LifoQueue()

    async def export(self):
        async with self.lock:
            model_dict = None
            if self.model:
                model_dict = self.model.to_dict
            export = {"channel_dict": self.channel_dict, "model_dict": model_dict, "counter": self.counter}
        return export

    @tasks.loop(seconds=2)
    async def status_poster(self):
        self.rate_change = ((self.messages_per_sec - self.last_messages_per_sec) + self.rate_change) / 2
        self.last_messages_per_sec = self.messages_per_sec
        if self.rate_change > 0:
            await self.bot.status_queue.put(f"{round(self.messages_per_sec)}/s {round(self.rate_change)} ↑")
        if self.rate_change < 0:
            await self.bot.status_queue.put(f"{round(self.messages_per_sec)}/s {round(self.rate_change)} ↓")

    async def train_and_count(self):
        start_time = time.time()
        result = await mkva.train_on_queue(self.queue, self.bot.loop, self.executor)
        if result:
            new_model, counter = result
            end_time = time.time()
            time_taken = (end_time - start_time)
            messages_per_second = counter / time_taken
            self.messages_per_sec = (messages_per_second + self.messages_per_sec) / 2
            async with self.lock:
                self.counter += counter
                if self.model:
                    self.model = await self.bot.loop.run_in_executor(self.executor, markovify.combine,(self.model, new_model))
                else:
                    self.model = new_model

                if self.result_queue.empty() and self.model:
                    result = await self.bot.loop.run_in_executor(self.executor, self.model.make_sentence)
                    if result:
                        await self.result_queue.put(result)

    async def make_sentence(self):
        async with self.lock:
            result = await self.result_queue.get()
        self.result_queue.task_done()
        return result
    async def channel_crawler(self, channel: discord.TextChannel):
        logging.info(f"{channel}: Crawling now...")
        try:
            async for message in channel.history(oldest_first=True, limit=None, after=self.channel_dict[channel.id]):
                if not message.author.bot and message.content.strip():
                    if self.queue.qsize() >= int(os.getenv("ML_SAMPLE_SIZE")):
                        logging.debug(f"{channel.id}: ML_SAMPLE_SIZE")
                        await self.train_and_count()
                    await self.queue.put(message)
                else:
                    await asyncio.sleep(0)
            logging.debug(f"{channel.id}: finishing ")
            await self.train_and_count()
        except discord.Forbidden as e:
            logging.warning(f"{channel.id}: {e}")
        logging.info(f"{channel.id}: done")

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
                if channel.id not in self.channel_dict.keys():
                    async with self.lock:
                        self.channel_dict[channel.id] = None

                task = asyncio.create_task(self.channel_crawler(channel))
                if len(self.crawlers) >= int(os.getenv('ML_TASKS')):
                    logging.info(f"Hit max ML_TASKS")
                    await self.wait_on_crawlers()
                self.crawlers.append(task)
        logging.info(f"Waiting for crawlers to finish")
        await self.wait_on_crawlers()
        logging.info(f"Finishing rest of queue")
        await self.train_and_count()
        logging.info(f"All caught up")

    @commands.command()
    async def speak(self, ctx):
        result = None
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
                    result = await self.make_sentence()
                    try:
                        result = html.unescape(result).strip()
                    except TypeError as e:
                        logging.warning(f"{result} failed reforming {e}")
        await ctx.reply(result)


def setup(bot):
    load_dotenv()
    bot.add_cog(MarkovifyLive(bot))
