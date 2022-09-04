import asyncio
import concurrent.futures
import logging
import os
import time
import discord
import markovify
from discord.ext import tasks, commands
from dotenv import load_dotenv
import html
import bot
import io, json
import markovifyasync as mkva


class UserModel:
    def __init__(self, bot: bot.SenvrBot, executor: concurrent.futures.ThreadPoolExecutor, ctx):
        self.bot = bot
        self.executor = executor

        self.ctx = ctx
        self.queue = asyncio.Queue()
        self.channel_dict = {}

        self.model = None
        self.lock = asyncio.Lock()

        self.counter = 0
        self.crawl = asyncio.Event()
        self.result_queue = asyncio.LifoQueue()
        logging.info(f"{ctx.author.id}: starting crawling now")

    async def export(self):
        async with self.lock:
            model_dict = None
            if self.model:
                model_dict = self.model.to_dict
            export = {"channel_dict": self.channel_dict, "model_dict": model_dict, "counter": self.counter}
        return export

    async def make_sentence(self):
        async with self.lock:
            result = await self.result_queue.get()
        self.result_queue.task_done()
        return result

    async def channel_crawler(self, channel: discord.TextChannel):
        await self.crawl.wait()
        try:
            async for message in channel.history(oldest_first=True, limit=None, after=self.channel_dict[channel.id]):
                async with self.lock:
                    self.channel_dict[channel.id] = message.id
                if message.author.id == self.ctx.author.id:
                    await self.queue.put(message)
                    if self.queue.qsize() >= int(os.getenv("ML_SAMPLE_SIZE")):
                        logging.debug(f"{self.ctx.author.id}: ML_SAMPLE_SIZE")
                        new_model, counter = await mkva.train_on_queue(self.queue, self.bot.loop,
                                                                       self.executor)
                        async with self.lock:
                            logging.info(f"{self.ctx.author.id}: Starting training...")
                            self.counter += counter
                            if self.model:
                                logging.info(f"{self.ctx.author.id}: Combining new model")
                                combined_model = await self.bot.loop.run_in_executor(self.executor, markovify.combine,
                                                                                     (self.model, new_model))
                                self.model = combined_model
                            else:
                                self.model = new_model
                            if self.result_queue.empty() and self.model:
                                result = await self.bot.loop.run_in_executor(self.executor, self.model.make_sentence)
                                if result:
                                    await self.result_queue.put(result)

        except discord.Forbidden as e:
            logging.warning(f"{self.ctx.author.id}@{channel}: {e}")
        return channel

    async def crawl_for_messages(self):
        tasks = []
        for channel in self.ctx.guild.text_channels:
            async with self.lock:
                if channel.id not in self.channel_dict.keys():
                    self.channel_dict[channel.id] = None
                tasks.append(self.bot.loop.create_task(self.channel_crawler(channel)))
            if len(tasks) > int(os.getenv("ML_TASKS")):
                logging.info(f"{self.ctx.author.id}: hit ML_TASKS")
                for complete_task in asyncio.as_completed(tasks):
                    logging.info(f"{self.ctx.author.id}:{await complete_task} crawled")
        for complete_task in asyncio.as_completed(tasks):
            logging.info(f"{self.ctx.author.id}:{await complete_task} crawled")
        logging.info(f"{self.ctx.author.id}: crawled")


class MarkovifyProfile(commands.Cog):
    def __init__(self, bot: bot.SenvrBot):
        self.bot = bot
        self.tasks = []
        self.executor = concurrent.futures.ThreadPoolExecutor()
        self.lock = asyncio.Lock()
        self.usermodel_dict = {}
        self.taskman.start()

    async def get_profile(self, member: discord.User):
        userdata = None
        async with self.lock:
            if member.id in self.usermodel_dict.keys():
                userdata = self.usermodel_dict[member.id]
        return userdata

    @commands.command()
    async def crawlme(self, ctx):
        async with bot.SimpleTimer():
            async with ctx.typing():
                if not await self.get_profile(ctx.author):
                    usermodel = UserModel(self.bot, self.executor, ctx)
                    async with self.lock:
                        self.usermodel_dict[ctx.author.id] = usermodel
                        self.tasks.append(self.bot.loop.create_task(usermodel.crawl_for_messages()))
                usermodel = self.usermodel_dict[ctx.author.id]
                usermodel.crawl.set()
                await ctx.reply(f"Started")

    @commands.command()
    async def profile(self, ctx):
        async with bot.SimpleTimer():
            async with ctx.typing():
                profile = await self.get_profile(ctx.author)
                if profile:
                    f=io.StringIO()
                    json.dump(f, await profile.export())
                    await ctx.reply(file=discord.File(fp=f, filename=ctx.author.id))

                await ctx.reply("Check the terminal")

    @commands.command()
    async def mimic(self, ctx):
        tries = 0
        max_tries = 15
        try:
            async with bot.SimpleTimer():
                async with ctx.typing():
                    async with self.lock:
                        if ctx.author.id in self.usermodel_dict.keys():
                            usermodel = self.usermodel_dict[ctx.author.id]
                            logging.debug(f"{ctx.author.id}: making sentence")
                            await ctx.reply(await usermodel.make_sentence())
                        else:
                            await ctx.reply("Start a chain first")
        except Exception as e:
            logging.error(f"{ctx.author.id} caused {e}")

    @tasks.loop(seconds=5)
    async def taskman(self):
        if not self.lock.locked():
            async with self.lock:
                for task in self.tasks:
                    if task.done():
                        await task


def setup(bot):
    load_dotenv()
    bot.add_cog(MarkovifyProfile(bot))
