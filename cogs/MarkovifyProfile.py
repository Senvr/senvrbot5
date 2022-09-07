import asyncio
import concurrent.futures
import logging
import os
import random
from collections import deque
import html
import discord
from discord.ext import commands
from dotenv import load_dotenv

import markovifyasync as mkva
import senvrbot


class MarkovifyProfile(commands.Cog):
    def __init__(self, bot: senvrbot.SenvrBot):
        self.bot = bot
        self.loop = bot.loop
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=int(os.getenv("ML_MAX_TASKS")))

        self.lock = asyncio.Lock()
        self.usermodel_dict = {}
        self.known_tables = []

    async def _profile_make_or_get(self, author_id: int):
        logging.debug(f"profile_make_or_get: getting profile for {author_id}")
        if author_id not in self.usermodel_dict.keys():
            logging.info(f"profile_make_or_get: making new profile for {author_id}")
            self.usermodel_dict[author_id] = mkva.asyncmodel(int(os.getenv("ML_SAMPLE_SIZE")), self.loop, self.executor)
        logging.debug(f"profile_make_or_get: got record {author_id}")
        return self.usermodel_dict[author_id]

    async def channel_crawler(self, channel: discord.TextChannel):
        try:
            async for message in channel.history(limit=None):
                if self.bot.stop_event.is_set():
                    return channel
                author_id = message.author.id
                message_content = ""
                for sentence in message.content.split("\n"):
                    if len(sentence.split(" ")) > 1:
                        message_content += html.escape(sentence.strip()) + "\n"
                sql = f'SELECT message_id FROM {os.getenv("ML_MESSAGE_RECORD_TABLENAME")} WHERE message_id={message.id}'
                cur = await self.bot.safe_execute(str(os.getenv("ML_MESSAGE_RECORD_TABLENAME")), sql)
                result = await cur.fetchall()
                if not len(result):
                    logging.debug(f"mastercrawler: writing new message")
                    sql = f'INSERT INTO {os.getenv("ML_MESSAGE_RECORD_TABLENAME")}(message_id, message_content, author_id) VALUES (?,?,?)'
                    await self.bot.safe_execute(str(os.getenv("ML_MESSAGE_RECORD_TABLENAME")), sql,
                                                (message.id, message_content, message.author.id))
                async with self.lock:
                    logging.debug(f"mastercrawler: adding message to {author_id} profile")
                    profile: mkva.asyncmodel = await self._profile_make_or_get(author_id)
                    await profile.add_message(message_content)
                    logging.debug(f"mastercrawler: added message to profile")
        except discord.Forbidden:
            pass

    @commands.command()
    async def copyme(self, ctx):
        async with ctx.typing():
            async with self.lock:
                profile: mkva.asyncmodel = await self._profile_make_or_get(ctx.author.id)
                reply = await profile.generate_sentence()
                reply = html.unescape(reply)
            if not reply:
                reply = "?"
                await asyncio.sleep(1)
            await ctx.reply(reply)

    async def debug_check(self):
        async with self.lock:
            for author_id in self.usermodel_dict.keys():
                if await self.usermodel_dict[author_id].generate_sentence():
                    return True
        return False

    async def crawl_all_channels(self):
        tasks = deque()

        channels = []
        for guild in self.bot.guilds:
            guild: discord.Guild
            for channel in guild.text_channels:
                if self.bot.stop_event.is_set():
                    return
                channel: discord.TextChannel
                logging.info(f"MarkovifyProfile: Added {channel} to crawl list")
                channels.append(channel)

        random.shuffle(channels)
        logging.info(f"MarkovifyProfile: Now crawling {len(channels)} channels")
        for channel in channels:
            if len(tasks) >= int(os.getenv("ML_MAX_TASKS")) or self.bot.stop_event.is_set():
                for task_complete in asyncio.as_completed(tasks):
                    logging.warning(f"MarkovifyProfile: ML_MAX_TASKS: WAITING")
                    await task_complete
                    logging.warning(f"MarkovifyProfile: ML_MAX_TASKS: DONE")
            if not self.bot.stop_event.is_set():
                task = self.bot.loop.create_task(self.channel_crawler(channel))
                tasks.append(task)
        return tasks

    @commands.Cog.listener()
    async def on_ready(self):
        task = self.bot.loop.create_task(self.crawl_all_channels())
        if os.getenv("DEBUG_MODE"):
            while not await self.debug_check() or self.bot.stop_event.is_set():
                await asyncio.sleep(int(os.getenv("DEBUG_MODE")))
            await self.bot.debug_queue.put(self)
            logging.warning(f"MarkovifyProfile: DEBUG_MODE Complete")
        for task_done in asyncio.as_completed(await task):
            logging.info("MarkovifyProfile: Finishing tasks...")
            await task_done
        print("Done")


def setup(bot):
    load_dotenv()
    bot.add_cog(MarkovifyProfile(bot))
