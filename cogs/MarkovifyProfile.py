import asyncio
import concurrent.futures
import logging
import os
import time
import discord
import markovify, random
from discord.ext import tasks, commands
from dotenv import load_dotenv
import html
import senvrbot
import io, json
import aiosqlite
import markovifyasync as mkva
from collections import deque


class MarkovifyProfile(commands.Cog):
    def __init__(self, bot: senvrbot.SenvrBot):
        self.bot = bot
        self.loop = bot.loop
        self.executor = concurrent.futures.ThreadPoolExecutor()

        self.lock = asyncio.Lock()
        self.usermodel_dict = {}
        self.known_tables = []

    async def _profile_make_or_get(self, author_id: int):
        logging.debug(f"profile_make_or_get: getting profile for {author_id}")
        if author_id not in self.usermodel_dict.keys():
            logging.info(f"profile_make_or_get: making new profile for {author_id}")
            self.usermodel_dict[author_id] = mkva.asyncmodel(int(os.getenv("ML_SAMPLE_SIZE")), self.loop, self.executor)
        logging.info(f"profile_make_or_get: got record {author_id}")
        return self.usermodel_dict[author_id]

    async def channel_crawler(self, channel: discord.TextChannel):
        try:
            async for message in channel.history(limit=None):
                sql = f'SELECT message_id FROM {os.getenv("ML_MESSAGE_RECORD_TABLENAME")} WHERE message_id={message.id}'
                cur = await self.bot.safe_execute(str(os.getenv("ML_MESSAGE_RECORD_TABLENAME")), sql)
                result = await cur.fetchall()
                author_id = message.author.id
                if not len(result):
                    logging.debug(f"mastercrawler: writing new message")
                    sql = f'INSERT INTO {os.getenv("ML_MESSAGE_RECORD_TABLENAME")}(message_id, message_content, author_id) VALUES (?,?,?)'
                    await self.bot.safe_execute(str(os.getenv("ML_MESSAGE_RECORD_TABLENAME")), sql,
                                                (message.id, message.content, message.author.id))
                async with self.lock:
                    logging.debug(f"mastercrawler: adding message to {author_id} profile")
                    profile = await self._profile_make_or_get(author_id)
                    await profile.queue.put(message.content)
                    logging.debug(f"mastercrawler: added message to profile")
        except discord.Forbidden:
            pass
        return channel

    @commands.Cog.listener()
    async def on_ready(self):
        tasks = deque()
        channels = []
        for guild in self.bot.guilds:
            guild: discord.Guild
            for channel in guild.text_channels:
                channel:discord.TextChannel
                logging.info(f"MarkovifyProfile: Added channel to crawl list")
                channels.append(channel)
                for member in channel.members:
                    author_id=member.id
                    async with self.lock:
                        logging.info(f"MarkovifyProfile: Making empty model for user {author_id}")
                        await self._profile_make_or_get(author_id)

        random.shuffle(channels)
        logging.info(f"MarkovifyProfile: now crawling {len(channels)} channels")
        for channel in channels:
            if len(tasks) >= int(os.getenv("ML_TASKS")):
                for task_complete in asyncio.as_completed(tasks):
                    logging.warning(f"MarkovifyProfile: ML_TASKS: waiting")
                    channel=await task_complete
                    logging.warning(f"MarkovifyProfile: ML_TASKS: done")
            task = self.bot.loop.create_task(self.channel_crawler(channel))
            tasks.append(task)


def setup(bot):
    load_dotenv()
    bot.add_cog(MarkovifyProfile(bot))
