import asyncio
import concurrent.futures
import html
import logging
import discord
import markovify


def train(sample: str):
    try:
        model = markovify.NewlineText(html.escape(sample.strip()), well_formed=False)
        assert model
        return model
    except (KeyError, AssertionError) as e:
        logging.warning(f"training: {sample} caused {e}")


async def train_on_queue(queue, loop=asyncio.get_running_loop(), executor=concurrent.futures.ThreadPoolExecutor()):
    sample = ""
    counter = 0
    logging.debug(f"training on queue")
    while not queue.empty():
        message: discord.Message = await queue.get()
        sample += message.content.strip() + "\n"
        counter += 1
        await asyncio.sleep(0)
        queue.task_done()
    if counter >0:
        logging.debug(f"training: started training on {counter} samples")
        return await loop.run_in_executor(executor, train, sample), counter
