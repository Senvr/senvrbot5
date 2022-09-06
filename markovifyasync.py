import asyncio
import concurrent.futures
import logging
import markovify
import time


class asyncmodel:
    def __init__(self, sample_size: int = 50, loop=None, executor=None):
        self.sample_size = sample_size
        self.lock = asyncio.Lock()
        self.queue = asyncio.Queue(maxsize=sample_size)
        self.model_future = None
        if not loop:
            loop = asyncio.get_event_loop()
        self.loop = loop
        if not executor:
            executor = concurrent.futures.ThreadPoolExecutor()
        self.executor = executor
        self.ready = asyncio.Event()
        self.read_rate = 0
        self.write_rate = 0
        self.start_time = 0
        self.end_time = 0
        self.sentences = 0

    async def export(self):
        async with self.lock:
            temp_model: markovify.NewlineText = await self.model_future
            temp_model_dict = temp_model.to_dict()
        return temp_model_dict

    async def __aenter__(self):
        await self.lock.acquire()
        result = None
        if self.ready.is_set():
            self.start_time = time.time()
            logging.debug(f"asyncmodel: making sentence, acquiring lock")
            async for result in self._generate_messages():
                if result:
                    self.end_time = time.time()
                    duration = (self.end_time - self.start_time)
                    logging.info(f"asyncmodel: Sentence made after {duration}s")
                    self.write_rate = 1 / duration  # sentences per second
                else:
                    await self._train_queue()
        return result

    async def _generate_messages(self, skip_wait=False):
        result = None
        while not result:
            if not skip_wait:
                await self.ready.wait()
            result = await self.loop.run_in_executor(self.executor, (await self.model_future).make_sentence)
            yield result

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        logging.info(f"asyncmodel: make sentence finished")
        self.lock.release()

    async def add_message(self, message: str):
        if self.queue.full():
            await self._train_queue()
        await self.queue.put(message)

    async def _train_queue(self):
        start_time = time.time()
        sample = []
        logging.debug(f"asyncmodel: making sample from internal queue")
        while not self.queue.empty():
            message: str = await self.queue.get()
            message_content = message.strip()
            sample.append(message_content)
            self.queue.task_done()
        sentences_amount = len(sample)
        self.sentences += sentences_amount
        logging.debug(f"asyncmodel: started training on {sentences_amount} new sentences")
        new_model_future = self.loop.run_in_executor(self.executor, train, sample)
        async with self.lock:
            if not self.model_future:
                logging.info(f"asyncmodel: First model set")
                self.model_future = new_model_future
            else:
                logging.debug(f"asyncmodel: waiting on training from old sample...")
                old_model = await self.model_future
                logging.debug(f"asyncmodel: ...waiting on training from new sample...")
                new_model = await new_model_future
                logging.debug(f"asyncmodel: ...started combining old sample with new sample")
                self.model_future = self.loop.run_in_executor(self.executor, combine, (old_model, new_model))
                self.read_rate = sentences_amount / (time.time() - start_time)  # sentences per second
                if not self.ready.is_set():
                    async for result in self._generate_messages(skip_wait=True):
                        if result:
                            self.ready.set()
                        break


def train(sample: list, well_formed: bool = True):
    try:
        model = markovify.NewlineText("\n".join(sample), well_formed=well_formed)
        assert model
        return model
    except (KeyError, AssertionError) as e:
        logging.error(f"train: {sample} Caused {e}")


def combine(models: list):
    try:
        combined_model = markovify.combine(models)
        assert combined_model
        return combined_model
    except (KeyError, AssertionError) as e:
        logging.error(f"combine: {models} Caused {e}")


if __name__ == "__main__":
    import json, os
    from dotenv import load_dotenv


    async def main():
        load_dotenv()
        logging.getLogger().setLevel(logging.DEBUG)
        rw_avg = 0
        rr_avg = 0
        with open("sample_data.json", 'r') as f:
            sample_messages = json.load(f)["messages"]
        test_model = asyncmodel(int(os.getenv("ML_SAMPLE_SIZE")))
        for sample_message in sample_messages:
            message_content = sample_message["content"]
            await test_model.add_message(message_content)
            #rw_avg = (rw_avg + test_model.write_rate) / 2
            #rr_avg = (rr_avg + test_model.read_rate) / 2
            #print(round(rw_avg), round(rr_avg))
        profile=await test_model.export()
        print(profile)
    asyncio.run(main())
