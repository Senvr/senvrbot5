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
        self._new_model_future = None
        self._old_model_future = None
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
        temp_model = await self._get_model(ignore_small=True)
        temp_model_dict = temp_model.to_dict()
        return temp_model_dict

    async def _get_model(self, ignore_small=True):
        async with self.lock:
            temp_model = None
            if self.model_future:
                if self.sentences > self.sample_size or ignore_small:
                    temp_model = await self.model_future
        return temp_model

    async def generate_sentence(self, max_tries=16):
        start_time=time.time()
        logging.info(f"asyncmodel: Generating sentence")
        tries = 0
        if not self.ready.is_set():
            return None
        async for result in self._result_generator():
            tries += 1
            if tries > max_tries:
                logging.info(f"asyncmodel: Max tries hit")
                return result
            elif result:
                duration = (time.time() - start_time)
                logging.info(f"asyncmodel: Sentence made after {duration}s")
                self.write_rate = 1 / duration  # sentences per second
                return result
            else:
                await asyncio.sleep(0)

    async def _result_generator(self):
        result_future = None
        result=None
        while not result:
            if result_future:
                result = await result_future
                result_future=None
                yield result
            else:
                logging.debug(f"asyncmodel: getting model")
                temp_model = await self._get_model()
                if temp_model:
                    logging.debug(f"waiting on model to be ready")
                    await self.ready.wait()
                    logging.debug(f"asyncmodel: generating sentence")
                    result_future = self.loop.run_in_executor(self.executor, temp_model.make_sentence)
                else:
                    logging.debug(f"model not ready")
                    break
        if not result:
            logging.warning(f"asyncmodel: unsetting ready, failed to make sentence")
            self.ready.clear()

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
        self._new_model_future = self.loop.run_in_executor(self.executor, train, sample)
        if not self.model_future:
            if not self._old_model_future:
                async with self.lock:
                    self.model_future = self._new_model_future
                logging.debug(f"asyncmodel: First model set")
                return
            else:
                logging.debug(f"asyncmodel: Setting model future to old model future")
                async with self.lock:
                    self.model_future=self._old_model_future

        async with self.lock:
            self._old_model_future = self.model_future
            logging.debug(f"asyncmodel: ...started combining old sample with new sample")
            self.model_future = self.loop.run_in_executor(self.executor, combine, (await self._old_model_future, await self._new_model_future))
            self.read_rate = sentences_amount / (time.time() - start_time)  # sentences per second
            self.ready.set()

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


def modelinfo(model: asyncmodel, start_time: float = None):
    print(f"{model.sentences} messages")
    if start_time:
        print(f"Took {time.time() - start_time}s")
        print(f"{model.sentences / (time.time() - start_time)} messages per second")
    print(f"Average model write rate: {round(model.write_rate)}")
    print(f"Average model read rate: {round(model.read_rate)}")


if __name__ == "__main__":
    import json, os
    from dotenv import load_dotenv


    async def main():
        load_dotenv()
        logging.getLogger().setLevel(logging.DEBUG)
        start_time = time.time()
        with open("sample_data.json", 'r') as f:
            sample_messages = json.load(f)["messages"]
        test_model = asyncmodel(int(os.getenv("ML_SAMPLE_SIZE")))
        for sample_message in sample_messages:
            message_content = sample_message["content"]
            await test_model.add_message(message_content)
            result = await test_model.generate_sentence()
            if result:
                logging.info(f"MODEL>{result}")
            modelinfo(test_model, start_time)

        async with test_model as model:
            print(await model.generate_sentence())
            profile = await test_model.export()
            modelinfo(test_model, start_time)
            print(f"Profile: {profile}")


    asyncio.run(main())
