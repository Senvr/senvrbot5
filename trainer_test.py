import asyncio
import websockets
import json

async def train():
    training_data = json.load(open("sample_data.json"))
    async with websockets.connect('ws://localhost:8765') as websocket:
        for message in training_data["messages"]:
            if message["type"] == "Default":
                await websocket.send(message["content"])
        print("Waiting for messages")
        async for message in websocket:
            print(message)
        print("No more messages")
asyncio.get_event_loop().run_until_complete(train())