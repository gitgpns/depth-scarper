from saver import SaverDB

import uuid
import asyncio
import websockets
import json
import gzip
import traceback


async def subscribe(url, subs, callback=None):
    async with websockets.connect(url) as websocket:
        for sub in subs:
            sub_str = json.dumps(sub)
            await websocket.send(sub_str)
            print(f"send: {sub_str}")

        while True:
            rsp = await websocket.recv()
            data = json.loads(gzip.decompress(rsp).decode())
            json_data = json.dumps(data)

            if "op" in data and data.get("op") == "ping":
                pong_msg = {"op": "pong", "ts": data.get("ts")}
                await websocket.send(json.dumps(pong_msg))
                print(f"send: {pong_msg}")
                continue
            if "ping" in data:
                pong_msg = {"pong": data.get("ping")}
                await websocket.send(json.dumps(pong_msg))
                print(f"send: {pong_msg}")
                continue
            rsp = await callback(json_data)


async def handle_ws_data(message):
    print("callback param", message)
    saver.save_data(message)

if __name__ == "__main__":
    saver = SaverDB('huobi')

    market_url = 'wss://api.hbdm.com/linear-swap-ws'

    market_subs = [
        {
            "sub": "market.BTC-USDT.depth.step0",
            "id": str(uuid.uuid1())
        },
        {
            "sub": "market.AAVE-USDT.depth.step0",
            "id": str(uuid.uuid1())
        },
        {
            "sub": "market.ETH-USDT.depth.step0",
            "id": str(uuid.uuid1())
        },
        {
            "sub": "market.MATIC-USDT.depth.step0",
            "id": str(uuid.uuid1())
        },
        {
            "sub": "market.SUSHI-USDT.depth.step0",
            "id": str(uuid.uuid1())
        },
        {
            "sub": "market.YFI-USDT.depth.step0",
            "id": str(uuid.uuid1())
        },
        {
            "sub": "market.BNB-USDT.depth.step0",
            "id": str(uuid.uuid1())
        }
    ]

    while True:
        try:
            asyncio.get_event_loop().run_until_complete(
                subscribe(market_url, market_subs, handle_ws_data))

        except Exception as e:
            traceback.print_exc()
            print('websocket connection error. reconnect rightnow')
