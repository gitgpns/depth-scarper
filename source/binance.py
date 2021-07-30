import json
import websocket
import random
import traceback
from saver import SaverDB


def run():

    socket = 'wss://fstream.binance.com/ws/'

    def on_open(ws):
        print("opened")
        subscribe_message = {
            "method": "SUBSCRIBE",
            "params":
                [
                    "btcusdt@depth20@100ms",
                    "ethusdt@depth20@100ms",
                    "aaveusdt@depth20@100ms",
                    "maticusdt@depth20@100ms",
                    "uniusdt@depth20@100ms",
                    "sushiusdt@depth20@100ms",
                    "yfiusdt@depth20@100ms",
                    "bnbusdt@depth20@100ms"

                ],
            "id": random.randint(1, 10000000)
        }

        ws.send(json.dumps(subscribe_message))

    def on_message(ws, message):
        saver.save_data(message)
        print(message)

    def on_close(ws):
        print("closed connection")

    ws = websocket.WebSocketApp(socket, on_open=on_open, on_message=on_message, on_close=on_close)
    ws.run_forever()


if __name__ == "__main__":
    saver = SaverDB('binance')

    while True:
        try:
            run()

        except Exception as e:
            traceback.print_exc()
            print('websocket connection error. reconnect rightnow')
