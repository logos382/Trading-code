"A script that retrive and store data feed from live service"

import websocket
import json
import _thread
import time
import rel


def on_open(ws):
    data = json.dumps({"op": "subscribe",
                       "channel": "ticker",
                       "market": "BTC-PERP"})
    ws.send(data)
    print("Connected")

def on_message(ws, message):
    print(message)

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(close_msg):
    print(f"Connection close")

if __name__ == "__main__":

    # try:
    #     while True:

    #         endpoint = "wss://ftx.com/ws/"

    #         websocket.enableTrace(True)

    #         ws = websocket.WebSocketApp(endpoint, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)

    #         # ws.run_forever(ping_interval=15)    # Set a ping to server every 15s
    #         ws.run_forever()

    # except KeyboardInterrupt:
    #     ws.close()

    endpoint = "wss://ftx.com/ws/"

    websocket.enableTrace(True)

    ws = websocket.WebSocketApp(endpoint, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)

    # ws.run_forever(dispatcher=rel)    # Set a ping to server every 15s
    ws.run_forever()    
    
    # rel.signal(2, rel.abort(), ws.close())  # Keyboard Interrupt
    
    # rel.dispatch()