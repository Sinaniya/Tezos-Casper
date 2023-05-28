from signalrcore.hub_connection_builder import HubConnectionBuilder
from time import sleep
from pprint import pprint

connection = HubConnectionBuilder()\
    .with_url('https://api.tzkt.io/v1/accounts')\
    .with_automatic_reconnect({
        "type": "interval",
        "keep_alive_interval": 10,
        "intervals": [1, 3, 5, 6, 7, 87, 3]
    })\
    .build()

def init():
    print("connection established, subscribing to blocks and operations")
    connection.send('SubscribeToHead', [])
    connection.send('SubscribeToOperations', 
                    [{'address': 'KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton', 
                      'types': 'transaction'}])

connection.on_open(init)
connection.on("head", pprint)
connection.on("operations", pprint)

connection.start()

try:
    while True:
        sleep(1)
except KeyboardInterrupt:
    pass
finally:
    print('shutting down...')
    connection.stop()