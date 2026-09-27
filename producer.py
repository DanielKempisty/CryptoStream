import asyncio
import websockets
import logging
from confluent_kafka import Producer

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def delivery_callback(err, msg):
    if err is not None:
        logger.error(f"Delivery error: {err}")
    else:
        logger.info(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def produce_to_kafka(producer, topic, message, callback_func):
    producer.produce(topic, value=message.encode("utf-8"), callback=callback_func)
    producer.poll(0)


async def connect_and_print(producer, uri):

    attempt = 0
    BASE_DELAY = 1
    MAX_DELAY = 60
    topic = "raw_trades"

    while True:
        try:
            async with websockets.connect(uri) as ws:
                async for raw_message in ws:
                    attempt = 0

                    produce_to_kafka(producer, topic, raw_message, delivery_callback)

        except (websockets.exceptions.ConnectionClosed, OSError):
            logger.warning("Trying to reconnect")
            attempt += 1
            delay = min(MAX_DELAY, BASE_DELAY * 2 ** attempt)
            await asyncio.sleep(delay)  

     
producer = Producer({"bootstrap.servers": "localhost:9094"})

try:
    asyncio.run(connect_and_print(producer, "wss://stream.binance.com:9443/ws/btcusdt@trade"))
finally:
    producer.flush()
