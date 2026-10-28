import asyncio
import json
import time
import websockets
import logging
from collections import Counter
from confluent_kafka import Producer

logger = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SYMBOLS = ["btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt"]

TOPICS = {
    "trade": "raw_trades",
    "bookTicker": "raw_book_ticker",
}

SUMMARY_INTERVAL = 10

delivered = Counter()


def delivery_callback(err, msg):
    if err is not None:
        logger.error(f"Delivery error: {err}")
    else:
        delivered[msg.topic()] += 1


def produce_to_kafka(producer, topic, key, message, callback_func):
    producer.produce(topic, key=key.encode("utf-8") if key else None, value=message.encode("utf-8"), callback=callback_func)
    producer.poll(0)


async def connect_and_print(producer, uri):

    attempt = 0
    BASE_DELAY = 1
    MAX_DELAY = 60
    last_summary = time.monotonic()

    while True:
        try:
            async with websockets.connect(uri) as ws:
                async for raw_message in ws:
                    attempt = 0

                    try:
                        message = json.loads(raw_message)
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON, skipping: {raw_message}")
                        continue

                    stream_type = message.get("stream", "").split("@")[-1]
                    topic = TOPICS.get(stream_type)

                    if topic is None:
                        logger.warning(f"Unknown stream type, skipping: {raw_message}")
                        continue

                    key = message.get("data", {}).get("s")

                    if key is None:
                        logger.warning(f"No symbol in message, sending without key: {raw_message}")

                    produce_to_kafka(producer, topic, key, raw_message, delivery_callback)

                    now = time.monotonic()
                    if now - last_summary >= SUMMARY_INTERVAL:
                        logger.info(f"Delivered in last {SUMMARY_INTERVAL}s: {dict(delivered)}")
                        delivered.clear()
                        last_summary = now

        except (websockets.exceptions.ConnectionClosed, OSError):
            logger.warning("Trying to reconnect")
            attempt += 1
            delay = min(MAX_DELAY, BASE_DELAY * 2 ** attempt)
            await asyncio.sleep(delay)


streams = "/".join(f"{symbol}@{stream_type}" for symbol in SYMBOLS for stream_type in TOPICS)

producer = Producer({"bootstrap.servers": "localhost:9094"})

try:
    asyncio.run(connect_and_print(producer, f"wss://stream.binance.com:9443/stream?streams={streams}"))
finally:
    producer.flush()
