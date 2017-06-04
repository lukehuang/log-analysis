import asyncio
from kafka.common import KafkaError
from aiokafka import AIOKafkaConsumer

from settings import KAFKA_SERVERS, KAFKA_RESULTS_TOPIC

async def consume_task(consumer):
    """

    Knows issues:
        https://github.com/aio-libs/aiokafka/issues/166

    :param consumer:
    :return:
    """
    while True:
        try:
            msg = await consumer.getone()
            print("consumed: ", msg.topic, msg.partition, msg.offset,
                  msg.key, msg.value, msg.timestamp)
        except KafkaError as err:
            pass

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    consumer = AIOKafkaConsumer(
        KAFKA_RESULTS_TOPIC,
        loop=loop,
        bootstrap_servers=KAFKA_SERVERS
    )
    loop.run_until_complete(consumer.start())
    c_task = loop.create_task(consume_task(consumer))
    try:
        loop.run_forever()
    finally:
        # Will gracefully leave consumer group; perform autocommit if enabled
        loop.run_until_complete(consumer.stop())
        c_task.cancel()
        loop.close()
