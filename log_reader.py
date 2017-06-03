import time
import asyncio
from aiokafka import AIOKafkaProducer

from settings import KAFKA_SERVERS, SAVEPOINT, LOG_FILE, KAFKA_TOPIC


class LogStreamer:
    def __init__(self, savepoint_file, log_file):
        self.loop = asyncio.get_event_loop()
        self.producer = AIOKafkaProducer(loop=self.loop, bootstrap_servers=KAFKA_SERVERS)
        self.savepoint_file = savepoint_file
        self.log_file = log_file

    @asyncio.coroutine
    def produce(self):
        last = self.savepoint_file.read()
        if last:
            self.log_file.seek(int(last))

        while True:
            line = log_file.readline()
            if not line:
                time.sleep(0.1)

                current_position = self.log_file.tell()

                if last != current_position:
                    self.savepoint_file.seek(0)
                    self.savepoint_file.write(str(current_position))

                continue
            else:
                log_entry = line.strip(' \t\n\r')

                '''
                Here we can convert our data to JSON. But I because JSON performance is not extremely good
                with standart libraries, and because we use asynchronous non-blocking model here, I think it's
                best to just pass data as is. I want to create as little as possible overhead here. We want to
                stream data as fast as possible.
                '''

                future = yield from self.producer.send(KAFKA_TOPIC, log_entry.encode())
                resp = yield from future
                print("Message produced: partition {}; offset {}".format(
                    resp.partition, resp.offset))

                '''
                # Also can use a helper to send and wait in 1 call
                resp = yield from self.producer.send_and_wait(
                    'foobar', key=b'foo', value=b'bar')
                resp = yield from self.producer.send_and_wait(
                    'foobar', b'message for partition 1', partition=1)
                    '''

    def start(self):
        self.loop.run_until_complete(self.producer.start())
        self.loop.run_until_complete(self.produce())
        self.loop.run_until_complete(self.producer.stop())
        self.loop.close()

if __name__ == '__main__':
    with open(SAVEPOINT, 'r+') as savepoint_file, open(LOG_FILE, 'r') as log_file:
        streamer = LogStreamer(savepoint_file, log_file)
        streamer.start()
