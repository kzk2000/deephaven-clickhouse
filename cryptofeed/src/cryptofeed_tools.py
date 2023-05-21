import asyncio
from aiokafka import AIOKafkaProducer
from cryptofeed.backends.kafka import BookKafka
from yapic import json

SYMBOLS = ['BTC-USD', 'ETH-USD', 'AVAX-USD', 'SOL-USD']


class ClickHouseBookKafka(BookKafka):
    topic = 'orderbooks'

    async def write(self, data: dict):
        await self._KafkaCallback__connect()
        data['ts'] = int(data.pop('timestamp') * 1_000_000_000)
        data['receipt_ts'] = int(data.pop('receipt_timestamp') * 1_000_000_000)
        data['bid'] = data['book'].pop('bid')
        data['ask'] = data['book'].pop('ask')
        del data['book']
        await self.producer.send_and_wait(self.topic, json.dumps(data).encode('utf-8'))


async def my_print(data, _receipt_time):
    print(data)


class KafkaCallback:
    def __init__(self, bootstrap='127.0.0.1', port=9092, topic=None, numeric_type=float, none_to=None,
                 **kwargs):  # working locally
        """
        bootstrap: str, list
            if a list, should be a list of strings in the format: ip/host:port, i.e.
                192.1.1.1:9092
                192.1.1.2:9092
                etc
            if a string, should be ip/port only
        """
        self.bootstrap = bootstrap
        self.port = port
        self.producer = None
        self.topic = topic if topic else self.default_topic
        self.numeric_type = numeric_type
        self.none_to = none_to

    async def __connect(self):
        if not self.producer:
            loop = asyncio.get_event_loop()
            self.producer = AIOKafkaProducer(acks=0,
                                             loop=loop,
                                             bootstrap_servers=f'{self.bootstrap}:{self.port}' if isinstance(
                                                 self.bootstrap, str) else self.bootstrap,
                                             client_id='cryptofeed')
            await self.producer.start()

    async def write(self, data: dict):
        await self._connect()
        await self.producer.send_and_wait(self.topic, json.dumps(data).encode('utf-8'))


class ClickHouseTradeKafka(KafkaCallback):
    default_topic = 'trades'

    async def __call__(self, dtype, receipt_timestamp: float):
        if isinstance(dtype, dict):
            data = dtype
        else:
            data = dtype.to_dict(numeric_type=self.numeric_type, none_to=self.none_to)
            if not dtype.timestamp:
                data['timestamp'] = receipt_timestamp
            data['receipt_timestamp'] = receipt_timestamp
        await self.write(data)

    async def write(self, data: dict):
        await self._KafkaCallback__connect()
        try:
            data['ts'] = int(data.pop('timestamp') * 1_000_000_000)
            data['receipt_ts'] = int(data.pop('receipt_timestamp') * 1_000_000_000)
            data['size'] = data.pop('amount')
            data['trade_id'] = data.pop('id')
            data.pop('type')
        except:
            pass

        await self.producer.send_and_wait(self.topic, json.dumps(data).encode('utf-8'))
