import asyncio
from cryptofeed import FeedHandler
from cryptofeed.defines import L2_BOOK
from cryptofeed.exchanges import Coinbase, Kraken, Bitstamp

import src.cryptofeed_tools as cft
import os


def main():
    # see docker_files/Dockerfile.cryptofeed where we set IS_DOCKER=True
    # by doing this here, we can also run this script locally
    # see https://www.confluent.io/blog/kafka-client-cannot-connect-to-broker-on-aws-on-docker-etc/#scenario-4
    kakfa_bootstrap = 'redpanda' if os.environ.get('IS_DOCKER') else 'localhost'
    kakfa_port = 29092 if os.environ.get('IS_DOCKER') else 9092

    ch_book_kafka = cft.ClickHouseBookKafka(bootstrap=kakfa_bootstrap, port=kakfa_port,
                                            snapshot_interval=25000, snapshots_only=True)

    callbacks = {L2_BOOK: [ch_book_kafka, cft.my_print]}

    # cft.SYMBOLS = ['BTC-USD']   # for testing

    f = FeedHandler()
    f.add_feed(Coinbase(max_depth=2000, channels=[L2_BOOK], symbols=cft.SYMBOLS, callbacks=callbacks))
    f.add_feed(Bitstamp(channels=[L2_BOOK], symbols=cft.SYMBOLS, callbacks=callbacks))
    f.add_feed(Kraken(channels=[L2_BOOK], symbols=cft.SYMBOLS, callbacks=callbacks))
    
    # Fix for Python 3.10+ asyncio event loop issue
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    f.run()


if __name__ == '__main__':
    main()
