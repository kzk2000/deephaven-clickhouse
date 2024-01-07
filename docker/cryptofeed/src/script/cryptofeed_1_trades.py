import os
from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES
from cryptofeed.exchanges import Coinbase, Bitstamp, Kraken

import src.cryptofeed_tools as cft


def main():
    # see docker_files/Dockerfile.cryptofeed where we set IS_DOCKER=True
    # by doing this here, we can also run this script locally
    # see https://www.confluent.io/blog/kafka-client-cannot-connect-to-broker-on-aws-on-docker-etc/#scenario-4
    kakfa_bootstrap = 'redpanda' if os.environ.get('IS_DOCKER') else 'localhost'
    kakfa_port = 29092 if os.environ.get('IS_DOCKER') else 9092

    ch_tradekafka = cft.ClickHouseTradeKafka(bootstrap=kakfa_bootstrap, port=kakfa_port)

    # cft.SYMBOLS = ['BTC-USD']   # for testing

    f = FeedHandler()
    f.add_feed(Coinbase(channels=[TRADES], symbols=cft.SYMBOLS, callbacks={TRADES: [ch_tradekafka, cft.my_print]}))
    f.add_feed(Bitstamp(channels=[TRADES], symbols=cft.SYMBOLS, callbacks={TRADES: [ch_tradekafka, cft.my_print]}))
    f.add_feed(Kraken(channels=[TRADES], symbols=cft.SYMBOLS, callbacks={TRADES: [ch_tradekafka, cft.my_print]}))
    f.run()


if __name__ == '__main__':
    main()
