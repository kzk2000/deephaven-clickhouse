from cryptofeed import FeedHandler
from cryptofeed.defines import L2_BOOK
from cryptofeed.exchanges import Coinbase, Kraken

import src.cryptofeed_tools as cft
import os


def main():
    kakfa_bootstrap = 'redpanda' if os.environ.get('IS_DOCKER') else 'localhost'
    kakfa_port = 29092 if os.environ.get('IS_DOCKER') else 9092

    ch_book_kafka = cft.ClickHouseBookKafka(bootstrap=kakfa_bootstrap, port=kakfa_port,
                                            snapshot_interval=25000, snapshots_only=True)

    callbacks = {L2_BOOK: [ch_book_kafka, cft.my_print]}

    f = FeedHandler()
    f.add_feed(Kraken(max_depth=10, channels=[L2_BOOK], symbols=cft.SYMBOLS, callbacks=callbacks))
    f.run()


if __name__ == '__main__':
    main()
