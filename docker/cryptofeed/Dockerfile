FROM python:3.10-bullseye
ENV IS_DOCKER True

# add default packages
COPY requirements.txt /requirements.txt
RUN pip3 install -r ./requirements.txt

# local hack to make L2 feeds work again (probably should be merged into the Cryptofeed packageoriginal)
COPY src/coinbase.py /usr/local/lib/python3.10/site-packages/cryptofeed/exchanges/coinbase.py

# copy latest project changes
COPY setup.py /cryptofeed/setup.py
COPY src/ /cryptofeed/src

WORKDIR /cryptofeed
RUN pip3 install .

RUN chmod +x /cryptofeed/src/script/cryptofeed_0_startup.sh
CMD ["/cryptofeed/src/script/cryptofeed_0_startup.sh"]
