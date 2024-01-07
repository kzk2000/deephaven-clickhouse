FROM python:3.10-bullseye
ENV IS_DOCKER True

# add default packages
COPY requirements.txt /requirements.txt
RUN pip3 install -r ./requirements.txt

# local hack to make L2 feeds work again (probably should be merged into the Cryptofeed packageoriginal)
COPY src/coinbase.py /usr/local/lib/python3.10/site-packages/cryptofeed/exchanges/coinbase.py

# copy latest project changes
COPY setup.py /setup.py
COPY src /src
RUN pip3 install .

RUN chmod +x ./src/script/cryptofeed_0_startup.sh
CMD ["/src/script/cryptofeed_0_startup.sh"]
