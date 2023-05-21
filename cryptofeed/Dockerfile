FROM python:3.8-bullseye
ENV IS_DOCKER True

# add default packages
COPY ./requirements.txt /requirements.txt
RUN pip3 install -r ./requirements.txt

# copy latest project changes
COPY ./setup.py /setup.py
COPY ./src /src
RUN pip3 install .

RUN chmod +x ./src/script/cryptofeed_0_startup.sh
CMD ["/src/script/cryptofeed_0_startup.sh"]
