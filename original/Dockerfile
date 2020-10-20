FROM python:alpine

RUN apk add --no-cache gcc musl-dev bash

RUN pip3 install --upgrade pip && \
	pip3 install --no-cache-dir aio-pika

COPY . .

ENTRYPOINT ["./wait-for-it.sh", "rabbitmq:5672",  "--", "python3", "send.py"]
