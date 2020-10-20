import sys, asyncio
from aio_pika import connect, Message, IncomingMessage, DeliveryMode, ExchangeType
from functools import partial


TOPIC_LISTEN = "my.o"
TOPIC_SEND = "my.i"


async def on_message(message: IncomingMessage, exchange):
    async with message.process():
        encoded_msg = ("Got {}".format(message.body.decode())).encode()
        message = Message(encoded_msg, delivery_mode=DeliveryMode.PERSISTENT)
        await asyncio.sleep(1)
        await exchange.publish(message, routing_key=TOPIC_SEND)

    
async def main(loop):
    connection = await connect("amqp://rabbitmq", loop=loop)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)
    topic_exchange = await channel.declare_exchange("topics", ExchangeType.TOPIC)
    
    queue = await channel.declare_queue("inter_queue", durable=True)
    
    # bind parameter to callable
    on_message_with_exchange = partial(on_message, exchange=topic_exchange)

    await queue.bind(topic_exchange, TOPIC_LISTEN)
    await queue.consume(on_message_with_exchange)
    

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main(loop))
    loop.run_forever()