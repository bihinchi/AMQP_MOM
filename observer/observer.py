import asyncio, datetime, aiofiles
from aio_pika import connect, IncomingMessage, ExchangeType


ALL_TOPICS = "#"
FILENAME = "shared/logs.txt"


async def clear_file():
   f = await aiofiles.open(FILENAME, "w")
   await f.close()


async def on_message(message: IncomingMessage):
    ts = datetime.datetime.utcnow().isoformat(timespec='milliseconds') + "Z"

    async with message.process():
        str_to_save = "{}, Topic {}: {}".format(ts, message.routing_key, message.body.decode())
        async with aiofiles.open(FILENAME, "a") as out:
            await out.write(str_to_save + "\n")
            await out.flush()


async def main(loop):
    await clear_file()
    
    connection = await connect("amqp://rabbitmq", loop=loop)

    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    topic_exchange = await channel.declare_exchange("topics", ExchangeType.TOPIC)
    queue = await channel.declare_queue("task_queue", durable=True)

    await queue.bind(topic_exchange, ALL_TOPICS)
    await queue.consume(on_message)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main(loop))
    loop.run_forever()