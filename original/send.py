import sys
import asyncio
from aio_pika import connect, Message, DeliveryMode, ExchangeType

FIRST_MSG_IDX = 1
LAST_MSG_IDX = 3
TOPIC = "my.o"


async def main(loop):

    connection = await connect("amqp://rabbitmq", loop=loop)
    channel = await connection.channel()
    topic_exchange = await channel.declare_exchange("topics", ExchangeType.TOPIC)
    
    
    # wait for listeners to activate
    await asyncio.sleep(2)
    
    
    # Sending the messages 
    for i in range(1, 4):
        message = Message(("MSG_" + str(i)).encode(), delivery_mode=DeliveryMode.PERSISTENT)
        await topic_exchange.publish(message, routing_key=TOPIC)
        
        if i != LAST_MSG_IDX:
            await asyncio.sleep(3)
            
    await connection.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))