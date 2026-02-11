import pika

from producer_interface import mqProducerInterface


class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.connection = None
        self.channel = None
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        parameters = pika.ConnectionParameters(
            host="localhost",  # ← CHANGE from "host.docker.internal"
            port=5672,
        )

        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type="direct",
            durable=True,
        )

def publishOrder(self, message: str, routing_key: str) -> None:
    self.channel.basic_publish(
        exchange=self.exchange_name,
        routing_key=routing_key,  # Use passed routing_key
        body=message,
        properties=pika.BasicProperties(delivery_mode=2)
    )
    print(f"Sent {routing_key}: {message}")
    if self.channel and self.channel.is_open:
        self.channel.close()
    if self.connection and self.connection.is_open:
        self.connection.close()
