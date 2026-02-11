import pika
from producer_interface import mqProducerInterface

class mqProducer(mqProducerInterface):
    def __init__(self, exchange_name: str) -> None:
        self.exchange_name = exchange_name
        self.connection = None
        self.channel = None
        self.setupRMQConnection()
    
    def setupRMQConnection(self) -> None:
        parameters = pika.ConnectionParameters(
            host="localhost",
            port=5672,
            virtual_host='/',
            credentials=pika.PlainCredentials('guest', 'guest')
        )
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        # TOPIC EXCHANGE CHANGE
        self.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type='topic',  # ← CHANGED from 'direct'
            durable=True
        )
    
    def publishOrder(self, message: str, routing_key: str) -> None:  # Added routing_key param
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"Sent {routing_key}: {message}")
        self.channel.close()
        self.connection.close()
