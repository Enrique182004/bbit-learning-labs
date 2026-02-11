# Copyright 2024 Bloomberg Finance L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import pika
import os

class mqProducerInterface:
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        # Save parameters to class variables
        self.routing_key = routing_key
        self.exchange_name = exchange_name

        # Call setupRMQConnection'
        self.setupRMQConnection()
        pass

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        self.m_connection = pika.BlockingConnection(parameters=conParams)
        # Establish Channel

        # Create the exchange if not already present
        self.m_channel.exchange_declare(self.exchange_name)
        pass

    def publishOrder(self, message: str) -> None:
        # Basic Publish to Exchange
        self.m_channel.basic_publish(self.exchange_name, self.routing_key, message, pika.BasicProperties(content_type='text/plain', delivery_mode=pika.DeliveryMode.Transient))

        # Close Channel
        self.m_channel.close()

        # Close Connection
        self.m_connection.close()
        pass
