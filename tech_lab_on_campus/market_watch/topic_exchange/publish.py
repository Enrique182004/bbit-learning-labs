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

import argparse
import sys

from solution.producer_sol import mqProducer  # pylint: disable=import-error

def main(ticker: str, price: float, sector: str) -> None:
    # Step 2: Create routing key
    routingKey = f"stock.{ticker}.{sector}"
    
    # Step 3: Create message  
    message = f"{ticker} is ${price}"
    
    # FIX: Remove routing_key from constructor
    producer = mqProducer(exchange_name="Tech Lab Topic Exchange")
    
    # Pass routing_key to publishOrder instead
    producer.publishOrder(message, routingKey)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("ticker", help="Stock ticker")
    parser.add_argument("price", type=float, help="Stock price")
    parser.add_argument("sector", help="Stock sector")
    args = parser.parse_args()

    sys.exit(main(args.ticker,args.price,args.sector))
