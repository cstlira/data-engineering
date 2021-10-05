#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Example Kafka Producer.
# Reads lines from stdin and sends to Kafka.
#
import json

from faker import Faker
from faker_vehicle import VehicleProvider
import random
import time
import uuid
import datetime
import asyncio

fake = Faker('pt_BR')
fake.add_provider(VehicleProvider)
from confluent_kafka import Producer
import sys
import plates

if __name__ == '__main__':
    if len(sys.argv) != 4:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]
    toll_gate_id = sys.argv[3]
    evh_connection_string = "Endpoint=sb://evhns-flira-dev-eastus-001.servicebus.windows.net/;SharedAccessKeyName=send-toll-event;SharedAccessKey=/9aySWCeQzy0=;EntityPath=eh-toll-flira-dev-eastus-001"

    # Producer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {'bootstrap.servers': broker,
            'security.protocol': 'sasl_ssl',
            'ssl.ca.location': '/usr/local/etc/openssl/cert.pem',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': '$ConnectionString',
            'sasl.password': f"{evh_connection_string}"
            }

    # Create Producer instance
    p = Producer(**conf)
    plates = plates.Plate()

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

    def send_car():
        car = fake.vehicle_object()
        #car["Price"] = round(random.uniform(1000.00, 1000000.00), 2)
        car["TollId"] = int(toll_gate_id)
        car["TagId"] = str(uuid.uuid4())
        car["Plate"] = plates.gen_plate()
        car["Timestamp"] = datetime.datetime.now().isoformat()
        try:
            p.produce(topic, json.dumps(car), callback=delivery_callback)
            #print(car)
        except Exception as e:
            sys.stderr.write('There was an error while generating your super car!')


    # Read lines from stdin, produce each line to Kafka
    for i in range(10000000):
        try:
            send_car()
            time.sleep(0.5)
        except BufferError:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(p))

        # Serve delivery callback queue.
        # NOTE: Since produce() is an asynchronous API this poll() call
        #       will most likely not serve the delivery callback for the
        #       last produce()d message.
        p.poll(0)

    # Wait until all messages have been delivered
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()
