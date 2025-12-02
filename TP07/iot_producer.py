from time import sleep
from json import dumps
from kafka import KafkaProducer
import random

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: dumps(v).encode('utf-8')
)

machines = ["M1", "M2", "M3"]

print("Starting IoT Producer...")
while True:
    machine = random.choice(machines)
    temperature = round(random.uniform(40, 120), 2)
    vibration = round(random.uniform(0, 20), 2)

    data = {
        "machine": machine,
        "temperature": temperature,
        "vibration": vibration
    }

    producer.send('machines-status', value=data)
    print("Sent:", data)

    sleep(2)
