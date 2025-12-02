from kafka import KafkaConsumer
from json import loads
from datetime import datetime

consumer = KafkaConsumer(
    'machines-status',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: loads(m.decode('utf-8'))
)

print("Starting Monitoring Consumer...")
for msg in consumer:
    data = msg.value
    machine = data['machine']
    temp = data['temperature']
    vib = data['vibration']

    status = "OK"
    if temp > 100 or vib > 15:
        status = "CRITICAL"
    elif temp > 80 or vib > 10:
        status = "WARNING"

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] Machine {machine} | Temp={temp}°C | Vib={vib} | Status={status}")

    if status in ("WARNING", "CRITICAL"):
        print("ALERTE PANNE POTENTIELLE !")
