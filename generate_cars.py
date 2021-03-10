from faker import Faker
from faker_vehicle import VehicleProvider
import random
import time
import uuid

fake = Faker('pt_BR')
fake.add_provider(VehicleProvider)

while True:
    car = fake.vehicle_object()
    car["Price"] = round(random.uniform(1000.00, 1000000.00), 2)
    car["Customer"] = str(uuid.uuid4())
    try:
        print(car)
        time.sleep(1)
    except Exception as e:
        print(e)
        continue
