import asyncio

from create_topic import create_topic
from producer import produce_building_sensors, produce_event
from consumer import consume_event
import random


def handle_building_sensors(data):
    if data["temperature"] > 40:
        produce_event(
            "temperature_alerts",
            {
                "sensor_id": data["sensor_id"],
                "timestamp": data["timestamp"],
                "temperature": data["temperature"],
                "message": f"{data['timestamp']}: {data['temperature']}°C for sensor with id {data['sensor_id']} is more than 40°C",
            },
        )
    if data["humidity"] > 80:
        produce_event(
            "humidity_alerts",
            {
                "sensor_id": data["sensor_id"],
                "timestamp": data["timestamp"],
                "humidity": data["humidity"],
                "message": f"{data['timestamp']}: {data['humidity']}% for sensor with id {data['sensor_id']} is more than 80%",
            },
        )
    if data["humidity"] < 20:
        produce_event(
            "humidity_alerts",
            {
                "sensor_id": data["sensor_id"],
                "timestamp": data["timestamp"],
                "humidity": data["humidity"],
                "message": f"{data['timestamp']}: {data['humidity']}% for sensor with id {data['sensor_id']} is less than 20%",
            },
        )


def handle_alert(data):
    print(data["message"])


async def main():
    create_topic("building_sensors")
    create_topic("temperature_alerts")
    create_topic("humidity_alerts")

    sensor_id = random.randint(1, 1000)

    producer = asyncio.create_task(produce_building_sensors(sensor_id))
    consumer_building_sensors = asyncio.create_task(
        consume_event("building_sensors", handle_building_sensors)
    )
    consumer_temperature_alerts = asyncio.create_task(
        consume_event("temperature_alerts", handle_alert)
    )
    consumer_humidity_alerts = asyncio.create_task(
        consume_event("humidity_alerts", handle_alert)
    )
    await asyncio.gather(
        producer,
        consumer_building_sensors,
        consumer_temperature_alerts,
        consumer_humidity_alerts,
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Keyboard interrupt received. Exiting...")
