import json
import logging
from aiokafka import AIOKafkaConsumer
from config.logging import Logger
from datastore.redis_store import save_weather_data
from consume.websocket_manager import WebSocketManager
import traceback
from starlette.websockets import WebSocketDisconnect

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class AsyncConsumer:
    def __init__(self, kafka_broker, topic, group_id, websocket_manager: WebSocketManager):
        self.kafka_broker = kafka_broker
        self.topic = topic
        self.group_id = group_id
        self.logger = Logger().setup_logger(service_name="consumer")
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_broker,
            group_id=self.group_id,
            auto_offset_reset="latest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        self.websocket_manager = websocket_manager

    async def start(self):
        await self.consumer.start()

    async def stop(self):
        await self.consumer.stop()

    async def consume(self):
        """Continuously consume messages, save them to Redis, and put them in the queue for SSE."""
        try:
            async for message in self.consumer:
                raw_data = message.value

                weather_data = {
                    "location": raw_data.get("location", "unknown"),
                    "temp": raw_data.get("main", {}).get("temp", 0.0),
                    "feels_like": raw_data.get("main", {}).get("feels_like", 0.0),
                    "pressure": raw_data.get("main", {}).get("pressure", 0),
                    "humidity": raw_data.get("main", {}).get("humidity", 0),
                    "wind_speed": raw_data.get("wind", {}).get("speed", 0.0),
                    "wind_deg": raw_data.get("wind", {}).get("deg", 0),
                    "wind_gust": raw_data.get("wind", {}).get("gust", 0.0),  
                    "clouds": raw_data.get("clouds", {}).get("all", 0),
                    "description": raw_data.get("weather", [{}])[0].get("description", "unknown"),
                    "timestamp": int(str(raw_data.get("raw_produce_dt", 0))[:10]),
                    "lat": raw_data.get("lat", "unknown"),
                    "lon": raw_data.get("lon", "unknown"),
                    "dt": int(str(raw_data.get("dt", 0))[:10])
                }

                key = f"weather:{weather_data['timestamp']}_{weather_data['location']}"

                try:
                    await self.websocket_manager.broadcast(json.dumps(weather_data))
                except WebSocketDisconnect:
                    self.logger.warning("WebSocket disconnected. Skipping message broadcast.")

                await save_weather_data(key, weather_data)

                try:
                    await self.consumer.commit()
                except Exception as e:
                    self.logger.error(f"Error committing Kafka offset: {e}")
                    
        except Exception as e:
            self.logger.error(f" [x] Error in consumer: {e}")
            self.logger.error(traceback.format_exc())
