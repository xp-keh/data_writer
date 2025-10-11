import asyncio
import logging
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, WebSocket
from consume.websocket_manager import WebSocketManager
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from consume.kafka import AsyncConsumer
from config.utils import get_env_value
from datastore.redis_store import init_redis
from writer.clickhouse_writer import bulk_write_to_clickhouse
from minio import Minio


kafka_broker = get_env_value("KAFKA_BROKER")
kafka_consume_topic = get_env_value("KAFKA_CONSUME_TOPIC")
kafka_consumer_group = get_env_value("KAFKA_CONSUMER_GROUP")

MINIO_ENDPOINT = get_env_value("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = get_env_value("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = get_env_value("MINIO_SECRET_KEY")
MINIO_BUCKET = get_env_value("MINIO_BUCKET")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI()
websocket_manager = WebSocketManager()
consumer = AsyncConsumer(kafka_broker, kafka_consume_topic, kafka_consumer_group, websocket_manager)

scheduler = AsyncIOScheduler()
scheduler.start()

minio_client = Minio(
    MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Create bucket if not exists
if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)
    logging.info(f"Bucket '{MINIO_BUCKET}' created.")
else:
    logging.info(f"Bucket '{MINIO_BUCKET}' already exists.")


@app.websocket("/")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time streaming."""
    await websocket_manager.connect(websocket)

    try:
        while True:
            await websocket.receive_text()
    except Exception as e:
        logging.info(f"WebSocket disconnected: {e}")
    finally:
        await websocket_manager.disconnect(websocket)

async def async_bulk_write_to_clickhouse():
    """Async function to bulk write to ClickHouse."""
    await bulk_write_to_clickhouse()

@app.on_event("startup")
async def startup_event():
    """Start Kafka consumer and schedule ClickHouse uploads on FastAPI startup."""
    await init_redis()
    await consumer.start()
    asyncio.create_task(consumer.consume()) 

    scheduler.add_job(
        async_bulk_write_to_clickhouse,
        trigger=CronTrigger(minute=0),
        id="clickhouse_upload",
        replace_existing=True
    )
    logging.info("Scheduled ClickHouse upload job every hour.")

@app.on_event("shutdown")
async def shutdown_event():
    """Stop Kafka consumer and scheduler on FastAPI shutdown"""
    await consumer.stop()
    scheduler.shutdown()

@app.get("/ping")
async def healthcheck():
    """Basic health check"""
    return {"status": "healthy"}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"], 
)