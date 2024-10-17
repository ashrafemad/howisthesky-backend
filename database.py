import copy
from datetime import datetime, timedelta, timezone

from motor.motor_asyncio import AsyncIOMotorClient

from constants import *

DB_CLIENT = AsyncIOMotorClient(DB_CONNECTION_STRING)


async def check_database_connection():
    await DB_CLIENT.server_info()
    print("Database connection established.")


async def get_db_instance():
    return DB_CLIENT.get_database(DB_NAME)


async def ensure_indexes():
    db = await get_db_instance()
    await db.weather.create_index(
        [("location", "2dsphere")], name="weather_location_2dsphere"
    )
    await db.forecast.create_index(
        [("location", "2dsphere")], name="forecast_location_2dsphere"
    )
    await db.weather.create_index([("expiration_time", 1)], expireAfterSeconds=0)


async def fetch_forecast_data(lat, lng, source, timezone_offset=0):
    db_instance = await get_db_instance()
    today = (
        (datetime.now(tz=timezone.utc) + timedelta(minutes=timezone_offset))
        .date()
        .isoformat()
    )
    today_predections = await db_instance.forecast.find_one(
        {
            "location": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [lat, lng]},
                    "$maxDistance": LOCATION_SEARCH_RADIUS,
                }
            },
            "source": source,
        },
        {
            f"prediction_data.{today}": 1,
            "_id": 0,
            "source": 1,
            "location": 1,
            "city": 1,
        },
        sort=[("timestamp", -1)],
    )
    return today_predections


async def fetch_weather_data(lat, lng, source):
    db_instance = await get_db_instance()
    nearest_weather = await db_instance.weather.find_one(
        {
            "location": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [lat, lng]},
                    "$maxDistance": LOCATION_SEARCH_RADIUS,
                }
            },
            "source": source,
        },
        {"_id": 0},
        sort=[("timestamp", -1)],
    )
    return nearest_weather


async def insert_weather_data(document):
    db_instance = await get_db_instance()
    expire_time = datetime.now(timezone.utc) + timedelta(hours=1)
    copied_doc = copy.deepcopy(document)
    copied_doc["timestamp"] = datetime.now(timezone.utc)
    copied_doc["expiration_time"] = expire_time
    result = await db_instance.weather.insert_one(copied_doc)
    return result


async def insert_forecast_data(document):
    db_instance = await get_db_instance()
    copied_doc = copy.deepcopy(document)
    copied_doc["timestamp"] = datetime.now(timezone.utc)
    result = await db_instance.forecast.insert_one(copied_doc)
    return result
