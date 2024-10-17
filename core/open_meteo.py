from datetime import datetime, timedelta, timezone

import requests

from constants import OPEN_METEO_API_URL
from database import (
    fetch_forecast_data,
    fetch_weather_data,
    insert_forecast_data,
    insert_weather_data,
)

from .interfaces import WeatherDataFetcher

SOURCE_NAME = "openmeteo"

WEATHER_CODE_MAPPING = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Drizzle: Light intensity",
    53: "Drizzle: Moderate intensity",
    55: "Drizzle: Dense intensity",
    56: "Freezing Drizzle: Light intensity",
    57: "Freezing Drizzle: Dense intensity",
    61: "Rain: Slight intensity",
    63: "Rain: Moderate intensity",
    65: "Rain: Heavy intensity",
    66: "Freezing Rain: Light intensity",
    67: "Freezing Rain: Heavy intensity",
    71: "Snow fall: Slight intensity",
    73: "Snow fall: Moderate intensity",
    75: "Snow fall: Heavy intensity",
    77: "Snow grains",
    80: "Rain showers: Slight intensity",
    81: "Rain showers: Moderate intensity",
    82: "Rain showers: Violent intensity",
    85: "Snow showers: Slight intensity",
    86: "Snow showers: Heavy intensity",
}


class OpenMeteoDataFetcher(WeatherDataFetcher):

    async def _fetch_data_from_api(self, lat, lng):
        url = f"{OPEN_METEO_API_URL}?latitude={lat}&longitude={lng}&current=temperature_2m,relative_humidity_2m,weather_code&hourly=temperature_2m,relative_humidity_2m,weather_code"
        resp = requests.get(url)
        success, forecast_formatted_data = self.format_forecast(resp)
        _, weather_formatted_data = self.format_weather(resp)
        if success:
            await insert_forecast_data(forecast_formatted_data)
            await insert_weather_data(weather_formatted_data)
        return success, forecast_formatted_data, weather_formatted_data

    async def fetch_forecast(self, lat, lng, timezone_offset=0):
        db_results = await fetch_forecast_data(lat, lng, SOURCE_NAME, timezone_offset)
        if db_results is not None and db_results.get("prediction_data"):
            return True, db_results
        success, forecast_formatted_data, _ = await self._fetch_data_from_api(lat, lng)
        today = (
            (datetime.now(tz=timezone.utc) + timedelta(minutes=timezone_offset))
            .date()
            .isoformat()
        )
        if forecast_formatted_data["prediction_data"].get(today):
            forecast_formatted_data["prediction_data"] = {
                today: forecast_formatted_data["prediction_data"][today]
            }
        return success, forecast_formatted_data

    async def fetch_weather(self, lat, lng):
        db_results = await fetch_weather_data(lat, lng, SOURCE_NAME)
        if db_results:
            return True, db_results
        success, _, weather_formatted_data = await self._fetch_data_from_api(lat, lng)
        return success, weather_formatted_data

    def _calculate_hours_prediction(self, response_data):
        hours_data = response_data["hourly"]
        time_list = hours_data["time"]
        temprature_list = hours_data["temperature_2m"]
        humidity_list = hours_data["relative_humidity_2m"]
        weather_code_list = hours_data["weather_code"]
        prediction_data = {}
        current_list = []
        hours_length = len(time_list) - 1
        current_date = time_list[0].split("T")[0]
        for index in range(len(time_list)):
            current_list.append(
                {
                    "current_temp": temprature_list[index],
                    "humidity": humidity_list[index],
                    "condition_text": WEATHER_CODE_MAPPING.get(
                        weather_code_list[index]
                    ),
                    "date": datetime.strptime(
                        time_list[index], "%Y-%m-%dT%H:%M"
                    ).replace(tzinfo=timezone.utc),
                }
            )
            if index < hours_length:
                next_date = time_list[index + 1].split("T")[0]
                if current_date != next_date:
                    prediction_data[current_date] = current_list
                    current_date = next_date
                    current_list = []
        return prediction_data

    def format_forecast(self, response_data):
        data = response_data.json()
        if response_data.status_code != 200:
            return False, data
        formatted_data = {
            "location": {
                "type": "Point",
                "coordinates": [
                    data["latitude"],
                    data["longitude"],
                ],
            },
            "prediction_data": self._calculate_hours_prediction(data),
            "source": SOURCE_NAME,
        }
        return True, formatted_data

    def format_weather(self, response_data):
        data = response_data.json()
        if response_data.status_code != 200:
            return False, data
        formatted_data = {
            "current_temp": data["current"]["temperature_2m"],
            "humidity": data["current"]["relative_humidity_2m"],
            "condition_text": WEATHER_CODE_MAPPING.get(data["current"]["weather_code"]),
            "source": SOURCE_NAME,
            "location": {
                "type": "Point",
                "coordinates": [data["latitude"], data["longitude"]],
            },
        }
        return True, formatted_data
