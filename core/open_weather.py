import copy
from datetime import datetime, timedelta, timezone

import requests

from constants import (
    OPEN_WEATHER_API_KEY,
    OPEN_WEATHER_FORECAST_API_URL,
    OPEN_WEATHER_ICON_URL,
    OPEN_WEATHER_WEATHER_API_URL,
)
from database import (
    fetch_forecast_data,
    fetch_weather_data,
    insert_forecast_data,
    insert_weather_data,
)

from .interfaces import WeatherDataFetcher

SOURCE_NAME = "openweathermap"


class OpenWeatherDataFetcher(WeatherDataFetcher):

    async def fetch_forecast(self, lat, lng, timezone_offset=0):
        db_results = await fetch_forecast_data(lat, lng, SOURCE_NAME, timezone_offset)
        if db_results:
            return True, db_results
        api_key = OPEN_WEATHER_API_KEY
        url = f"{OPEN_WEATHER_FORECAST_API_URL}?lat={lat}&lon={lng}&appid={api_key}"
        resp = requests.get(url)
        success, formatted_data = self.format_forecast(resp)
        if success:
            await insert_forecast_data(formatted_data)
        today = (
            (datetime.now(tz=timezone.utc) + timedelta(minutes=timezone_offset))
            .date()
            .isoformat()
        )
        formatted_data["prediction_data"] = {
            today: formatted_data["prediction_data"][today]
        }
        return success, formatted_data

    async def fetch_weather(self, lat, lng):
        db_results = await fetch_weather_data(lat, lng, SOURCE_NAME)
        if db_results:
            return True, db_results
        api_key = OPEN_WEATHER_API_KEY
        url = f"{OPEN_WEATHER_WEATHER_API_URL}?lat={lat}&lon={lng}&appid={api_key}"
        resp = requests.get(url)
        success, formatted_data = self.format_weather(resp)
        if success:
            await insert_weather_data(formatted_data)
        return success, formatted_data

    def _format_single_object(self, weather_object):
        formatted_response = {
            "current_temp": round(weather_object["main"]["temp"] / 10, 3),
            "humidity": round(weather_object["main"]["humidity"], 3),
            "condition_text": weather_object["weather"][0]["description"].capitalize(),
            "icon": f"{OPEN_WEATHER_ICON_URL}{weather_object['weather'][0]['icon']}@4x.png",
        }
        return formatted_response

    def _calculate_hours_prediction(self, response_data):
        """
        Open weather API returns forecast for each 3 hours
        since the contract expects a whole 5 days
        we're filling gaps (missing hours) by the previous hour values
        """
        prediction_data = {}
        hours_data = response_data["list"]
        hours_difference = 3
        current_date = (
            datetime.fromtimestamp(hours_data[0]["dt"], tz=timezone.utc)
            .date()
            .isoformat()
        )
        current_list = []
        hours_length = len(hours_data) - 1
        for index, hour in enumerate(hours_data):
            hour_object = self._format_single_object(hour)
            unix_timestamp = hour["dt"]
            hour_object["date"] = datetime.fromtimestamp(
                unix_timestamp, tz=timezone.utc
            )
            current_list.append(hour_object)
            for i in range(1, hours_difference):
                gap_obj = copy.copy(hour_object)
                gap_obj["date"] = datetime.fromtimestamp(
                    unix_timestamp + (3600 * i), tz=timezone.utc
                )
                current_list.append(gap_obj)

            if index < hours_length:
                next_date = (
                    datetime.fromtimestamp(hours_data[index + 1]["dt"], tz=timezone.utc)
                    .date()
                    .isoformat()
                )
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
            "city": data["city"]["name"],
            "location": {
                "type": "Point",
                "coordinates": [
                    data["city"]["coord"]["lat"],
                    data["city"]["coord"]["lon"],
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
        formatted_data = self._format_single_object(data)
        formatted_data["city"] = data["name"]
        formatted_data["source"] = SOURCE_NAME
        formatted_data["location"] = {
            "type": "Point",
            "coordinates": [data["coord"]["lat"], data["coord"]["lon"]],
        }
        return True, formatted_data
