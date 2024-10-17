from typing import Optional

from fastapi import APIRouter, HTTPException


from .open_weather import OpenWeatherDataFetcher
from .open_meteo import OpenMeteoDataFetcher

router = APIRouter()


@router.get("/weather/", tags=["weather"])
@router.get("/weather/{type}/", tags=["weather"])
async def get_weather_data(
    lat: float,
    lng: float,
    type: Optional[str] = "openweathermap",
):
    if not lat or not lng:
        raise HTTPException(status_code=404, detail="Lat or Lng values not set")
    fetcher = None
    match type:
        case "openweathermap":
            fetcher = OpenWeatherDataFetcher()
        case "openmeteo":
            fetcher = OpenMeteoDataFetcher()

    success, data = await fetcher.fetch_weather(lat, lng)
    return {"error": data} if not success else data


@router.get("/forecast/", tags=["forecast"])
@router.get("/forecast/{type}/", tags=["forecast"])
async def get_forecast_data(
    lat: float,
    lng: float,
    type: Optional[str] = "openweathermap",
    timezone: Optional[int] = 0,
):
    if not lat or not lng:
        raise HTTPException(status_code=404, detail="Lat or Lng values not set")
    fetcher = None
    match type:
        case "openweathermap":
            fetcher = OpenWeatherDataFetcher()
        case "openmeteo":
            fetcher = OpenMeteoDataFetcher()
    success, data = await fetcher.fetch_forecast(lat, lng, timezone)
    return {"error": data} if not success else data

