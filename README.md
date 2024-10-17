# HOW IS THE SKY
Briefly, we're fetching weather/forecast data from two main sources [Open Weather Map](https://openweathermap.org/). [Open Meteo](https://open-meteo.com/)

## Stack:
Programming Language: Python3.10
Database: MongoDB
APIs: Python FastAPI + Uvicorn

## HOW to Run
1- make sure to load the required environment variables in .env file (you can find the required keys in [constants.py](/constants.py) file)

2- `pip install -r requirements.txt`

3- `uvicorn main:app --reload`

That's it

## Tech notes:
- This is app was built to simulate having multiple data sources with different request/response structure, so we used [Strategy Pattern]() so each service has it's own way of fetching the data and [Adapter Pattern]() to format different responses into unified format.
- Weather data are being stored for one hour (cached in the database) then they will expire and user will get fresh weather data after that
- We're using special indexes for data expiration and geolocation search and the app makes sure they can be created in the database
