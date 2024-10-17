from abc import ABC, abstractmethod


class WeatherDataFetcher(ABC):

    @abstractmethod
    def fetch_weather(self):
        pass

    def fetch_forecast(self):
        pass

    @abstractmethod
    def format_weather(self, response_data):
        # TODO: write the contract expected output and try to strict it
        pass

    @abstractmethod
    def format_forecast(self, response_data):
        # TODO: write the contract expected output and try to strict it
        pass
