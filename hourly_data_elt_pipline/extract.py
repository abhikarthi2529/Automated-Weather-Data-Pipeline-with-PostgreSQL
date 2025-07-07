import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

class ExtractWeatherData:
    def __init__(self):
        cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
        retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
        self.openmeteo = openmeteo_requests.Client(session = retry_session)
    
    def get_start_end_times(self):
        end_time= pd.Timestamp.now() - pd.Timedelta(days = 1)
        start_time = end_time - pd.Timedelta(days=2)
        start_time = start_time.strftime('%Y-%m-%dT%H:%M')
        end_time = end_time.strftime('%Y-%m-%dT%H:%M')
        
        return {'start_time':start_time, 'end_time':end_time}
        

    def get_request(self, start_end_times_dict):
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": 49.8863,
            "longitude": 119.4966,
            "start_hour" : start_end_times_dict['start_time'],
            "end_hour": start_end_times_dict['end_time'],
            "hourly": "temperature_2m",
            'temperature_unit': 'celsius'
        }
        responses = self.openmeteo.weather_api(url, params=params)
        return responses[0]


    def process_hourly_data(self, response):
        
        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

        hourly_data = {"date": pd.date_range(
            start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
            end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = hourly.Interval()),
            inclusive = "left"
        )}

        hourly_data["temperature_2m"] = hourly_temperature_2m

        hourly_dataframe = pd.DataFrame(data = hourly_data)
        return hourly_dataframe
