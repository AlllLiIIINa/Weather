import asyncio
import json
import logging
import os
import platform
from typing import List, Dict, Optional, Tuple
import aiohttp
from celery import Celery
from fuzzywuzzy import fuzz
from geonamescache import GeonamesCache
from googletrans import Translator

# Celery configuration
celery = Celery('app',
                broker='redis://localhost:6379/0',
                backend='redis://localhost:6379/1',
                broker_connection_retry_on_startup=True)

# Custom configuration for Windows
if platform.system() == 'Windows':
    celery.conf.update(
        worker_pool='solo',
        broker_connection_retry=True,
    )

# General Celery settings
celery.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)


# Configuration class to store API keys and constants
class Config:
    OPEN_WEATHER_API_KEY = '026d6338733e963e4889dc018571ec87'
    WEATHER_API_KEY = 'a9a9b829defc40e8877145331251302'
    RESULTS_DIR = 'weather_data'
    BASE_URL = "http://api.openweathermap.org/data/2.5"
    TEMPERATURE_RANGE = (-50, 50)
    VALID_REGIONS = ['Europe', 'Asia', 'America', 'Africa', 'Oceania']


# Class for processing geographic data
class GeographicProcessor:
    def __init__(self):
        self.gc = GeonamesCache()  # Initialize GeonamesCache for city and country data
        self.translator = Translator()  # Google Translator for language conversion
        self._initialize_data()

    def _initialize_data(self):
        # Load city and country data
        self.cities = self.gc.get_cities()
        self.countries = self.gc.get_countries()

        self.city_names = {}
        for city_id, city_data in self.cities.items():
            city_name = city_data['name'].lower()
            if city_name not in self.city_names:
                self.city_names[city_name] = []
            self.city_names[city_name].append(city_data)

        self.continent_mapping = {
            'AF': 'Africa',
            'AS': 'Asia',
            'EU': 'Europe',
            'NA': 'America',
            'SA': 'America',
            'OC': 'Oceania'
        }

    # Translate a city name to English if necessary
    def _translate_to_english(self, city: str) -> str:
        try:
            if all(ord(char) < 128 for char in city):
                return city
            translation = self.translator.translate(city, dest='en')
            return translation.text
        except Exception as e:
            logging.warning(f"Translation failed for {city}: {str(e)}")
            return city

    # Find the best matching city in the database using fuzzy matching
    def _find_best_city_match(self, city_name: str) -> Tuple[Optional[Dict], int]:
        city_lower = city_name.lower()
        best_match = None
        best_score = 0

        if city_lower in self.city_names:
            best_match = max(self.city_names[city_lower],
                             key=lambda x: x['population'])
            return best_match, 100

        for db_city_name in self.city_names.keys():
            score = fuzz.ratio(city_lower, db_city_name)
            if score > best_score and score > 80:
                best_match = max(self.city_names[db_city_name],
                                 key=lambda x: x['population'])
                best_score = score

        return best_match, best_score

    # Determine the region of a country based on its code
    def get_region(self, country_code: str) -> str:
        try:
            country_data = self.countries[country_code]
            continent_code = country_data['continentcode']
            return self.continent_mapping.get(continent_code, 'Unknown')
        except KeyError:
            return 'Unknown'

    def process_city(self, city: str) -> Optional[Dict]:
        try:
            translated_name = self._translate_to_english(city)
            city_data, score = self._find_best_city_match(translated_name)

            if city_data and score > 80:
                country_code = city_data['countrycode']
                region = self.get_region(country_code)

                return {
                    'original_name': city,
                    'standardized_name': city_data['name'],
                    'country_code': country_code,
                    'region': region,
                }

            logging.warning(f"No good match found for city: {city}")
            return None

        except Exception as e:
            logging.error(f"Error processing city {city}: {str(e)}")
            return None


# Class for interacting with the OpenWeather API
class WeatherAPI:
    def __init__(self):
        self.session = None

    async def _init_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()

    def _validate_weather_data(self, data: Dict) -> bool:
        try:
            # Validate temperature is a number
            if not isinstance(data['main']['temp'], (int, float)):
                logging.error("Temperature is not a number")
                return False

            # Validate temperature range
            temperature = float(data['main']['temp'])
            if not Config.TEMPERATURE_RANGE[0] <= temperature <= Config.TEMPERATURE_RANGE[1]:
                logging.error(f"Temperature {temperature}°C outside valid range")
                return False

            # Validate weather description
            if not isinstance(data['weather'][0]['description'], str):
                logging.error("Weather description is not a string")
                return False

            if not data['weather'][0]['description'].strip():
                logging.error("Weather description is empty")
                return False

            return True
        except Exception as e:
            logging.error(f"Error validating weather data: {str(e)}")
            return False

    async def _fetch_weather_api(self, city: str) -> Optional[Dict]:
        try:
            async with self.session.get(
                    "http://api.weatherapi.com/v1/current.json",
                    params={
                        'key': Config.WEATHER_API_KEY,
                        'q': city,
                    }
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        'main': {
                            'temp': data['current']['temp_c']
                        },
                        'weather': [{
                            'description': data['current']['condition']['text'].lower()
                        }]
                    }
                else:
                    logging.error(f"Error fetching weather from WeatherAPI for {city}: {response.status}")
                    return None
        except Exception as e:
            logging.error(f"Error fetching weather from WeatherAPI for {city}: {str(e)}")
            return None

    async def _fetch_openweather(self, city: str) -> Optional[Dict]:
        try:
            async with self.session.get(
                    f"{Config.BASE_URL}/weather",
                    params={
                        'q': city,
                        'units': 'metric',
                        'APPID': Config.OPEN_WEATHER_API_KEY
                    }
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logging.error(f"Error fetching weather from OpenWeather for {city}: {response.status}")
                    return None
        except Exception as e:
            logging.error(f"Error fetching weather from OpenWeather for {city}: {str(e)}")
            return None

    async def fetch_weather(self, city: str) -> Optional[Dict]:
        await self._init_session()

        # Try OpenWeather first
        data = await self._fetch_openweather(city)

        # If OpenWeather fails, try WeatherAPI
        if data is None or not self._validate_weather_data(data):
            logging.info(f"Falling back to WeatherAPI for {city}")
            data = await self._fetch_weather_api(city)

        # Validate and process the data
        if data and self._validate_weather_data(data):
            return self._process_weather_data(data)

        return None

    async def close(self):
        if self.session:
            await self.session.close()

    def _process_weather_data(self, data: Dict) -> Optional[Dict]:
        try:
            temperature = float(data['main']['temp'])
            return {
                'temperature': f"{temperature}°C",
                'description': data['weather'][0]['description']
            }
        except Exception as e:
            logging.error(f"Error processing weather data: {str(e)}")
            return None


# Celery task for processing weather data
@celery.task(bind=True, name='app.process_weather_data')
def process_weather_data(self, cities: List[str]) -> Dict:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(_process_weather_data(cities, self.request.id))


async def _process_weather_data(cities: List[str], task_id: str) -> Dict:
    try:
        weather_api = WeatherAPI()
        geo_processor = GeographicProcessor()
        results = {region: [] for region in Config.VALID_REGIONS}

        async def process_city(city: str):
            city_info = geo_processor.process_city(city)
            if not city_info:
                logging.warning(f"Could not process city: {city}")
                return None

            city_query = f"{city_info['standardized_name']},{city_info['country_code']}"
            weather_data = await weather_api.fetch_weather(city_query)

            if weather_data:
                return {
                    'region': city_info['region'],
                    'city': city_info['standardized_name'],
                    'temperature': weather_data['temperature'],
                    'description': weather_data['description']
                }
            return None

        tasks = [process_city(city) for city in cities]
        city_results = await asyncio.gather(*tasks)

        # Group results by region and update existing files
        for result in city_results:
            if result and result['region'] in results:
                region = result['region']
                city_data = {
                    'city': result['city'],
                    'temperature': result['temperature'],
                    'description': result['description']
                }

                # Create directory for region if it doesn't exist
                directory = os.path.join(Config.RESULTS_DIR, region)
                os.makedirs(directory, exist_ok=True)

                filename = f'task_{task_id}.json'
                filepath = os.path.join(directory, filename)

                existing_data = []
                if os.path.exists(filepath):
                    with open(filepath, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)

                # Update or add city data
                city_updated = False
                for i, item in enumerate(existing_data):
                    if item['city'] == city_data['city']:
                        existing_data[i] = city_data
                        city_updated = True
                        break

                if not city_updated:
                    existing_data.append(city_data)

                # Save updated data
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(existing_data, f, indent=2, ensure_ascii=False)

                results[region].append(city_data)

        await weather_api.close()
        return {'status': 'completed'}
    except Exception as e:
        logging.error(f"Task failed: {str(e)}")
        return {'status': 'failed', 'error': str(e)}
