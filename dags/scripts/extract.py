import os
from dotenv import load_dotenv
import requests
import pandas as pd

# Загрузить переменные окружения из .env файла

load_dotenv()

API_KEY = os.getenv('OPENWEATHER_API_KEY')
CITY = 'Moscow'
URL = f'http://api.openweathermap.org/data/2.5/weather?q={CITY}' \
    f'&appid={API_KEY}&units=metric'


def fetch_weather_data():
    try:
        # Отправляем GET-запрос к API
        response = requests.get(URL)

        # Проверяем, что запрос успешен (код 200)
        if response.status_code == 200:
            data = response.json()

            # Пытаемся извлечь необходимые данные
            weather_data = {
                'city': data.get('name', 'Unknown'),
                'temperature': data['main'].get('temp', None),
                'humidity': data['main'].get('humidity', None),
                'weather': data['weather'][0].get('description', 'No description')
            }

            # Проверка на наличие необходимых данных
            if weather_data['temperature'] is None or weather_data['humidity'] is None:
                raise ValueError(
                    "Не удалось извлечь все необходимые данные из ответа API")

            return weather_data
        else:
            # Если статус код не 200, выводим ошибку
            raise Exception(
                f"Ошибка при получении данных: {response.status_code}"
            )

    except requests.exceptions.RequestException as e:
        # Обрабатываем ошибки, связанные с запросом
        raise Exception(f"Ошибка при выполнении запроса: {str(e)}")
    except Exception as e:
        # Обрабатываем другие возможные ошибки
        raise Exception(f"Произошла ошибка: {str(e)}")
