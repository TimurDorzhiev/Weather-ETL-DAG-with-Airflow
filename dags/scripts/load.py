from sqlalchemy import create_engine
import pandas as pd

# Подключение к базе данных
DATABASE_URL = 'postgresql:// airflow: airflow@postgres/airflow'
engine = create_engine(DATABASE_URL)


def load_data(df, engine):
    try:
        # Загружаем данные в таблицу 'weather_data'
        df.to_sql('weather_data', con=engine, if_exists='append',
                  index=False, chunksize=1000)
        print("Данные успешно загружены в базу данных")
    except Exception as e:
        # Обработка ошибок
        print(f"Ошибка при загрузке данных в базу данных: {str(e)}")
