from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.extract import fetch_weather_data  # Импорт функции извлечения
from scripts.transform import transform_data    # Импорт функции обработки
from scripts.load import load_data              # Импорт функции загрузки
from sqlalchemy import create_engine
import pandas as pd

# Параметры подключения к базе данных
DATABASE_URL = 'postgresql://airflow:airflow@postgres/airflow'
engine = create_engine(DATABASE_URL)

# Параметры по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Определение DAG
with DAG(
    'weather_etl',
    default_args=default_args,
    description='ETL процесс для извлечения данных о погоде',
    schedule_interval='@daily',  # Запуск раз в день
    catchup=False
) as dag:

    # Задача извлечения данных
    def extract_task(**kwargs):
        data = fetch_weather_data()
        if not data:
            raise ValueError("Data extraction failed")
        kwargs['ti'].xcom_push(key='weather_data', value=data)

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_task,
        provide_context=True
    )

    # Задача обработки данных
    def transform_task(**kwargs):
        data = kwargs['ti'].xcom_pull(task_ids='extract', key='weather_data')
        # Ваш код преобразования данных
        df = transform_data(data)
        # Сериализация датафрейма в JSON и передача в XCom
        data_json = df.to_json()
        kwargs['ti'].xcom_push(key='weather_data', value=data_json)

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_task,
        provide_context=True
    )

    # Задача загрузки данных
    def load_task(**kwargs):
        # Извлекаем данные из XCom
        data_json = kwargs['ti'].xcom_pull(
            task_ids='transform', key='weather_data')
        # Восстановление датафрейма из JSON
        df = pd.read_json(data_json)
        # Ваш код загрузки данных в базу данных
        load_data(df, engine)

    load = PythonOperator(
        task_id='load',
        python_callable=load_task,
        provide_context=True
    )

    # Определяем порядок выполнения
    extract >> transform >> load
