# Weather ETL with Airflow

Этот проект представляет собой ETL-процесс для извлечения данных о погоде, их преобразования и загрузки в базу данных PostgreSQL с использованием Apache Airflow. Проект реализован с использованием Docker для простоты развертывания.

## Структура проекта

- `dags/`: Содержит DAG и необходимые скрипты для ETL.
- `docker-compose.yml`: Конфигурация для запуска Airflow и базы данных PostgreSQL в контейнерах.
- `requirements.txt`: Зависимости для Airflow и Python.

## Запуск проекта

1. Клонируйте репозиторий:

   ```bash
   git clone <URL_репозитория>
   cd my_airflow_project

   ```

2. Запустите контейнеры Docker:

   ```bash
   docker-compose up -d

   ```

3. Перейдите на веб-интерфейс Airflow по адресу http://localhost:8080 и включите DAG weather_etl.
