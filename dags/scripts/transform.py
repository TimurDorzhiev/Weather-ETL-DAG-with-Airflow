import pandas as pd

def transform_data(data):
    """
    Преобразует данные о погоде в DataFrame для последующей загрузки.
    """
    # Проверяем, что все нужные данные присутствуют
    if not all(key in data for key in ['city', 'temperature', 'humidity', 'weather']):
        raise ValueError("Отсутствуют необходимые данные для преобразования в DataFrame")
    
    # Создаем DataFrame из входных данных
    df = pd.DataFrame([data], columns=['city', 'temperature', 'humidity', 'weather'])

    
    df['retrived_at'] = pd.Timestamp.now()
    return df
