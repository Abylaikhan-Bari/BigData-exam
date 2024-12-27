import dask.dataframe as dd

# Указываем путь к файлу retail_sales_dataset
retail_sales_path = "datasets/retail_sales_dataset.csv"

def process_dataset(file_path, date_column, amount_column):
    """
    Обрабатывает набор данных для группировки по годам и вычисления суммы продаж.
    :param file_path: Путь к файлу
    :param date_column: Название столбца с датами
    :param amount_column: Название столбца с суммой продаж
    :return: Результат группировки или сообщение об ошибке
    """
    try:
        # Загрузка данных с указанием минимальных типов для экономии памяти
        df = dd.read_csv(file_path, dtype={amount_column: 'float64'})

        # Проверяем наличие необходимых столбцов
        if date_column not in df.columns or amount_column not in df.columns:
            return f"Ошибка: отсутствуют столбцы '{date_column}' или '{amount_column}'"

        # Преобразуем столбец даты в формат datetime
        df[date_column] = dd.to_datetime(df[date_column], errors='coerce')

        # Проверяем, есть ли некорректные значения в столбце дат
        if df[date_column].isna().sum().compute() > 0:
            return f"Ошибка: есть некорректные значения в столбце '{date_column}'"

        # Извлекаем год из даты
        df['Year'] = df[date_column].dt.year

        # Группируем данные по году и вычисляем сумму продаж
        result = df.groupby('Year')[amount_column].sum().compute()
        return result
    except Exception as e:
        return f"Ошибка при обработке файла {file_path}: {e}"

# Обработка данных из retail_sales_dataset
retail_result = process_dataset(retail_sales_path, 'Date', 'Total Amount')

# Вывод результатов
print("Результаты для retail_sales_dataset:")
print(retail_result)
