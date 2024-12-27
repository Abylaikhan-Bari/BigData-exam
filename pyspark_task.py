from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

# Создаем сессию Spark
spark = SparkSession.builder.appName("SalesShareCalculation").getOrCreate()

# Указываем путь к файлу retail_sales_dataset
retail_sales_path = "datasets/retail_sales_dataset.csv"

# Загружаем данные из файла
df = spark.read.csv(retail_sales_path, header=True, inferSchema=True)

# Проверка доступных столбцов
print(f"Доступные столбцы: {df.columns}")

# Обновляем список требуемых столбцов
required_columns = ["Product Category", "Total Amount"]
missing_columns = [col for col in required_columns if col not in df.columns]

if missing_columns:
    print(f"Отсутствуют необходимые столбцы: {missing_columns}")
else:
    # Преобразуем столбец "Total Amount" к типу float
    df = df.withColumn("Total Amount", col("Total Amount").cast("double"))

    # Вычисляем общую сумму продаж
    total_sales = df.agg(_sum("Total Amount").alias("Total Sales")).collect()[0]["Total Sales"]

    # Добавляем новый столбец с долей суммы продаж
    df_with_share = df.withColumn("Sales Share", col("Total Amount") / total_sales)

    # Группируем данные по категории товаров и суммируем продажи
    category_sales = df.groupBy("Product Category").agg(
        _sum("Total Amount").alias("Category Sales"),
        (_sum("Total Amount") / total_sales).alias("Category Share")
    )

    # Выводим результат
    category_sales.show()
