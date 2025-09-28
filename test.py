from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("TestTask").getOrCreate()


schema_products = StructType([
    StructField("id", IntegerType(), False),
    StructField("name_product", StringType(), False)
])
schema_categories = StructType([
    StructField("id", IntegerType(), False),
    StructField("name_category", StringType(), False)
])
schema_product_category = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("category_id", IntegerType(), False)
])

data_products = [
    (1, "Яблоко"),
    (2, "Молоко"),
    (3, "Огурец"),
    (4, "Сыр")
]
products_df_test = spark.createDataFrame(data_products, schema_products)


data_categories = [
    (101, "Фрукты"),
    (102, "Зеленые"),
    (103, "Молочные продукты"),
    (104, "Напитки")
]
categories_df_test = spark.createDataFrame(data_categories, schema_categories)


data_product_category = [
    (1, 101),
    (1, 102),
    (2, 103),
    (4, 103)
]
product_category_df_test = spark.createDataFrame(data_product_category, schema_product_category)


result_df_test = get_products_with_categories_and_uncategorized(
    products_df_test,
    categories_df_test,
    product_category_df_test
)


print("--- Исходный набор данных ---")
products_df_test.show()
categories_df_test.show()
product_category_df_test.show()

print("--- Результат выполнения метода ---")

result_df_test.sort("Product_Name", "Category_Name").show()


expected_data = [
    ("Молоко", "Молочные продукты"),  
    ("Огурец", None),                 
    ("Сыр", "Молочные продукты"),      
    ("Яблоко", "Фрукты"),             
    ("Яблоко", "Зеленые")
]

expected_schema = StructType([
    StructField("Product_Name", StringType(), False),  
    StructField("Category_Name", StringType(), True)  
])


expected_df = spark.createDataFrame(
    expected_data,
    schema=expected_schema  
).sort("Product_Name", "Category_Name")

def assert_dataframes_equal(df1, df2):
    assert df1.schema == df2.schema, "Схемы датафреймов не совпадают!"
    df1_sorted = df1.sort(*df1.columns)
    df2_sorted = df2.sort(*df2.columns)
    
    # Проверяем количество строк и сами строки
    assert df1_sorted.count() == df2_sorted.count(), "Количество строк не совпадает!"
    
    # Получаем результат как список кортежей и сравниваем их
    rows1 = df1_sorted.collect()
    rows2 = df2_sorted.collect()
    
    # Преобразуем Row в tuple для надежного сравнения
    rows1_tuple = [tuple(row) for row in rows1]
    rows2_tuple = [tuple(row) for row in rows2]

    assert rows1_tuple == rows2_tuple, "Содержимое датафреймов не совпадает!"
    print("✅ Тест пройден: Содержимое результата совпадает с ожидаемым!")

# Проверяем!
assert_dataframes_equal(result_df_test, expected_df)

# Остановка SparkSession
spark.stop()