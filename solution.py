from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def get_products_with_categories_and_uncategorized(
    products_df: DataFrame,
    categories_df: DataFrame,
    product_category_df: DataFrame
) -> DataFrame:
    """
    Возвращает датафрейм, содержащий все пары «Имя продукта – Имя категории»
    имена всех продуктов, у которых нет категорий.

    :param products_df: DataFrame продуктов (id, name_product).
    :param categories_df: DataFrame категорий (id, name_category).
    :param product_category_df: DataFrame связей (product_id, category_id).
    :return: DataFrame с колонками 'Product_Name', 'Category_Name'.
    """
    
    # 1. Объединение продуктов и связей (Left Join): 
    #   Сохраняет все продукты, даже без связей.
    products_with_links = products_df.alias("p").join(
        product_category_df.alias("pc"),
        F.col("p.id") == F.col("pc.product_id"),
        "left"
    ).select(
        F.col("p.name_product").alias("Product_Name"),
        F.col("pc.category_id").alias("Category_ID") 
    )
    result_df = products_with_links.alias("pl").join(
        categories_df.alias("c"),
        F.col("pl.Category_ID") == F.col("c.id"),
        "left"
    ).select(
        F.col("pl.Product_Name"),
        F.col("c.name_category").alias("Category_Name")
    )
    


    return result_df
