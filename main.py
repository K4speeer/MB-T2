from os import path as p
from pyspark.sql import SparkSession, functions as F

data_dir_path = p.join(p.dirname(__file__), "data")

products_csv = p.join(data_dir_path, 'products.csv')
categories_csv = p.join(data_dir_path, 'categories.csv')
relation_csv = p.join(data_dir_path, 'products_category.csv')

spark = SparkSession.builder.appName("ProductCategories").getOrCreate()

products_df = spark.read.csv(products_csv, header=True, inferSchema=True)
categories_df = spark.read.csv(categories_csv, header=True, inferSchema=True)
relations_df = spark.read.csv(relation_csv, header=True, inferSchema=True)

# Optional: View uploaded data files as dataframes
 
# print("Products:")
# products_df.show()

# print("Categories:")
# categories_df.show()

# print("Product-Category Relationships:")
# relations_df.show()

def prods_to_cats(products_df, categories_df, relation_df):
  """
  Analyze and cretes a dataframe of pairs Product - Category depending on Relation between them

  Args:
    products_df : Products dataframe
    categories_df : Catigories dataframe
    relation_df : The relationship between categories and products

  Returns:
    DataFrame with pairs of [Product_name - Category_name] with duplicates if the product belongs to many categories
    in addition to all products that don't belong to any category

  """

  # Adding a column that places product_name beside every product_id
  joined_df = relation_df.join(products_df, on='product_id', how='left').select('category_id', "product_name")
  # Adding a column that places category_name beside every category_id
  output_df = joined_df.join(categories_df, on='category_id', how='left').select('product_name', "category_name")
  # Replacing NULLs' with 'No Category' in category_name column
  # Returning last modified DataFrame
  return output_df.fillna('No Category', 'category_name')


analyzed_df = prods_to_cats(products_df, categories_df, relations_df)
analyzed_df.show()


spark.stop()