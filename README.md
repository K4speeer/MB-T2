# MB-T2
MB Task-2

Solved using custom dataframes to represent task conditions in Google Colab.

The function ```prods_to_cats(products_df, categories_df, relation_df)``` 
is implemented in a *lazy* manner by adding a column of data to the left 
of a base dataframe that shows the relationship between products and categories (if it exists),
representing the product name and category name respectively.
