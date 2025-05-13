import pandas as pd

SAMPLE_DATA_HEADER = "\nSample data:"


# Read the data files
print("Reading orders data...")
orders_df = pd.read_excel('data/orders_apr_2025.xlsx')
print("\nOrders DataFrame Structure:")
print(f"Shape: {orders_df.shape}")
print(f"Columns: {orders_df.columns.tolist()}")
print(SAMPLE_DATA_HEADER)
print(orders_df.head().to_string())

print("\n" + "="*80 + "\n")

print("Reading order items data...")
order_items_df = pd.read_excel('data/order_items_apr_2025.xlsx')
print("\nOrder Items DataFrame Structure:")
print(f"Shape: {order_items_df.shape}")
print(f"Columns: {order_items_df.columns.tolist()}")
print(SAMPLE_DATA_HEADER)
print(order_items_df.head().to_string())

print("\n" + "="*80 + "\n")

print("Reading products data...")
products_df = pd.read_csv('data/products.csv')
print("\nProducts DataFrame Structure:")
print(f"Shape: {products_df.shape}")
print(f"Columns: {products_df.columns.tolist()}")
print(SAMPLE_DATA_HEADER)
print(products_df.head().to_string())
