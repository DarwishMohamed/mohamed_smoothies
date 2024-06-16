import pandas as pd
from datetime import datetime
import hashlib

# Sample data, ideally fetched from the Snowflake table
data = [
    {'ORDER_UID': 216, 'ORDER_FILLED': False, 'NAME_ON_ORDER': 'Kevin', 'INGREDIENTS': 'Apples Lime Ximenia', 'ORDER_TS': '2024-06-16 10:07:44.742 -0700'},
    {'ORDER_UID': 217, 'ORDER_FILLED': True, 'NAME_ON_ORDER': 'Divya', 'INGREDIENTS': 'Dragon Fruit Guava Figs Jackfruit Blueberries', 'ORDER_TS': '2024-06-16 10:07:49.565 -0700'},
    {'ORDER_UID': 218, 'ORDER_FILLED': True, 'NAME_ON_ORDER': 'Xi', 'INGREDIENTS': 'Vanilla Fruit Nectarine', 'ORDER_TS': '2024-06-16 10:07:53.461 -0700'}
]

# Create DataFrame
df = pd.DataFrame(data)

# Convert ORDER_TS to datetime
df['ORDER_TS'] = pd.to_datetime(df['ORDER_TS'], format='%Y-%m-%d %H:%M:%S.%f %z')

# Ensure ingredients are properly formatted (trim whitespace, consistent case, etc.)
df['INGREDIENTS'] = df['INGREDIENTS'].str.strip().str.lower()

# Function to compute hash for ingredients
def compute_hash(ingredients):
    return int(hashlib.md5(ingredients.encode()).hexdigest(), 16)

# Apply the hash function
df['HASH'] = df['INGREDIENTS'].apply(compute_hash)

# Display the DataFrame
print(df[['ORDER_UID', 'ORDER_FILLED', 'NAME_ON_ORDER', 'INGREDIENTS', 'ORDER_TS', 'HASH']])

# Compute the total hash value
total_hash_value = df['HASH'].sum()
print("Total hash value:", total_hash_value)
