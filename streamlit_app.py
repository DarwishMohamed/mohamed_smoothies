import streamlit as st
from snowflake.snowpark.functions import col
import pandas as pd

# Write directly to the app
st.title("Customize Your Smoothie 🍹")
st.write("E5tar el fakha el enta 3ayezha w engez mat2refnash")

# Get the Snowflake session
cnx = st.connection("snowflake")
session = cnx.session()

# Function to truncate orders table
def truncate_orders():
    session.sql("TRUNCATE TABLE smoothies.public.orders").collect()

# Button to truncate orders table
if st.button('Truncate Orders Table'):
    truncate_orders()
    st.success('Orders table truncated!', icon="✅")

# Function to create orders as specified
def create_order(name_on_order, ingredients, fill_order=False):
    ingredients_string = ' '.join(ingredients).strip().lower()
    my_insert_stmt = f"""
    INSERT INTO smoothies.public.orders (ingredients, name_on_order, order_filled)
    VALUES ('{ingredients_string}', '{name_on_order}', {'TRUE' if fill_order else 'FALSE'})
    """
    session.sql(my_insert_stmt).collect()
    st.success(f'Order for {name_on_order} created!', icon="✅")

# Creating orders according to the challenge lab directions
if st.button('Create Orders for DORA Check'):
    truncate_orders()  # Start fresh
    create_order('Kevin', ['Apples', 'Lime', 'Ximenia'], fill_order=False)
    create_order('Divya', ['Dragon Fruit', 'Guava', 'Figs', 'Jackfruit', 'Blueberries'], fill_order=True)
    create_order('Xi', ['Vanilla Fruit', 'Nectarine'], fill_order=True)
    st.success('Orders for Kevin, Divya, and Xi have been created and marked as required!', icon="✅")

# Verify the hash values for DORA Check
def verify_hash_values():
    query = """
    SELECT name_on_order, order_filled, ingredients, HASH(ingredients) AS hash_ing
    FROM smoothies.public.orders
    WHERE name_on_order IN ('Kevin', 'Divya', 'Xi')
    """
    result = session.sql(query).to_pandas()
    
    # Check formatting and display results
    for index, row in result.iterrows():
        st.write(f"Order: {row['NAME_ON_ORDER']}, Filled: {row['ORDER_FILLED']}, Ingredients: {row['INGREDIENTS']}, Hash: {row['HASH_ING']}")
    
    # Calculate total hash value
    total_hash_value = result['HASH_ING'].sum()
    st.write(f"Total hash value: {total_hash_value}")
    
    expected_hash_value = 2881182761772377708
    if total_hash_value == expected_hash_value:
        st.success('Hash values verified!', icon="✅")
    else:
        st.error('Hash values did not match.', icon="❌")

# Button to verify hash values
if st.button('Verify Hash Values for DORA Check'):
    verify_hash_values()
