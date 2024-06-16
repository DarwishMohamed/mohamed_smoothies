# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col
import requests
import pandas as pd

# Write directly to the app
st.title("Customize Your Smoothie 🍹")
st.write("E5tar el fakha el enta 3ayezha w engez mat2refnash")

# Get the Snowflake session
cnx = st.connection("snowflake")
session = cnx.session()

# Fetch the data from the fruit_options table
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'), col('SEARCH_ON'))

# Convert the Snowpark DataFrame to a Pandas DataFrame
pd_df = my_dataframe.to_pandas()

# Display the Pandas DataFrame
st.dataframe(pd_df)

# Function to truncate orders table
def truncate_orders():
    session.sql("TRUNCATE TABLE smoothies.public.orders").collect()

# Button to truncate orders table
if st.button('Truncate Orders Table'):
    truncate_orders()
    st.success('Orders table truncated!', icon="✅")

# Additional script to mark orders as filled
def mark_order_filled(name_on_order):
    mark_filled_stmt = f"""
    UPDATE smoothies.public.orders
    SET order_filled = TRUE
    WHERE name_on_order = '{name_on_order}'
    """
    session.sql(mark_filled_stmt).collect()

# Function to create orders as specified
def create_order(name_on_order, ingredients, fill_order=False):
    ingredients_string = ' '.join(ingredients)
    my_insert_stmt = f"""
    INSERT INTO smoothies.public.orders (ingredients, name_on_order, order_filled)
    VALUES ('{ingredients_string}', '{name_on_order}', {'TRUE' if fill_order else 'FALSE'})
    """
    session.sql(my_insert_stmt).collect()
    st.write(f"Order: {name_on_order}, Filled: {'True' if fill_order else 'False'}, Ingredients: {ingredients_string}, Hash: {hash(ingredients_string)}")

# Creating orders according to the challenge lab directions
if st.button('Create Orders for Divya and Xi'):
    create_order('Divya', ['Dragon Fruit', 'Guava', 'Figs', 'Jackfruit', 'Blueberries'], fill_order=True)
    create_order('Xi', ['Vanilla Fruit', 'Nectarine'], fill_order=True)
    st.success('Orders for Divya and Xi have been created and marked as required!', icon="✅")

# Verify the hash values for DORA Check
def verify_hash_values():
    query = """
    SELECT SUM(hash_ing) AS total_hash_value FROM (
        SELECT HASH(ingredients) AS hash_ing
        FROM smoothies.public.orders
        WHERE order_ts IS NOT NULL 
        AND name_on_order IS NOT NULL 
        AND (
            (name_on_order = 'Kevin' AND order_filled = FALSE AND HASH(ingredients) = 7976616299844859825) 
            OR (name_on_order ='Divya' AND order_filled = TRUE AND HASH(ingredients) = -6112358379204300652)
            OR (name_on_order ='Xi' AND order_filled = TRUE AND HASH(ingredients) = 1016924841131818535)
        )
    )
    """
    result = session.sql(query).collect()
    total_hash_value = result[0]['TOTAL_HASH_VALUE']
    st.write(f"Total hash value: {total_hash_value}")

    expected_hash_value = 2881182761772377708
    if total_hash_value == expected_hash_value:
        st.success('Hash values verified!', icon="✅")
    else:
        st.error('Hash values did not match.', icon="❌")

# Button to verify hash values
if st.button('Verify Hash Values for DORA Check'):
    verify_hash_values()
