# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col
import requests
import pandas as pd

# Write directly to the app
st.title("Customize Your Smoothie 🍹")
st.write("E5tar el fakha el enta 3ayezha w engez mat2refnash")

# Input for name on order
name_on_order = st.text_input('Name on Order', '')

# Get the Snowflake session
cnx = st.connection("snowflake")
session = cnx.session()

# Fetch the data from the fruit_options table
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'), col('SEARCH_ON'))

# Convert the Snowpark DataFrame to a Pandas DataFrame
pd_df = my_dataframe.to_pandas()

# Display the Pandas DataFrame
st.dataframe(pd_df)

# Use the Pandas DataFrame for the multiselect
ingredients_list = st.multiselect(
    'Choose up to 5 ingredients',
    pd_df['FRUIT_NAME']
)

if ingredients_list:
    ingredients_string = ' '.join(ingredients_list)

    for fruit_chosen in ingredients_list:
        st.subheader(fruit_chosen + ' Nutrition Information')
        fruityvice_response = requests.get("https://fruityvice.com/api/fruit/" + fruit_chosen)
        fv_df = st.dataframe(data=fruityvice_response.json(), use_container_width=True)
        search_on = pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'].iloc[0]
        st.write('The search value for ', fruit_chosen, ' is ', search_on, '.')

    # Display the SQL insert statement
    my_insert_stmt = f"""
    INSERT INTO smoothies.public.orders (ingredients, name_on_order, order_filled)
    VALUES ('{ingredients_string}', '{name_on_order}', FALSE)
    """
    st.write(my_insert_stmt)

    # Display the submit button
    time_to_insert = st.button('Submit Order')

    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        st.success(f'Your Smoothie is ordered, {name_on_order}!', icon="✅")

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
    st.success(f'Order for {name_on_order} created!', icon="✅")

# Function to truncate orders table
def truncate_orders():
    session.sql("TRUNCATE TABLE smoothies.public.orders").collect()

# Button to truncate orders table
if st.button('Truncate Orders Table'):
    truncate_orders()
    st.success('Orders table truncated!', icon="✅")

# Creating orders according to the challenge lab directions
if st.button('Create Orders for DORA Check'):
    truncate_orders()  # Start fresh
    create_order('Kevin', ['Apples', 'Lime', 'Ximenia'], fill_order=False)
    create_order('Divya', ['Dragon Fruit', 'Guava', 'Figs', 'Jackfruit', 'Blueberries'], fill_order=True)
    create_order('Xi', ['Vanilla Fruit', 'Nectarine'], fill_order=True)
    st.success('Orders for Kevin, Divya, and Xi have been created and marked as required!', icon="✅")

# Function to verify hash values
def verify_hash_values():
    hash_query = """
    SELECT SUM(hash_ing) as total_hash_value
    FROM (
        SELECT hash(ingredients) as hash_ing
        FROM smoothies.public.orders
        WHERE order_ts IS NOT NULL 
        AND name_on_order IS NOT NULL 
        AND (
            (name_on_order = 'Kevin' AND order_filled = FALSE AND hash(ingredients) = 7976616299844859825) 
            OR (name_on_order = 'Divya' AND order_filled = TRUE AND hash(ingredients) = -6112358379204300652)
            OR (name_on_order = 'Xi' AND order_filled = TRUE AND hash(ingredients) = 1016924841131818535)
        )
    )
    """
    result = session.sql(hash_query).collect()
    total_hash_value = result[0]['TOTAL_HASH_VALUE']
    expected_hash_value = 2881182761772377708

    if total_hash_value == expected_hash_value:
        st.success("Hash values verified!", icon="✅")
    else:
        st.error("Hash values did not match.")
    st.write(f"Total hash value: {total_hash_value}")

# Button to verify hash values
if st.button('Verify Hash Values for DORA Check'):
    verify_hash_values()
