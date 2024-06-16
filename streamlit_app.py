# Import necessary packages
import streamlit as st
from snowflake.snowpark.functions import col
import requests
import pandas as pd

# Title of the app
st.title("Customize Your Smoothie 🍹")
st.write("E5tar el fakha el enta 3ayezha w engez mat2refnash")

# Input for name on order
name_on_order = st.text_input('Name on Order', '')

# Establish Snowflake session
cnx = st.connection("snowflake")
session = cnx.session()

# Fetch the fruit options data
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'), col('SEARCH_ON'))

# Convert Snowflake DataFrame to Pandas DataFrame
pd_df = my_dataframe.to_pandas()

# Display the Pandas DataFrame
st.dataframe(pd_df)

# Multiselect for ingredients
ingredients_list = st.multiselect('Choose up to 5 ingredients', pd_df['FRUIT_NAME'])

if ingredients_list:
    ingredients_string = ' '.join(ingredients_list)

    for fruit_chosen in ingredients_list:
        st.subheader(fruit_chosen + ' Nutrition Information')
        fruityvice_response = requests.get("https://fruityvice.com/api/fruit/" + fruit_chosen)
        fv_df = st.dataframe(data=fruityvice_response.json(), use_container_width=True)
        search_on = pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'].iloc[0]
        st.write('The search value for ', fruit_chosen, ' is ', search_on, '.')

    # SQL insert statement for the order
    my_insert_stmt = f"""
    INSERT INTO smoothies.public.orders (ingredients, name_on_order, order_filled)
    VALUES ('{ingredients_string}', '{name_on_order}', FALSE)
    """
    st.write(my_insert_stmt)

    # Button to submit order
    time_to_insert = st.button('Submit Order')
    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        st.success(f'Your Smoothie is ordered, {name_on_order}!', icon="✅")

# Function to create orders
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

# Button to create orders for DORA check
if st.button('Create Orders for DORA Check'):
    truncate_orders()  # Clean start
    create_order('Kevin', ['Apples', 'Lime', 'Ximenia'], fill_order=False)
    create_order('Divya', ['Dragon Fruit', 'Guava', 'Figs', 'Jackfruit', 'Blueberries'], fill_order=True)
    create_order('Xi', ['Vanilla Fruit', 'Nectarine'], fill_order=True)
    st.success('Orders for Kevin, Divya, and Xi have been created!', icon="✅")

# Function to verify hash values
def verify_hash_values():
    query = """
    SELECT name_on_order, order_filled, hash(ingredients) AS hash_ing
    FROM smoothies.public.orders
    WHERE name_on_order IN ('Kevin', 'Divya', 'Xi')
    """
    result = session.sql(query).collect()
    
    # Display the hash values for each order
    for row in result:
        st.write(f"Order: {row['NAME_ON_ORDER']}, Filled: {row['ORDER_FILLED']}, Ingredients: {row['HASH_ING']}")

    # Calculate the total hash value
    total_hash = sum(row['HASH_ING'] for row in result)
    st.write("Total hash value: ", total_hash)

    expected_hash_value = 2881182761772377708
    if total_hash == expected_hash_value:
        st.success('Hash values verified!', icon="✅")
    else:
        st.error('Hash values did not match.', icon="❌")

# Button to verify hash values
if st.button('Verify Hash Values for DORA Check'):
    verify_hash_values()
