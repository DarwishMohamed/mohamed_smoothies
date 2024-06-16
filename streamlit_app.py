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
    ingredients_string = ' '.join([fruit.lower().strip() for fruit in ingredients_list])

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
    ingredients_string = ' '.join([ingredient.lower().strip() for ingredient in ingredients])
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
    create_order('Kevin', ['apples', 'lime', 'ximenia'], fill_order=False)
    create_order('Divya', ['dragon fruit', 'guava', 'figs', 'jackfruit', 'blueberries'], fill_order=True)
    create_order('Xi', ['vanilla fruit', 'nectarine'], fill_order=True)
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
