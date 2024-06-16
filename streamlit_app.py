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

# Checkbox to mark the order as filled or not
order_filled = st.checkbox('Mark Order as Filled')

if ingredients_list:
    ingredients_string = ' '.join(ingredients_list)

    for fruit_chosen in ingredients_list:
        st.subheader(fruit_chosen + ' Nutrition Information')
        search_on = pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'].iloc[0]
        fruityvice_response = requests.get(f"https://fruityvice.com/api/fruit/{search_on}")
        fv_df = pd.DataFrame.from_dict(fruityvice_response.json(), orient='index').T
        st.dataframe(fv_df)
        st.write(f'The search value for {fruit_chosen} is {search_on}.')

    # Display the SQL insert statement
    my_insert_stmt = f"""
    INSERT INTO smoothies.public.orders (ingredients, name_on_order, order_filled)
    VALUES ('{ingredients_string}', '{name_on_order}', {'TRUE' if order_filled else 'FALSE'})
    """
    st.write(my_insert_stmt)

    # Display the submit button
    time_to_insert = st.button('Submit Order')

    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        st.success(f'Your Smoothie is ordered, {name_on_order}!', icon="✅")

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

# Verify the hash values for DORA Check
def verify_hash_values():
    query = """
    SELECT SUM(hash_ing) AS total_hash FROM (
        SELECT name_on_order, order_filled, HASH(ingredients) AS hash_ing FROM smoothies.public.orders
        WHERE order_ts IS NOT NULL
        AND name_on_order IN ('Kevin', 'Divya', 'Xi')
    )
    """
    result = session.sql(query).collect()
    total_hash_value = result[0]['TOTAL_HASH']
    st.write(f"Total hash value: {total_hash_value}")

    expected_hash_value = 2881182761772377708
    if total_hash_value == expected_hash_value:
        st.success('Hash values verified!', icon="✅")
    else:
        st.error('Hash values did not match.', icon="❌")

# Button to verify hash values
if st.button('Verify Hash Values for DORA Check'):
    verify_hash_values()
