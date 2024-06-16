# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col
import requests
import pandas as pd

# Write directly to the app
st.title("Customize Your Smoothie:smoothie:")
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
    INSERT INTO smoothies.public.orders (ingredients, name_on_order)
    VALUES ('{ingredients_string}', '{name_on_order}')
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
    SET order_filled = True
    WHERE name_on_order = '{name_on_order}'
    """
    session.sql(mark_filled_stmt).collect()

# Example usage to mark specific orders as filled
if st.button('Mark Orders for Divya and Xi as Filled'):
    mark_order_filled('Divya')
    mark_order_filled('Xi')
    st.success('Orders for Divya and Xi have been marked as filled!', icon="✅")
