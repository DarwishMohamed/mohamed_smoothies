# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col
import requests
import hashlib

# Write directly to the app
st.title("Customize Your Smoothie 🍹")
st.write("E5tar el fakha el enta 3ayezha w engez mat2refnash")

# Input for name on order
name_on_order = st.text_input('Name on Order', '')

order_filled = st.checkbox('Mark order as filled')

cnx = st.connection("snowflake")
session = cnx.session()
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'), col('SEARCH_ON'))

# Convert the Snowpark dataframe to a Pandas Dataframe so we can use the LOC function
pd_df = my_dataframe.to_pandas()

ingredients_list = st.multiselect(
    'Choose up to 5 ingredients',
    pd_df['FRUIT_NAME'].tolist()
)

def calculate_hash(ingredients, delimiter):
    ingredients.sort()  # Sorting the ingredients
    ingredients_string = delimiter.join(ingredients).strip()
    return int(hashlib.md5(ingredients_string.encode('utf-8')).hexdigest(), 16)

if ingredients_list:
    # Generate ingredient strings using different sources
    ingredients_string_comma_space = ', '.join(ingredients_list)
    ingredients_string_space = ' '.join(ingredients_list)
    ingredients_string_comma = ','.join(ingredients_list)

    search_on_list = [pd_df.loc[pd_df['FRUIT_NAME'] == fruit, 'SEARCH_ON'].iloc[0] for fruit in ingredients_list]
    search_on_string_comma_space = ', '.join(search_on_list)
    search_on_string_space = ' '.join(search_on_list)
    search_on_string_comma = ','.join(search_on_list)

    fruit_option_list = [pd_df.loc[pd_df['FRUIT_NAME'] == fruit, 'FRUIT_NAME'].iloc[0] for fruit in ingredients_list]
    fruit_option_string_comma_space = ', '.join(fruit_option_list)
    fruit_option_string_space = ' '.join(fruit_option_list)
    fruit_option_string_comma = ','.join(fruit_option_list)

    # Calculate hashes using different concatenation techniques and sources
    hash_fruit_name_space = calculate_hash(ingredients_list, ' ')
    hash_fruit_name_comma = calculate_hash(ingredients_list, ',')
    hash_fruit_name_comma_space = calculate_hash(ingredients_list, ', ')

    hash_search_on_space = calculate_hash(search_on_list, ' ')
    hash_search_on_comma = calculate_hash(search_on_list, ',')
    hash_search_on_comma_space = calculate_hash(search_on_list, ', ')

    hash_fruit_option_space = calculate_hash(fruit_option_list, ' ')
    hash_fruit_option_comma = calculate_hash(fruit_option_list, ',')
    hash_fruit_option_comma_space = calculate_hash(fruit_option_list, ', ')

    st.write(f"Hash using FRUIT_NAME with space as delimiter: {hash_fruit_name_space}")
    st.write(f"Hash using FRUIT_NAME with comma as delimiter: {hash_fruit_name_comma}")
    st.write(f"Hash using FRUIT_NAME with comma and space as delimiter: {hash_fruit_name_comma_space}")
    st.write(f"Hash using SEARCH_ON with space as delimiter: {hash_search_on_space}")
    st.write(f"Hash using SEARCH_ON with comma as delimiter: {hash_search_on_comma}")
    st.write(f"Hash using SEARCH_ON with comma and space as delimiter: {hash_search_on_comma_space}")
    st.write(f"Hash using FRUIT_OPTION with space as delimiter: {hash_fruit_option_space}")
    st.write(f"Hash using FRUIT_OPTION with comma as delimiter: {hash_fruit_option_comma}")
    st.write(f"Hash using FRUIT_OPTION with comma and space as delimiter: {hash_fruit_option_comma_space}")

    # Default hash used in INSERT statement (update as necessary)
    hash_ing = hash_fruit_name_comma_space
    st.write(f"Calculated hash: {hash_ing}")

    my_insert_stmt = f"""
    INSERT INTO smoothies.public.orders(ingredients, name_on_order, order_filled, hash_ing)
    VALUES ('{ingredients_string_comma_space}', '{name_on_order}', '{str(order_filled).upper()}', '{str(hash_ing)}')
    """

    st.write(my_insert_stmt)

    # Display the submit button
    time_to_insert = st.button('Submit Order')

    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        st.success(f'Your Smoothie is ordered, {name_on_order}!', icon="✅")
