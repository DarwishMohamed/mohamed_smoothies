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
    ingredients_string = delimiter.join(ingredients).strip()
    return int(hashlib.md5(ingredients_string.encode()).hexdigest(), 16)

if ingredients_list:
    ingredients_string = ', '.join(ingredients_list)
    
    for fruit_chosen in ingredients_list:
        search_on = pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'].iloc[0]
        fruit_option = pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen, 'FRUIT_NAME'].iloc[0]
        st.write('The search value for ', fruit_chosen, ' is ', search_on, '.')
        st.write('The fruit option value for ', fruit_chosen, ' is ', fruit_option, '.')

        st.subheader(fruit_chosen + ' Nutrition Information')
        fruityvice_response = requests.get("https://fruityvice.com/api/fruit/" + search_on)
        fv_df = st.dataframe(data=fruityvice_response.json(), use_container_width=True)

    # Calculate hashes using different delimiters and sources
    hash_using_fruit_name_space = calculate_hash(ingredients_list, ' ')
    hash_using_fruit_name_comma = calculate_hash(ingredients_list, ',')
    hash_using_fruit_name_comma_space = calculate_hash(ingredients_list, ', ')

    search_on_list = [pd_df.loc[pd_df['FRUIT_NAME'] == fruit, 'SEARCH_ON'].iloc[0] for fruit in ingredients_list]
    hash_using_search_on_space = calculate_hash(search_on_list, ' ')
    hash_using_search_on_comma = calculate_hash(search_on_list, ',')
    hash_using_search_on_comma_space = calculate_hash(search_on_list, ', ')

    fruit_option_list = [pd_df.loc[pd_df['FRUIT_NAME'] == fruit, 'FRUIT_NAME'].iloc[0] for fruit in ingredients_list]
    hash_using_fruit_option_space = calculate_hash(fruit_option_list, ' ')
    hash_using_fruit_option_comma = calculate_hash(fruit_option_list, ',')
    hash_using_fruit_option_comma_space = calculate_hash(fruit_option_list, ', ')

    st.write(f"Hash using FRUIT_NAME with space as delimiter: {hash_using_fruit_name_space}")
    st.write(f"Hash using FRUIT_NAME with comma as delimiter: {hash_using_fruit_name_comma}")
    st.write(f"Hash using FRUIT_NAME with comma and space as delimiter: {hash_using_fruit_name_comma_space}")
    st.write(f"Hash using SEARCH_ON with space as delimiter: {hash_using_search_on_space}")
    st.write(f"Hash using SEARCH_ON with comma as delimiter: {hash_using_search_on_comma}")
    st.write(f"Hash using SEARCH_ON with comma and space as delimiter: {hash_using_search_on_comma_space}")
    st.write(f"Hash using FRUIT_OPTION with space as delimiter: {hash_using_fruit_option_space}")
    st.write(f"Hash using FRUIT_OPTION with comma as delimiter: {hash_using_fruit_option_comma}")
    st.write(f"Hash using FRUIT_OPTION with comma and space as delimiter: {hash_using_fruit_option_comma_space}")

    # Default hash used in INSERT statement (update as necessary)
    hash_ing = hash_using_fruit_name_comma_space
    st.write(f"Calculated hash: {hash_ing}")

    my_insert_stmt = f"""
    INSERT INTO smoothies.public.orders(ingredients, name_on_order, order_filled, hash_ing)
    VALUES ('{ingredients_string}', '{name_on_order}', '{str(order_filled).upper()}', '{str(hash_ing)}')
    """

    st.write(my_insert_stmt)

    # Display the submit button
    time_to_insert = st.button('Submit Order')

    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        st.success(f'Your Smoothie is ordered, {name_on_order}!', icon="✅")
