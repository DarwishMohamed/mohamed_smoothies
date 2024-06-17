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

def calculate_hash(ingredients):
    ingredients_string = ' '.join(ingredients).strip()
    return int(hashlib.md5(ingredients_string.encode()).hexdigest(), 16)

if ingredients_list:
    ingredients_string_list = []
    search_on_string_list = []

    for fruit_chosen in ingredients_list:
        ingredients_string_list.append(fruit_chosen)
        search_on = pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'].iloc[0]
        search_on_string_list.append(search_on)
        st.write('The search value for ', fruit_chosen, ' is ', search_on, '.')

        st.subheader(fruit_chosen + ' Nutrition Information')
        fruityvice_response = requests.get("https://fruityvice.com/api/fruit/" + search_on)
        fv_df = st.dataframe(data=fruityvice_response.json(), use_container_width=True)

    # Create the different ingredient strings
    ingredients_string = ' '.join(ingredients_string_list).strip()
    search_on_string = ' '.join(search_on_string_list).strip()

    # Calculate hashes for each method
    hash_using_fruit_name = calculate_hash(ingredients_string_list)
    hash_using_search_on = calculate_hash(search_on_string_list)
    hash_using_ingredients_string = calculate_hash(ingredients_string)

    st.write(f"Hash using FRUIT_NAME: {hash_using_fruit_name}")
    st.write(f"Hash using SEARCH_ON: {hash_using_search_on}")
    st.write(f"Hash using concatenated string: {hash_using_ingredients_string}")

    # Choose one of the methods for the insert statement (adjust as needed based on results)
    chosen_hash = hash_using_fruit_name  # Change this based on which hash matches the expected value

    my_insert_stmt = f"""
    INSERT INTO smoothies.public.orders(ingredients, name_on_order, order_filled, hash_ing)
    VALUES ('{ingredients_string}', '{name_on_order}', '{str(order_filled).upper()}', {chosen_hash})
    """

    st.write(my_insert_stmt)

    # Display the submit button
    time_to_insert = st.button('Submit Order')

    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        st.success(f'Your Smoothie is ordered, {name_on_order}!', icon="✅")
