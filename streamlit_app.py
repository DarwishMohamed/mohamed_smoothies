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
#st.dataframe(data=my_dataframe, use_container_width=True)
#st.stop()

# Convert the Snowpark dataframe to a Pandas Dataframe so we can use the LOC function
pd_df = my_dataframe.to_pandas()
#st.dataframe(pd_df)
#st.stop()

ingredients_list = st.multiselect(
    'Choose up to 5 ingredients',
    my_dataframe.select(col('FRUIT_NAME')).collect()
)

def calculate_hash(ingredients):
    ingredients_string = ', '.join(ingredients).strip()
    return int(hashlib.md5(ingredients_string.encode()).hexdigest(), 16)

if ingredients_list:
    ingredients_string = ', '.join(ingredients_list).strip()
    hash_ing = calculate_hash(ingredients_list)

    st.write(f"Concatenated ingredients string: '{ingredients_string}'")
    st.write(f"List used for hash calculation: {ingredients_list}")
    st.write(f"Calculated hash: {hash_ing}")

    # st.write(ingredients_string)
    my_insert_stmt = """
    INSERT INTO smoothies.public.orders(ingredients, name_on_order, order_filled, hash_ing)
    VALUES ('""" + ingredients_string.strip() + """', '""" + name_on_order + """', '""" + str(order_filled).upper() + """', '""" + str(hash_ing) + """')
    """

    st.write(my_insert_stmt)

    # Display the submit button
    time_to_insert = st.button('Submit Order')

    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        st.success(f'Your Smoothie is ordered, {name_on_order}!', icon="✅")
