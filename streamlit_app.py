# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col
import requests 

# Write directly to the app
st.title("Customize Your Smoothie 🍹")
st.write("E5tar el fakha el enta 3ayezha w engez mat2refnash")

# Input for name on order
name_on_order = st.text_input('Name on Order', '')

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
    my_dataframe
)

import hashlib

def calculate_hash(ingredients):
    ingredients_string = ', '.join(ingredients).strip()
    return int(hashlib.md5(ingredients_string.encode()).hexdigest(), 16)

# Ingredients as specified in the DORA checker
kevin_ingredients = ['Apples', 'Lime', 'Ximenia']
divya_ingredients = ['Dragon Fruit', 'Guava', 'Figs', 'Jackfruit', 'Blueberries']
xi_ingredients = ['Vanilla Fruit', 'Nectarine']

# Calculate hashes
kevin_hash = calculate_hash(kevin_ingredients)
divya_hash = calculate_hash(divya_ingredients)
xi_hash = calculate_hash(xi_ingredients)

print(f"Kevin's hash: {kevin_hash}")
print(f"Divya's hash: {divya_hash}")
print(f"Xi's hash: {xi_hash}")







if ingredients_list:
    ingredients_string = ''

    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen + ' '
        
        search_on=pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'].iloc[0]
        st.write('The search value for ', fruit_chosen,' is ', search_on, '.')

        st.subheader(fruit_chosen + 'Nutrition Information')
        fruityvice_response = requests.get("https://fruityvice.com/api/fruit/" + fruit_chosen)
        fv_df = st.dataframe(data=fruityvice_response.json(), use_container_width=True) 

    # st.write(ingredients_string)

    my_insert_stmt = """ 
    INSERT INTO smoothies.public.orders(ingredients, name_on_order)
    VALUES ('""" + ingredients_string.strip() + """', '""" + name_on_order + """')"""

    st.write(my_insert_stmt)

    # Display the submit button
    time_to_insert = st.button('Submit Order')

    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        st.success(f'Your Smoothie is ordered, {name_on_order}!', icon="✅")
