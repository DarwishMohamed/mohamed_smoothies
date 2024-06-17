What needs to be done : 

1) Automate email being sent to lewis so that it automatically sends every monday with the filter for shell and unilever 
2) Need to write some code which automates an email being sent to the respective account managers form the salesforce table and automatically detects clients from the table and sends email every monady morning
3) mitigae SMTP error 
4) email automation to jack task 


1) change your code so that the coresignal goldlayer table is uk_us_data_jobs_skills not job_industries then do a pull request (both you and faith need to do one) 
2) Need to make test out faith's email automation code 
3) try to get any python script to run on scheduler, if it does, replicate that with email_automation_general 









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
