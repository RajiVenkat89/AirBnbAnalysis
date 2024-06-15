# Importing libraries
import json
import pandas as pd
import mysql.connector
import streamlit as st
import plotly.express as px
from streamlit_option_menu import option_menu

##Connection to the database
client=mysql.connector.connect(host='localhost',user='root',password='root',database='airbnb')
cursor=client.cursor()

## This function is used to take the required columns from the JSON and cleanse the data and keep it
#ready for further processing
def data_preparation():
    json_open=open("C:\\Users\\Dharmarajan\\Documents\\Guvi\\Project\\Project 3\\sample_airbnb.json",'r')
    json_data=json.load(json_open)
    data={'id':[],'listing_url':[],'name':[],'prop_type':[],'room_type':[],
      'bed_type':[],'min_nights':[],'max_nights':[],'cancel_pol':[],
      'accommodates':[],'bedrooms':[],'beds':[],'no_of_reviews':[],
      'bathrooms':[],'price':[],'cleaning_fee':[],'extra_ppl':[],
      'guests_incl':[],'picture_url':[],'review_sc_rtg':[],'host_id':[],
      'host_name':[],'host_resp_time':[],'host_resp_rate':[],'host_super_host':[],
      'host_id_verified':[],'amenities':[],'addr_loctype':[],'addr_longitude':[],
      'addr_latitude':[],'addr_loc_exact':[],'addr_street':[],'addr_govt_area':[],
      'addr_country':[],'avail_30':[],'avail_60':[],'avail_90':[],'avail_365':[]
       }
    for i in json_data:
        id=i['_id']
        listing_url=i['listing_url']
        name=i['name']
        prop_type=i['property_type']
        room_type=i['room_type']
        bed_type=i['bed_type']
        min_nights=i['minimum_nights']
        max_nights=i['maximum_nights']
        cancel_pol=i['cancellation_policy']
        accommodates=i['accommodates']
        bedrooms=i.get('bedrooms',0)
        beds=i.get('beds',0)
        no_of_reviews=i['number_of_reviews']
        bathrooms=i.get('bathrooms',0)
        price=i['price']
        cleaning_fee=i.get('cleaning_fee',0)
        extra_ppl=i['extra_people']
        guests_incl=i['guests_included']
        picture_url=i['images']['picture_url']
        review_sc_rtg=i['review_scores'].get('review_scores_rating',0)
        host_id=i['host']['host_id']
        host_name=i['host']['host_name']
        host_resp_time=i['host'].get('host_response_time','Not Specified')
        host_resp_rate=i['host'].get('host_response_rate',0)
        host_super_host=i['host']['host_is_superhost']
        host_id_verified=i['host']['host_identity_verified']
        amenities_list=[]
        for j in i['amenities']:
            amenities_list.append(j)
        amenities_list.sort()
        amenities=' '
        for k in amenities_list:
            amenities+=k+','
        addr_loctype=i['address']['location']['type']
        addr_longitude=i['address']['location']['coordinates'][0]
        addr_latitude=i['address']['location']['coordinates'][1]
        addr_loc_exact=i['address']['location']['is_location_exact']
        addr_street=i['address']['street']
        addr_govt_area=i['address']['government_area']
        addr_country=i['address']['country']
        avail_30=i['availability']['availability_30']
        avail_60=i['availability']['availability_60']
        avail_90=i['availability']['availability_90']
        avail_365=i['availability']['availability_365']
        data['id'].append(id)
        data['listing_url'].append(listing_url)
        data['name'].append(name)
        data['prop_type'].append(prop_type)
        data['room_type'].append(room_type)
        data['bed_type'].append(bed_type)
        data['min_nights'].append(min_nights)
        data['max_nights'].append(max_nights)
        data['cancel_pol'].append(cancel_pol)            
        data['accommodates'].append(accommodates)
        data['bedrooms'].append(bedrooms)
        data['beds'].append(beds)
        data['no_of_reviews'].append(no_of_reviews)
        data['bathrooms'].append(bathrooms)
        data['price'].append(price)
        data['cleaning_fee'].append(cleaning_fee)
        data['extra_ppl'].append(extra_ppl)
        data['guests_incl'].append(guests_incl)
        data['picture_url'].append(picture_url)
        data['review_sc_rtg'].append(review_sc_rtg)
        data['host_id'].append(host_id)
        data['host_name'].append(host_name)
        data['host_resp_time'].append(host_resp_time)
        data['host_resp_rate'].append(host_resp_rate)
        data['host_super_host'].append(host_super_host)
        data['host_id_verified'].append(host_id_verified)
        data['amenities'].append(amenities)
        data['addr_loctype'].append(addr_loctype)
        data['addr_longitude'].append(addr_longitude)
        data['addr_latitude'].append(addr_latitude)
        data['addr_loc_exact'].append(addr_loc_exact)
        data['addr_street'].append(addr_street)
        data['addr_govt_area'].append(addr_govt_area)
        data['addr_country'].append(addr_country)
        data['avail_30'].append(avail_30)
        data['avail_60'].append(avail_60)
        data['avail_90'].append(avail_90)
        data['avail_365'].append(avail_365)
    data_df=pd.DataFrame(data)
    data_df['host_super_host'] = data_df['host_super_host'].map({True: 'Yes', False: 'No'})
    data_df['host_id_verified'] = data_df['host_id_verified'].map({True: 'Yes', False: 'No'})
    data_df['addr_loc_exact'] = data_df['addr_loc_exact'].map({True: 'Yes', False: 'No'})
    return data_df

#This function is used to get the dataframe as input from the cleaned data and then create the  table and insert
#the data into the AIRBNB database
def data_insertion(data_df):
    create_query="""create table if not exists airbnb_prop_dtls(id bigint primary key, listing_url text,
	name varchar(255), prop_type varchar(255), room_type varchar(255), bed_type	varchar(255), min_nights int, 
	max_nights	int, cancel_pol	varchar(255), accommodates	int, bedrooms int, beds	int, no_of_reviews	int,
	bathrooms float, price float, cleaning_fee	float, extra_ppl int, guests_incl int, picture_url text,
	review_sc_rtg int, host_id bigint, host_name varchar(255), host_resp_time varchar(255), host_resp_rate int,
	host_super_host	varchar(255), host_id_verified	varchar(255), amenities	text, addr_loctype	varchar(255), 
    addr_longitude float, addr_latitude	float, addr_loc_exact varchar(255), addr_street	varchar(255), 
    addr_govt_area varchar(255), addr_country varchar(255), avail_30 int, avail_60	int, avail_90 int, avail_365 int);"""
    cursor.execute(create_query)
    client.commit()
    del_query="delete from airbnb_prop_dtls "
    cursor.execute(del_query)
    client.commit()
    insert_query="""insert into airbnb_prop_dtls (id,listing_url,name,prop_type,room_type,bed_type,min_nights,max_nights,
    cancel_pol,accommodates,bedrooms,beds,no_of_reviews,bathrooms,price,cleaning_fee,extra_ppl,guests_incl,picture_url,
    review_sc_rtg,host_id,host_name,host_resp_time,host_resp_rate,host_super_host,host_id_verified,amenities,addr_loctype,
    addr_longitude,addr_latitude,addr_loc_exact,addr_street,addr_govt_area,addr_country,avail_30,avail_60,avail_90,
    avail_365) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s)"""
    cursor.executemany(insert_query,data_df.values.tolist())
    client.commit()

#This is used to get the distinct country from the table. This is required for the analysis and insights
def country_list():
    cursor.execute(f"select distinct addr_country from airbnb_prop_dtls order by addr_country")
    output=cursor.fetchall()
    df=pd.DataFrame(output,columns=['Country'])
    df.index.name="S.No"
    df.index+=1
    return df

#This function is used to get the average of the column passed based on the country
def country_select_avg(column_name):
    cursor.execute(f"select addr_country,Convert((round(avg({column_name}),2)),float) as average from airbnb_prop_dtls group by addr_country order by average desc")
    output=cursor.fetchall()
    df=pd.DataFrame(output,columns=['Country','Average'])
    df.index.name="S.No"
    df.index+=1
    return df

#This function is used to get the count of the column passed based on the country
def country_select_cnt(column_name):
    cursor.execute(f"select addr_country,count(distinct {column_name}) as count from airbnb_prop_dtls group by addr_country order by count desc")
    output=cursor.fetchall()
    df=pd.DataFrame(output,columns=['Country','Count'])
    df.index.name="S.No"
    df.index+=1
    return df

#This function is used to get the required feature from the table based on the column name provided 
def column_name(country, column_name):
    cursor.execute(f"select {column_name},count(host_name) as count from airbnb_prop_dtls where addr_country='{country}' group by {column_name} order by count desc")
    output=cursor.fetchall()
    df=pd.DataFrame(output,columns=[column_name,'Count'])
    df.index.name="S.No"
    df.index+=1
    return df

#This function is used to get the host name count based on the type of column passed and the country name .
def host_count(country,column_value,column_name):
    cursor.execute(f"select host_name,count({column_name}) as count from airbnb_prop_dtls where addr_country='{country}' and {column_name}='{column_value}' group by host_name order by count desc limit 10")
    output=cursor.fetchall()
    df=pd.DataFrame(output,columns=['Host_Name','Count'])
    df.index.name="S.No"
    df.index+=1
    return df

#This function is used to get the count of any feature based on the column name provided 
def feature_count(column_name):
    cursor.execute(f"select {column_name},count({column_name}) as count from airbnb_prop_dtls group by {column_name} order by count desc limit 10")
    output=cursor.fetchall()
    df=pd.DataFrame(output,columns=[column_name,'Count'])
    df.index.name="S.No"
    df.index+=1
    df['Percentage'] = round((df['Count'] / df['Count'].sum()) * 100,2)
    return df

#This function is used to plot a pie chart based on the parameters provided
def piechart(data,names,values,title):
    fig=px.pie(data,names=names,values=values,title=title)  
    fig.update_layout(title_font_size=22)
    fig.update_traces(textposition='none',textfont=dict(color='white'))
    st.plotly_chart(fig,use_container_width=True)

#This function is used to plot a bar chart based on the parameters provided
def barchart(data,x,y,text,xaxis_title,title):
    fig=px.bar(data,x=x,y=y,title=title)
    fig.update_traces(text=data[text])
    fig.update_xaxes(title_text=xaxis_title)
    st.plotly_chart(fig,use_container_width=False)

#This function is used to plot a line chart based on the parameters provided
def linechart(data,x,y,xaxis_title,title):
    fig=px.line(data,x=x,y=y,title=title)
    fig.update_xaxes(title_text=xaxis_title)
    st.plotly_chart(fig,use_container_width=False)

#This function is used to plot a geo chart based on the parameters provided
def scatter_geo(dataframe,locations,color,hover_data,locationmode,size,title,color_continuous_scale='agsunset'):
    fig=px.scatter_geo(data_frame=dataframe,
                        locations=locations,
                        color=color,
                        hover_data=hover_data,
                        locationmode=locationmode,
                        size=size,
                        title=title,
                        color_continuous_scale=color_continuous_scale)
    fig.update_layout({'geo':{'resolution':50}})
    st.plotly_chart(fig,use_container_width=True)
    st.table(data)

# The below is the main pogram which does the following
# 1.Read the data from the JSON and insert into the database
# 2.Export the data from the database as a CSV file to be provided as input to the tableau
# 3.Provides the geo analysis based on the Price,Listings,Host,Ratings
# 4.Provides the host analysis data based on the country chosen . This is done for Room Type,Property Type,Cancellation Policy,Response time
# 5.Provides insights on the different fatures available in AIRBNB.
# 6.All the insights are provided in different tabs and shown as either bar/Pie/Line graph . 
# 7.Streamlit is designed in a user friendly way so that the end user can navigate through the menus easily .

st.set_page_config(page_title='AIRBNB',layout="wide")

st.markdown(f'<h1 style= "text-align:center;size:24px;color:blue;">AIRBNB ANALYSIS</h1>',unsafe_allow_html=True)
st.header(":blue[**Welcome!!!**]")
with st.sidebar:
    image_url='https://raw.githubusercontent.com/RajiVenkat89/AirBnbAnalysis/main/airbnb.jpeg'
    st.image(image_url,use_column_width=True)
    selected = option_menu(menu_title='',options=["Data Migration","Country Analysis","Host Analysis","Features"],
        icons=['database','pin-map','people-fill','pencil','list-task'], menu_icon="cast", default_index=0)

if(selected=='Data Migration'):
    tab1,tab2=st.tabs(["Import to DB", "Export as CSV"])
    with tab1: 
        st.write("**Data Processing and Insertion into Database**")
        col1,col2,col3=st.columns(3)
        with col2:
            button = st.button(label='Insert Data')
            data=data_preparation()
            if(button==True):
                try:
                    data_insertion(data)
                    st.write(":green[*Data Inserted Successfully*]")            
                except Exception as e:
                    st.write(e)
    with tab2:
        st.write("**Data Processing and Export to CSV**")
        col1,col2,col3=st.columns(3)
        with col2:
            download=st.button(label='Downlad as csv')
            data=data_preparation()
            if(download==True):
                data.to_csv("AIRBNB_DATA.csv")
                st.write("Download Successful")

if(selected=='Country Analysis'):
    st.write("Geospatial vizualization of AirBnb listing across countries based on Average(Price/Ratings)/Count(Listings/Host)")
    tab1,tab2,tab3,tab4=st.tabs(["Price","Ratings","Listings","Host"])
    with tab1: 
        data=country_select_avg('price')
        scatter_geo(data,'Country','Average',['Average'],'country names','Average','Average Price')
    with tab2:
        data=country_select_avg('review_sc_rtg')
        scatter_geo(data,'Country','Average',['Average'],'country names','Average','Average Ratings')
    with tab3:
        data=country_select_cnt('name')
        scatter_geo(data,'Country','Count',['Count'],'country names','Count','Listing Count')
    with tab4:
        data=country_select_cnt('host_name')
        scatter_geo(data,'Country','Count',['Count'],'country names','Count','Host Count')

if(selected=='Host Analysis'):
    country_list=country_list()
    country=st.selectbox(label='Country',options=country_list)              
    tab1,tab2,tab3,tab4=st.tabs(["Room Type","Property Type","Response Time","Cancellation Policy"])
    with tab1:
        room_type_data=column_name(country,'room_type')
        room_type=st.selectbox("Room Type",room_type_data['room_type'])
        count_data=host_count(country,room_type,'room_type')
        piechart(count_data,'Host_Name','Count','Room Type')
        barchart(count_data,'Host_Name','Count','Count','Room Type','Room Type')
    with tab2:
        prop_type_data=column_name(country,'prop_type')
        prop_type=st.selectbox("Property Type",prop_type_data['prop_type'])
        count_data=host_count(country,prop_type,'prop_type')
        piechart(count_data,'Host_Name','Count','Property Type')
        barchart(count_data,'Host_Name','Count','Count','Property Type','Property Type')        
    with tab3:
        host_resp_time_data=column_name(country,'host_resp_time')
        host_resp_time=st.selectbox("Property Type",host_resp_time_data['host_resp_time'])
        count_data=host_count(country,host_resp_time,'host_resp_time')
        piechart(count_data,'Host_Name','Count','Host Response Time')
        barchart(count_data,'Host_Name','Count','Count','Host Response Time','Host Response Time')       
    with tab4:
        cancel_pol_data=column_name(country,'cancel_pol')
        cancel_pol=st.selectbox("Property Type",cancel_pol_data['cancel_pol'])
        count_data=host_count(country,cancel_pol,'cancel_pol')
        piechart(count_data,'Host_Name','Count','Cancellation Policy')
        barchart(count_data,'Host_Name','Count','Count','Cancellation Policy','Cancellation Policy') 
       
if(selected=='Features'):
    tab1,tab2,tab3=st.tabs(['Property_Type','Room Type','Bed Type'])
    with tab1:
        select_option=st.radio(":red[Chart Type]",["Pie Chart","Bar Chart", "Line Chart"],horizontal=True,key='prop')
        data=feature_count('prop_type')
        if select_option=='Pie Chart':
            piechart(data,'prop_type','Count','Property Type')
        elif select_option=='Bar Chart':
            barchart(data,'prop_type','Count','Percentage','Property Type','Property Type')
        elif select_option=='Line Chart':
            linechart(data,'prop_type','Count','Property Type','Property Type')
    with tab2:
        select_option=st.radio(":red[Chart Type]",["Pie Chart","Bar Chart", "Line Chart"],horizontal=True,key='room')
        data=feature_count('room_type')
        if select_option=='Pie Chart':
            piechart(data,'room_type','Count','Room Type')
        elif select_option=='Bar Chart':
            barchart(data,'room_type','Count','Percentage','Room Type','Room Type')
        elif select_option=='Line Chart':    
            linechart(data,'room_type','Count','Room Type','Room Type')
    with tab3:
        select_option=st.radio(":red[Chart Type]",["Pie Chart","Bar Chart", "Line Chart"],horizontal=True,key='bedtype')
        data=feature_count('bed_type')
        if select_option=='Pie Chart':
            piechart(data,'bed_type','Count','Bed Type')
        elif select_option=='Bar Chart':
            barchart(data,'bed_type','Count','Percentage','Bed Type','Bed Type')
        elif select_option=='Line Chart':
            linechart(data,'bed_type','Count','Bed Type','Bed Type')

    tab1,tab2,tab3,tab4=st.tabs(['Minimum Nights','Maximum Nights','Cancellation Policy','Accomodates'])
    with tab1:
        select_option=st.radio(":red[Chart Type]",["Pie Chart","Bar Chart"],horizontal=True,key='minn')
        data=feature_count('min_nights')
        if select_option=='Pie Chart':
            piechart(data,'min_nights','Count','Minimum Nights')
        elif select_option=='Bar Chart':
            barchart(data,'min_nights','Count','Percentage','Minimum Nights','Minimum Nights')
    with tab2:
        select_option=st.radio(":red[Chart Type]",["Pie Chart","Bar Chart"],horizontal=True,key='maxx')
        data=feature_count('max_nights')
        if select_option=='Pie Chart':
            piechart(data,'max_nights','Count','Maximum Nights')
        elif select_option=='Bar Chart':
            barchart(data,'max_nights','Count','Percentage','Maximum Nights','Maximum Nights')
    with tab3:
        select_option=st.radio(":red[Chart Type]",["Pie Chart","Bar Chart", "Line Chart"],horizontal=True,key='cancel')
        data=feature_count('cancel_pol')
        if select_option=='Pie Chart':
            piechart(data,'cancel_pol','Count','Cancellation Policy')
        elif select_option=='Bar Chart':
            barchart(data,'cancel_pol','Count','Percentage','Cancellation Policy','Cancellation Policy')
        elif select_option=='Line Chart':
            linechart(data,'cancel_pol','Count','Cancellation Policy','Cancellation Policy')
    with tab4:
        select_option=st.radio(":red[Chart Type]",["Pie Chart","Bar Chart"],horizontal=True,key='accom')
        data=feature_count('accommodates')
        if select_option=='Pie Chart':
            piechart(data,'accommodates','Count','Accommmodates')
        elif select_option=='Bar Chart':
            barchart(data,'accommodates','Count','Percentage','Accommmodates','Accommmodates')
    
    tab1,tab2,tab3=st.tabs(['Bedrooms','No of Beds','Bathrooms'])
    with tab1:
        select_option=st.radio(":red[Chart Type]",["Pie Chart","Bar Chart"],horizontal=True,key='bedrooms')
        data=feature_count('bedrooms')
        if select_option=='Pie Chart':
            piechart(data,'bedrooms','Count','Bedrooms')
        elif select_option=='Bar Chart':
            barchart(data,'bedrooms','Count','Percentage','Bedrooms','Bedrooms')
    with tab2:
        select_option=st.radio(":red[Chart Type]",["Pie Chart","Bar Chart"],horizontal=True,key='noofbed')
        data=feature_count('beds')
        if select_option=='Pie Chart':
            piechart(data,'beds','Count','Bed')
        elif select_option=='Bar Chart':
            barchart(data,'beds','Count','Percentage','Bed','Bed')
    with tab3:
        select_option=st.radio(":red[Chart Type]",["Pie Chart","Bar Chart"],horizontal=True,key='bathroom')
        data=feature_count('bathrooms')
        if select_option=='Pie Chart':
            piechart(data,'bathrooms','Count','Bathroom')
        elif select_option=='Bar Chart':
            barchart(data,'bathrooms','Count','Percentage','Bathroom','Bathroom')
    tab1,tab2,tab3,tab4=st.tabs(['No of Reviews','Cleaning Fee','Extra People','Guests Included'])
    with tab1:
        select_option=st.radio(":red[Chart Type]",["Pie Chart","Bar Chart"],horizontal=True,key='reviews')
        data=feature_count('no_of_reviews')
        if select_option=='Pie Chart':
            piechart(data,'no_of_reviews','Count','No of Reviews')
        elif select_option=='Bar Chart':
            barchart(data,'no_of_reviews','Count','Percentage','No of Reviews','No of Reviews')
    with tab2:
        select_option=st.radio(":red[Chart Type]",["Pie Chart","Bar Chart"],horizontal=True,key='clean')
        data=feature_count('cleaning_fee')
        if select_option=='Pie Chart':
            piechart(data,'cleaning_fee','Count','Cleaning Fee')
        elif select_option=='Bar Chart':
            barchart(data,'cleaning_fee','Count','Percentage','Cleaning Fee','Cleaning Fee')
    with tab3:
        select_option=st.radio(":red[Chart Type]",["Pie Chart","Bar Chart"],horizontal=True,key='extra')
        data=feature_count('extra_ppl')
        if select_option=='Pie Chart':
            piechart(data,'extra_ppl','Count','Extra People')
        elif select_option=='Bar Chart':
            barchart(data,'extra_ppl','Count','Percentage','Extra People','Extra People')
    with tab4:
        select_option=st.radio(":red[Chart Type]",["Pie Chart","Bar Chart"],horizontal=True,key='guest')
        data=feature_count('guests_incl')
        if select_option=='Pie Chart':  
            piechart(data,'guests_incl','Count','Guests Included')
        elif select_option=='Bar Chart':
            barchart(data,'guests_incl','Count','Percentage','Guests Included','Guests Included')
    tab1,tab2,tab3,tab4=st.tabs(['Availability_30','Availability 60', 'Availability 90','Availability 365'])
    with tab1:
        select_option=st.radio(":red[Chart Type]",["Pie Chart","Bar Chart"],horizontal=True,key='avail30')
        data=feature_count('avail_30')
        if select_option=='Pie Chart':  
            piechart(data,'avail_30','Count','Availability 30')
        elif select_option=='Bar Chart':
            barchart(data,'avail_30','Count','Percentage','Availability 30','Availability 30')
    with tab2:
        select_option=st.radio(":red[Chart Type]",["Pie Chart","Bar Chart"],horizontal=True,key='avail60')
        data=feature_count('avail_60')
        if select_option=='Pie Chart':  
            piechart(data,'avail_60','Count','Availability 60')
        elif select_option=='Bar Chart':
            barchart(data,'avail_60','Count','Percentage','Availability 60','Availability 60')
    with tab3:
        select_option=st.radio(":red[Chart Type]",["Pie Chart","Bar Chart"],horizontal=True,key='avail90')
        data=feature_count('avail_90')
        if select_option=='Pie Chart':  
            piechart(data,'avail_90','Count','Availability 90')
        elif select_option=='Bar Chart':
            barchart(data,'avail_90','Count','Percentage','Availability 90','Availability 90')
    with tab4:
        select_option=st.radio(":red[Chart Type]",["Pie Chart","Bar Chart"],horizontal=True,key='avail365')
        data=feature_count('avail_365')
        if select_option=='Pie Chart':  
            piechart(data,'avail_365','Count','Availability 365')
        elif select_option=='Bar Chart':
            barchart(data,'avail_365','Count','Percentage','Availability 365','Availability 365')