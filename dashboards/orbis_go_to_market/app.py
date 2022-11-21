# Imports
# -----------------------------------------------------------
import numpy as np
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from components.NavBar import NavBar
from components.SideBar import SideBar
import components.PlotFunctions as PlotFunctions
import plotly.express as px
import datetime


# -----------------------------------------------------------
# change icon and page name
st.set_page_config(page_title = "TOMTOM", page_icon = "https://seeklogo.com/images/T/TomTom-logo-76DD5E06F5-seeklogo.com.jpg")

# -----------------------------------------------------------
# Data ingestion
df = px.data.gapminder().query("year==2007")
daterange = [
    '1/1/2022', '1/2/2022', '1/3/2022', '1/4/2022', '1/5/2022', '1/6/2022', '1/7/2022', '1/8/2022', '1/9/2022', '1/10/2022', '1/11/2022',
    '1/12/2022', '1/1/2023', '1/2/2023', '1/3/2023', '1/4/2023', '1/5/2023'
]
#daterange = [datetime.date(date.split('/')[2], date.split('/')[1], date.split('/')[0]) for date in daterange]

df_new = df.copy()
base_df = pd.DataFrame()

for date in daterange:
    
    df_new['lifeExp'] = df_new['lifeExp'] * 1.1
    split_date = date.split('/')
    df_new['date'] = datetime.date(int(split_date[2]), int(split_date[1]), int(split_date[0]))
    
    base_df = pd.concat([base_df, df_new])
df = df.dropna()

d3 = st.date_input("Select Date:", pd.to_datetime('1/1/2022'))
plot_df = base_df[base_df['date'] == d3]

fig = px.choropleth(
    df, 
    locations="iso_alpha",
    color="lifeExp", # lifeExp is a column of gapminder
    hover_name="country", # column to add to hover information
    labels={'lifeExp': 'Expected time to-maket'},
    color_continuous_scale=['red', 'yellow', 'green']   #px.colors.sequential.Plasma
)

# -----------------------------------------------------------
# SideBar
letter = SideBar()

# -----------------------------------------------------------
# NavBar
NavBar = NavBar()
NavBar.create_nav_bar()

# -----------------------------------------------------------

# Main
# -----------------------------------------------------------
# Create a title for your app
st.title("Orbis GO-TO MARKET estimation")

st.plotly_chart(fig, use_container_width=True)



# -----------------------------------------------------------


