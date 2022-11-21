# Imports
# -----------------------------------------------------------
import numpy as np
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from components.NavBar import NavBar
from components.SideBar import SideBar
import components.PlotFunctions as PlotFunctions


# -----------------------------------------------------------
# change icon and page name
st.set_page_config(page_title = "TOMTOM", page_icon = "https://seeklogo.com/images/T/TomTom-logo-76DD5E06F5-seeklogo.com.jpg")

# -----------------------------------------------------------
# Data ingestion
t = pd.date_range("1/1/2000", periods=100)
df = pd.DataFrame(np.random.randn(100, 4), index=t, columns=list("ABCD"))
df = df.cumsum()


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
st.title("Title")


# """
# Some text here
# """

# Show plot
st.write(PlotFunctions.example_plot(df, letter = letter))

# -----------------------------------------------------------


