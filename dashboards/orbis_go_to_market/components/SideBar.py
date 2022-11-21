import streamlit as st

def SideBar():
    sidebar = st.sidebar

    letter = sidebar.selectbox(
        'Plot letter:',
        ('A', 'B', 'C', 'D'))

    sidebar.write(
        """
        Some text here
        """
    )

    return letter