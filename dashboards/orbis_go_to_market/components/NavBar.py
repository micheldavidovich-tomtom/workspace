style = """
.navbar .navbar-brand{
    margin-left:20px !important;
    }
    """

buttons = {
    "names": ["GitHub", "Confluence", "Jira"],
    "links": [
        "https://github.com/tomtom-internal/github-maps-analytics-strategic-analysis",
        "https://confluence.tomtomgroup.com/display/MANA/Strategic+Analysis",
        "https://jira.tomtomgroup.com/projects/STAN/summary"

    ]
}
side_bar = """
  <style>
    /* The whole sidebar */
    .css-1lcbmhc.e1fqkh3o0{
      margin-top: 3.8rem;
    }
     
     /* The display arrow */
    .css-sg054d.e1fqkh3o3 {
      margin-top: 5rem;
      }
  </style> 
  """
import streamlit as st

class NavBar():

    def __init__(self, style = style, buttons = buttons, side_bar = side_bar):

        self.style = style
        self.buttons = buttons
        self.side_bar = side_bar

    def create_nav_bar(self):

        buttons_html = ""
        for name, link in zip(self.buttons["names"], self.buttons["links"]):
            buttons_html += f"""<li class="nav-item">
                    <a class="nav-link" href="{link}" target="_blank">{name}</a>
                </li>"""
        nav_bar_html = f"""<style>
        {self.style}
    </style>
<nav class="navbar fixed-top navbar-expand-lg navbar-light bg-light">
        <a class="navbar-brand" href="https://www.tomtom.com/es_es/" target="_blank" style="margin-left:15px!important;">
        <img alt="Qries" src="https://raw.githubusercontent.com/jesusprieto-tomtom/svg-png/main/strategic-analysis.svg"
                width="270" height="40">
        </a>
        <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarNav" aria-controls="navbarNav" aria-expanded="false" aria-label="Toggle navigation">
            <span class="navbar-toggler-icon"></span>
        </button>
        <div class="collapse navbar-collapse" id="navbarNav">
            <ul class="navbar-nav">
            {buttons_html}
            </ul>
        </div>
    </nav>
"""

        st.markdown('<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">', unsafe_allow_html=True)
        st.markdown(nav_bar_html, unsafe_allow_html=True)
        st.markdown(self.side_bar, unsafe_allow_html=True)

        # print(nav_bar_html)


