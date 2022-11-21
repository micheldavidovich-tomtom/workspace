# Streamlit Template
The template is structured as follows:

- [**app.py:**](app.py) The script we should execute to run our app.
- [**components:**](components) Folder with all the different components of our app.
    - [**NavBar:**](components/NavBar.py) Code to create the NavBar. As streamlit does not support natively a navegation bar, it has been build by injecting html in the DOM. This will be modified once streamlit adds this capability. In the mean time, do not modify.
    - [**SideBar:**](components/SideBar.py) Code to create a side bar. All the widgets created should be placed within this script.
    - [**PlotFunctions:**](components/PlotFunctions.py) Script to place all the functions required to create the plots.

## Run locally
To run the application locally we simply need to execute
```
streamlit run app.py
```
within the app folder.

Once the command has been executed we should be able to find the application running on port 8501:

![image](https://user-images.githubusercontent.com/89970838/157858885-6815335c-2324-4498-81cf-50faf209e678.png)

## Production
To containerized the application and store the image in the azure container registry execute:
```
az acr build --registry strategicAnalysisAppRegistry --subscription maps-data-analytics-dev --resource-group StrategicAnalysis --image appname .
```

Finally, create a container instance with the uploaded image.



