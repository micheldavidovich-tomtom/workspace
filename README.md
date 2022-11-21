# Local databricks 9.1 LTS-like environment

This tutorial shows how to access TomTom's FCD traces from a local host with the [trace-databricks](https://confluence.tomtomgroup.com/display/FCD/Accessing+FCD+Archive+from+Databricks) library. Instead of using a databricks cluster, we will use vscode and docker.

For convenience, you can use this template repository and modify it to fit your needs.

## Prerequisites

+ Visual Studio Code (vscode)
+ git
+ docker and docker-compose
+ Remote-Containers extension for vscode
+ azure-cli `az`

## Azure login

Open a system console (cmd, PowerShell, terminal, etc) and login to azure and to our container registry

```
az login
az acr login -n mapsanalytics
```
## Open in container

Open vscode and open the root folder of your repository, e.g. `github-maps-analytics-template`. Then, with the Remote-Containers extension, select `Reopen in Container`.

You should now have a jupyterlab server running in http://localhost:8000/.

Navigate to `/workspace/notebooks/hello_world.ipynb` in jupyter to quick start.




