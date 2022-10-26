# Local development

Use python version 3.10 for development.  

`pyenv` is the recommended method to install and manage python versions.

## To set up your local development environment run the following commands: 
```
python -m venv venv/
source venv/bin/activate
python -m pip install pip-tools
make update
```

## To update dependencies:
### DO NOT UPDATE requirements.txt or requirements.in manually!!!

1. If you have not set up your local environment run the steps described above.
  
2.  Activate your local environment in not already activated.
```
source venv/bin/activate
```
3. Make required changes to `pyproject.toml`
   
4. Generate a new version of requirements.in and requirements.txt and apply updated requirements.txt to venv.
```
make update
```
Any python code changes will be picked up automatically after make update has been executed at lease once.

##Testing
To run pytest:
```
make pytest
```
Pytest is configured to also run black and flake8.  Formatting failures are treated as test failures.

# Docker
When building the docker image to indicate the version set an environment var
```
IMAGE_VERSION=<version>
```
If IMAGE_VERSION is not set via env var, the default value is `<username>-dev` (e.g. gleonard-dev) 

## Building the docker image 
To build a docker image run:
```
make image
```
To update environment, run pytest and build a new image run:
```
make build
```
## Running Overwatch as a Docker container locally
After building the docker image use the following command to launch the container:
```
make run CREDENTIAL_VOLUME_MOUNT=<location of service account file> DESTINATION_CREDENTIAL_FILENAME=<service_account_filename>.json SLACK_BOT_TOKEN=<slackbot_token>
```

To run the docker image with access to a shell prompt use (generally for debugging purposes):
```
make shell 
```

To stop the docker container:
```
make stop
```
# Running Overwatch via Airflow
Testing Overwatch with Airflow can be accomplished by running Airflow locally.  
Follow the steps outlined in https://mana.mozilla.org/wiki/pages/viewpage.action?spaceKey=SRE&title=WTMO+Developer+Guide 
to set up Airflow

1. The Container Registry in the automated-analysis-dev project has been enabled (https://console.cloud.google.com/gcr/images/automated-analysis-dev?project=automated-analysis-dev).  
    This is where development images are pushed and pulled.  To push a development docker image use (see IMAGE_VERSION notes above).  
```
docker push 
```
2.  Launch airflow and create gck  (see https://mana.mozilla.org/wiki/pages/viewpage.action?spaceKey=SRE&title=WTMO+Developer+Guide)
3.  Create the following Variables in Airflow:
    - `overwatch_slack_token` and set the value to the Slack token (contact gleonard@mozilla.com for access). 
    - `overwatch_image_version` and set to the value of IMAGE_VERSION or `<username>-dev` if IMAGE_VERSION is not set
4.  Copy dags/overwatch.py from this project to telemetry-airflow/dags.  Airflow will load the DAG file automatically and it will be listed in http://localhost:8000/home


