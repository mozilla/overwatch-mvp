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

1. If you have not setup your local environment run the steps described above.
  
2.  Activate your local environment in not already activated.
```
source venv/bin/activate
```
3. Make required changes to `pyproject.toml`
   
4. Generate a new version of requirements.in and requirements.txt and apply updated requirements.txt to venv.
```
make update
```

##Testing
To run pytest:
```
make pytest
```
Pytest is configured to also run black and flake8.  Formatting failures are treated as test failures.

## Building the docker image 
To build a docker image run:
```
make image
```
To update environment, run pytest and build a new image run:
```
make build
```
##Running Overwatch as a Docker container
After building the docker image use:
```
make run CREDENTIAL_VOLUME_MOUNT=<location of service account file> DESTINATION_CREDENTIAL_FILENAME=<service_account_filename>.json SLACK_BOT_TOKEN=<slackbot_token>
```

To run the docker image with access to a shell prompt use:
```
make shell
```
