# Local Development

Use python version **3.10** for development.

`pyenv` is the recommended method to install and manage python versions.

## Python Setup
```
python -m venv venv/
source venv/bin/activate
python -m pip install pip-tools
python -m pip install -e ".[testing]"
pre-commit install
```

## Developing in PyCharm
### Run/Debug Configurations
1. Select `Module name:` from the drop list for the first field and enter `analysis.cli`
1. In the `Parameters:` field enter `run-analysis --date=2022-11-15 ./config_files`  (any date can be specified)
1. In the `Environment variables:` field enter

       PYTHONUNBUFFERED=1;SLACK_BOT_TOKEN=<slackbot_token>;GOOGLE_APPLICATION_CREDENTIALS=<service account file>;DEV_REPORT_SLACK_CHANNEL=overwatch-mvp
    Contact gleonard@mozilla.com for SLACK_BOT_TOKEN
## Updating Dependencies
**DO NOT UPDATE requirements.txt or requirements.in manually!**

1. If you have not set up your local environment run the steps described above.

2.  Activate your local environment in not already activated.
```
source venv/bin/activate
```
3. Make required changes to `pyproject.toml`

4. Generate a new version of requirements.in and requirements.txt and apply updated requirements.txt to venv.
```
make update_deps
```

## Testing
To run pytest:
```
make pytest
```
Pytest is configured to also run black and flake8.  Formatting failures are treated as test failures.

# Docker
## Setting Image Version
When building the docker image set the following environment variable to indicate the version
```
IMAGE_VERSION=<version>
```
If IMAGE_VERSION is not set the default value is `<username>-dev` (e.g. gleonard-dev) is used

## Building the Docker Image
To build a docker image run:
```
make image
```
To update environment, run pytest and build a new image run:
```
make build
```
## Running Locally
After building the docker image, use the following command to launch the container.  `make run` is
configured to publish reports to the development `#overwatch-mvp` Slack channel instead of the production
Slack channel `#overwatch-reports`
```
make run RUN_DATE=<YYYY-MM-DD> CREDENTIAL_VOLUME_MOUNT=<location of service account file> DESTINATION_CREDENTIAL_FILENAME=<service_account_filename>.json SLACK_BOT_TOKEN=<slackbot_token>
```

To run the docker image with access to a shell prompt use (generally for debugging purposes):
```
make shell
```

To stop the docker container:
```
make stop
```
# Running via Airflow
Testing Overwatch with Airflow can be accomplished by running Airflow locally.
Follow the steps outlined in https://mana.mozilla.org/wiki/pages/viewpage.action?spaceKey=SRE&title=WTMO+Developer+Guide
to set up Airflow

1. The Container Registry in the `automated-analysis-dev` project has been enabled (https://console.cloud.google.com/gcr/images/automated-analysis-dev?project=automated-analysis-dev).
    This is where development images are pushed and pulled.  To push a development docker image use (see IMAGE_VERSION notes above).
```
make dev_push
```
2.  Launch airflow and create gke  (see https://mana.mozilla.org/wiki/pages/viewpage.action?spaceKey=SRE&title=WTMO+Developer+Guide)
3.  Create the following Variables in Airflow:
    - `overwatch_slack_token` and set the value to the Slack token (contact gleonard@mozilla.com for access).
    - `overwatch_image_version` and set to the value of IMAGE_VERSION or `<username>-dev` if IMAGE_VERSION is not set
4.  Update your local copy of overwatch.py DAG in `telemetry-airflow` with the following changes:
    1. image repository - replace `moz-fx-data-airflow-prod-88e0` with `automated-analysis-dev`
    1. report slack channel - add `"DEV_REPORT_SLACK_CHANNEL" "overwatch-mvp"` to `env_vars` dict for `GKEPodOperator`
    1. add GCP dev GKE cluster settings to `GKEPodOperator` (replacing <username> with your username:
        1. gcp_conn_id="google_cloud_gke_sandbox",
        1. project_id="moz-fx-data-gke-sandbox",
        1. cluster_name="<username>-gke-sandbox",
        1. location="us-west1",
