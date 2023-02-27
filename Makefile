IMAGE_BASE     := overwatch
IMAGE_VERSION  := $(if $(IMAGE_VERSION),$(IMAGE_VERSION),$(shell whoami)-dev)
IMAGE_NAME     := $(IMAGE_BASE):$(IMAGE_VERSION)
TEST_IMAGE_NAME:= $(IMAGE_BASE):latest-test
CONTAINER_NAME := $(IMAGE_BASE)
IMAGE_REPO	   := gcr.io/automated-analysis-dev

.PHONY: update_deps

all: build

update_deps:
	./update_deps

build: update_deps local_test image

# Local development and testing including running with a local Airflow instance.
local_test:
	PYTHONPATH=. pytest --cache-clear tests analysis

image:
	docker build --no-cache -t ${IMAGE_NAME} --target=app -f Dockerfile .

dev_push:
	docker tag ${IMAGE_NAME} ${IMAGE_REPO}/${IMAGE_NAME}
	docker push ${IMAGE_REPO}/${IMAGE_NAME}

run:
	docker run -v ${CREDENTIAL_VOLUME_MOUNT}:/app/credentials \
		-e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/${DESTINATION_CREDENTIAL_FILENAME} \
		-e SLACK_BOT_TOKEN=${SLACK_BOT_TOKEN} \
		-e DEV_REPORT_SLACK_CHANNEL=overwatch-mvp \
		--name ${CONTAINER_NAME} \
		--rm \
		 ${IMAGE_NAME} run-analysis ./config_files --date=$(RUN_DATE)

shell:
	docker run -it --entrypoint=/bin/bash --rm --name ${CONTAINER_NAME} ${IMAGE_NAME}

stop:
	docker stop ${CONTAINER_NAME}

# circleCI tasks
test_image: ## Builds test Docker image containing all dev requirements
	docker build --no-cache -t ${TEST_IMAGE_NAME} --target=test -f Dockerfile .

ci_test: test_image ## Builds test Docker image and executes Python tests
	docker run ${TEST_IMAGE_NAME} python -m pytest tests analysis
