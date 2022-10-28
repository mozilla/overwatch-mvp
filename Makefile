IMAGE_BASE     := overwatch
IMAGE_VERSION  := $(if $(IMAGE_VERSION),$(IMAGE_VERSION),$(shell whoami)-dev)
IMAGE_NAME     := $(IMAGE_BASE):$(IMAGE_VERSION)
TEST_IMAGE_NAME:= $(IMAGE_BASE):latest-test
CONTAINER_NAME := $(IMAGE_BASE)
IMAGE_REPO	   := gcr.io/automated-analysis-dev

all: build

update:
	./update_deps
	pip install -e ".[testing]"

build: update local_test image

# Local development and testing including running with a local Airflow instance.
local_test:
	PYTHONPATH=. pytest --cache-clear tests analysis

image:
	docker build -t ${IMAGE_NAME} --target=app -f Dockerfile .

dev_push:
	docker tag ${IMAGE_NAME} ${IMAGE_REPO}/${IMAGE_NAME}
	docker push ${IMAGE_REPO}/${IMAGE_NAME}
 
run:
	docker run -v ${CREDENTIAL_VOLUME_MOUNT}:/app/credentials \
		-e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/${DESTINATION_CREDENTIAL_FILENAME} \
		-e SLACK_BOT_TOKEN=${SLACK_BOT_TOKEN} \
		--name ${CONTAINER_NAME} \
		--rm \
		 ${IMAGE_NAME} 

shell:
	docker run -it --entrypoint=/bin/bash --rm --name ${CONTAINER_NAME} ${IMAGE_NAME}

stop:
	docker stop ${CONTAINER_NAME}

# circleCI tasks
test-image: ## Builds test Docker image containing all dev requirements
	docker build -t ${TEST_IMAGE_NAME} --target=test -f Dockerfile .

ci_test: test-image ## Builds test Docker image and executes Python tests
	docker run ${TEST_IMAGE_NAME} python -m pytest tests analysis


