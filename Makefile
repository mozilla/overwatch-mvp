IMAGE_BASE     := overwatch
IMAGE_VERSION  := $(if $(IMAGE_VERSION),$(IMAGE_VERSION),$(shell whoami)-dev)
IMAGE_NAME     := $(IMAGE_BASE):$(IMAGE_VERSION)
CONTAINER_NAME := $(IMAGE_BASE)
IMAGE_REPO	   := gcr.io/automated-analysis-dev

all: build

update:
	./update_deps
	pip install -e ".[testing]"

pytest:
	PYTHONPATH=. pytest --cache-clear tests analysis

image:
	docker build -t ${IMAGE_NAME} .

build: update pytest image 

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

push:
	docker tag ${IMAGE_NAME} ${IMAGE_REPO}/${IMAGE_NAME}
	docker push ${IMAGE_REPO}/${IMAGE_NAME}