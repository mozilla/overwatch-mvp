IMAGE_BASE     ?= overwatch
IMAGE_VERSION  ?= latest
IMAGE_NAME     ?= $(IMAGE_BASE):$(IMAGE_VERSION)
CONTAINER_NAME ?= $(IMAGE_BASE)

all: pytest

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
		 ${IMAGE_NAME} 

shell:
	docker run -it --entrypoint=/bin/bash --rm --name ${CONTAINER_NAME} ${IMAGE_NAME}

