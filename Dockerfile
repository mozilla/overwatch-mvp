ARG PYTHON_VERSION=3.10
FROM python:${PYTHON_VERSION} as base

MAINTAINER Glenda Leonard <gleonard@mozilla.com>

ARG PIP_VERSION=22.0.3

RUN apt-get update && \
    apt-get install -y --no-install-recommends wkhtmltopdf && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.txt
RUN python -m pip install --no-cache-dir --upgrade pip==${PIP_VERSION} \
    && python -m pip install --no-cache-dir -r requirements.txt

# test stage used for running tests only
FROM base AS test
COPY . .
RUN python -m pip install -e ".[testing]"

# final stage for running in prod
FROM base AS app
RUN mkdir -p /app
WORKDIR /app
ENV PYTHONPATH "${PYTHONPATH}:/app"
COPY . .
RUN python -m pip install --no-cache-dir .

ENTRYPOINT ["overwatch"]
CMD ["run-analysis", "./config_files"]
