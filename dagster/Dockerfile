FROM python:3.11-slim

# Standard Python flags
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

ARG POETRY_VERSION=1.6.1

# Move to a temporary directory
WORKDIR /tmp

# Install poetry and set to not create an additional virtualenv (we are already in a container)
RUN pip install "poetry==$POETRY_VERSION" && \
    poetry config virtualenvs.create false

# Copy package manifests inside the container
COPY pyproject.toml poetry.lock ./

# Install packages based on package manifests
# This is an automated process: do not ask for user input
RUN poetry install --no-interaction --no-ansi --with pipelines,dagster

# Move to the directory where we mount the source code (see: docker-compose.yml)
ENV DAGSTER_HOME /app

WORKDIR /app

CMD [ "dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-f", "./src/definitions.py" ]
