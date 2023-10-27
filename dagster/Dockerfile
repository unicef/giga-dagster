FROM python:3.11-bullseye

# Java flags
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

# Standard Python flags
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

ARG POETRY_VERSION=1.6.1

# Move to a temporary directory
WORKDIR /tmp

# Update packages & install JDK
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk-headless && \
    apt-get clean

# Install poetry,
# set to not create an additional virtualenv (we are already in a container),
# enable parallel installs
RUN pip install "poetry==$POETRY_VERSION" && \
    poetry config virtualenvs.create false && \
    poetry config installer.max-workers 4

# Copy package manifests inside the container
COPY pyproject.toml poetry.lock ./

# Install packages based on package manifests
# This is an automated process: do not ask for user input
RUN poetry install --no-interaction --no-ansi --with pipelines,dagster

# Move to the directory where we mount the source code (see: docker-compose.yml)
WORKDIR /app

# Run the development server and listen on the specified port
CMD [ "dagster", "dev", "-h", "0.0.0.0", "-p", "3002" ]
