FROM python:3.11-slim

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

ARG POETRY_VERSION=1.6.1

WORKDIR /tmp

RUN pip install "poetry==$POETRY_VERSION" && \
    poetry config virtualenvs.create false

COPY pyproject.toml poetry.lock ./

RUN poetry install --no-interaction --no-ansi

WORKDIR /app

CMD [ "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "3001", "--reload", "--workers", "1" ]
