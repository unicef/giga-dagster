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

CMD [ \
    "hypercorn", \
    "main:app", \
    "--bind", "0.0.0.0:3001", \
    "--log-file", "-", \
    "--access-logfile", "-", \
    "--workers", "1", \
    "--reload", \
    "--debug" \
]
