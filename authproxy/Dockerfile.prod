FROM python:3.11 AS deps

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
ENV POETRY_VERSION 1.6.1

WORKDIR /tmp

RUN pip install "poetry==$POETRY_VERSION"

COPY pyproject.toml poetry.lock ./

RUN poetry export -f requirements.txt --without-hashes --without dev > requirements.txt

FROM python:3.11 AS prod

WORKDIR /tmp

COPY --from=deps /tmp/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app

COPY ./authproxy/ ./authproxy/
COPY ./*.py ./

ENV PORT 8088

CMD [ "/bin/sh", "-c",  "uvicorn main:app --host 0.0.0.0 --port $PORT --proxy-headers" ]
