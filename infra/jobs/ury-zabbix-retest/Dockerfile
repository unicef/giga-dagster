FROM python:3.12-slim

RUN pip install --no-cache-dir requests==2.32.3 \
 && useradd --uid 10001 --no-create-home app

WORKDIR /app
COPY zabbix_client.py retest_connection.py ./

USER 10001
ENTRYPOINT ["python", "-u", "/app/retest_connection.py"]
