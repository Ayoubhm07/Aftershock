FROM python:3.11-slim

WORKDIR /opt/app

COPY docker/notebook/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY src /opt/app/src

ENV PYTHONPATH=/opt/app \
    PYTHONUNBUFFERED=1
