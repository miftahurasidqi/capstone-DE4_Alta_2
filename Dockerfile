# https://github.com/dbt-labs/dbt-core/blob/main/docker/Dockerfile

ARG build_for=linux/amd64

FROM --platform=$build_for python:3.10.7-slim-bullseye as base

RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
    git \
    ssh-client \
    software-properties-common \
    make \
    build-essential \
    ca-certificates \
    libpq-dev \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

RUN python -m pip install --upgrade pip setuptools wheel --no-cache-dir

WORKDIR /usr/app

FROM base as dbt-bigquery
COPY requirments.txt requirments.txt
RUN python -m pip install -r requirments.txt