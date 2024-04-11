# 1. Base image
FROM python:3.10-slim

ARG APP_ENV='dev'

# Python environment variables
ENV APP_DIR='/project' \
    APP_ENV=${APP_ENV}

WORKDIR "$APP_DIR"

ENV PYTHONPATH "${PYTHONPATH}:${APP_DIR}"

# Copy Python project files
COPY ./main.py .
COPY ./requirements.txt .
COPY ./utils/ ${APP_DIR}/utils/
COPY ./protocols/ ${APP_DIR}/protocols/

# Install Python packages, Node.js, TypeScript, and build essentials including gcc
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt