## ------------------------------- Builder Stage ------------------------------ ## 
FROM python:3.13-bookworm AS builder

RUN apt-get update && apt-get install --no-install-recommends -y \
        build-essential && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Download the latest installer, install it and then remove it
ADD https://astral.sh/uv/install.sh /install.sh
RUN chmod -R 755 /install.sh && /install.sh && rm /install.sh

# Set up the UV environment path correctly
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app

# Copy only the requirements file initially
COPY requirements.txt .

# Install dependencies using UV
RUN uv pip install -r requirements.txt --system

## ------------------------------- Production Stage ------------------------------ ##
FROM python:3.13-slim-bookworm AS production

# Install PostgreSQL client libraries needed for psycopg
RUN apt-get update && apt-get install -y \
    libpq-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Create a non-root user for the application
RUN useradd --create-home appuser

WORKDIR /app

# Copy application code
COPY . .

# Copy installed dependencies from builder stage
COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Set non-sensitive environment variables with defaults
ENV REDIS_HOST="redis"
ENV REDIS_PORT="6379"
ENV REDIS_DB="0"
ENV REDIS_STREAM="1"
ENV REDIS_STREAM_NAME="webhooks"
ENV REDIS_STREAM_GROUP="webhook_processors"
ENV REDIS_STREAM_CONSUMER="consumer1"

# Sensitive variables that should be provided at runtime
ENV REDIS_PASSWORD=""
ENV DB_HOST="vps.uvepom.com"
ENV DB_PORT="5432"
ENV DB_NAME="postgres"
ENV DB_USER="postgres"
ENV DB_PASSWORD=""

# Switch to non-root user
USER appuser

# Start the consumer service
CMD ["python", "consumer.py"] 