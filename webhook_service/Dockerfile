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

# Copy application code - make sure main.py is at the root
COPY . .
# Copy installed dependencies from builder stage
COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Set environment variables with default values
ENV DB_HOST="vps.uvepom.com"
ENV DB_PORT="5432"
ENV DB_NAME="postgres"
ENV DB_USER="postgres"
ENV REDIS_HOST="redis"
ENV REDIS_PORT="6379"
ENV REDIS_DB="0"
ENV REDIS_STREAM="1"
ENV REDIS_STREAM_NAME="webhooks"
ENV REDIS_STREAM_GROUP="webhook_processors"
ENV REDIS_STREAM_CONSUMER="consumer1"

ENV DB_PASSWORD=""
ENV REDIS_PASSWORD=""
ENV REDIS_USER=""

# Switch to non-root user
USER appuser

# Expose the FastAPI port
EXPOSE 8080

# Start the application
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--log-level", "info"]