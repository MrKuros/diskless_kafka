FROM python:3.12-slim

WORKDIR /app

# Copy requirements if we had them, but we only have a few dependencies
RUN pip install --no-cache-dir minio psycopg2-binary kafka-python

COPY . /app/

# The server listens on 9092 internally
EXPOSE 9092

CMD ["python", "server.py"]
