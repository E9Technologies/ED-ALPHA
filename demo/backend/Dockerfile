# Backend service image for FastAPI
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system deps only once for smaller layers
RUN apt-get update \ 
    && apt-get install -y --no-install-recommends build-essential libpq-dev \ 
    && rm -rf /var/lib/apt/lists/*

COPY demo/backend/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY demo/backend/ .

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
