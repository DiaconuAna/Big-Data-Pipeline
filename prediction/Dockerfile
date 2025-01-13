FROM python:3.10-slim

# Copy your code and ML model into the container
COPY . /app
WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    libsasl2-dev \
    libsasl2-modules \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "prediction.py"]
