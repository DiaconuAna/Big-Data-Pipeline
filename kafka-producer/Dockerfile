# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Copy the spark-consumer folder into the container
COPY . /app
WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run main.py when the container launches
CMD ["python", "main.py"]
