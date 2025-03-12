# Use a smaller, optimized Python image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy only the requirements file for better caching
COPY requirements.txt .

# Install system dependencies (needed for some Python packages)
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application files
COPY . .

# Ensure the .env file is included (if not ignored)
COPY .env .env

# Expose FastAPI's default port
EXPOSE 8000

# Run FastAPI with Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
