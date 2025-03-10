# Use an official Python image as the base
FROM python:3.10

# Set the working directory
WORKDIR /app

# Copy the project files
COPY . .

# Install dependencies
RUN pip install --upgrade pip
RUN pip install fastapi uvicorn kafka-python pyspark google-cloud-bigquery transformers

# Expose the FastAPI porthttps://duckduckgo.com/?q=Yahoo+finance+API&t=newext&atb=v468-5__&ia=web
EXPOSE 8000

# Start the FastAPI server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
