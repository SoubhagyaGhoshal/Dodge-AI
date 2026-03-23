FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Run the ingestion script to build the SQLite database
RUN python ingest.py

# Start the FastAPI app on port 7860 (default for Hugging Face Spaces)
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "7860"]
