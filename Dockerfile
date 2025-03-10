# FROM python:3.10-slim

# WORKDIR /app

# COPY requirements.txt .
# RUN pip install -r requirements.txt

# COPY app/ .

# # Create data directories
# RUN mkdir -p /app/data/archive /app/data/current

# CMD ["python", "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]