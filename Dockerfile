FROM python:3.11-slim

# Sistem deps (pandas/openpyxl üçün)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc libpq-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# kod
COPY . /app/

# nonroot (optional)
RUN useradd -m appuser
USER appuser

EXPOSE 8000

# uvicorn — 0.0.0.0 ilə açırıq ki şəbəkədən görünsün
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
