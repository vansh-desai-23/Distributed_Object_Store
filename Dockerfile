FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY object_store object_store
COPY migrations migrations
COPY alembic.ini .
CMD ["uvicorn", "object_store.worker:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
