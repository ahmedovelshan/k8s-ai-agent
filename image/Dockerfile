FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY detector.py collector.py orchestrator.py .

CMD ["python", "detector.py"]
