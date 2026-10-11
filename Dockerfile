FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV RUN_ENV=docker
ENV PYTHONUNBUFFERED=1

CMD ["python", "peer_node.py", "A"]
