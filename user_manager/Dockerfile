FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    postgresql-client \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

    
COPY wait-for-db.sh /wait-for-db.sh
COPY requirements.txt .
COPY ./proto ./proto
COPY ./server.py .
COPY ./models.py .
COPY ./db_user.py .
COPY ./user_pb2.py .
COPY ./user_pb2_grpc.py .

RUN chmod +x /wait-for-db.sh
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 50051

CMD ["/wait-for-db.sh", "python", "server.py"]