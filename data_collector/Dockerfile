FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    postgresql-client \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py ./
COPY credentials.json ./ 
COPY user_pb2.py ./
COPY user_pb2_grpc.py ./

RUN echo '#!/bin/bash' >> wait_for_db.sh \
    && echo 'host=$1' >> wait_for_db.sh \
    && echo 'user=$2' >> wait_for_db.sh \
    && echo 'db=$3' >> wait_for_db.sh \
    && echo 'until PGPASSWORD=$POSTGRES_PASSWORD psql -h $host -U $user -d $db -c "select 1" > /dev/null 2>&1; do' >> wait_for_db.sh \
    && echo '  echo "PostgreSQL del Data Collector non disponibile - attendo..."' >> wait_for_db.sh \
    && echo '  sleep 3' >> wait_for_db.sh \
    && echo 'done' >> wait_for_db.sh \
    && echo 'echo "PostgreSQL del Data Collector è disponibile."' >> wait_for_db.sh \
    && chmod +x wait_for_db.sh

EXPOSE 50052

ENV PYTHONUNBUFFERED=1

CMD ["/bin/bash", "-c", "/app/wait_for_db.sh db_collector collector_user collector_db && exec python server.py"]