FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y libmariadb-dev gcc tzdata && \
    ln -sf /usr/share/zoneinfo/US/Pacific /etc/localtime && \
    echo "US/Pacific" > /etc/timezone

RUN apt-get update && apt-get install -y libmariadb-dev gcc

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
