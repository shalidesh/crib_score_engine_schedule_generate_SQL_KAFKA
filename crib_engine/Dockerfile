FROM python:3.10-slim-bullseye

RUN apt-get clean \
  && apt-get update --fix-missing \
  && apt-get install -y \
    pkg-config \
    default-libmysqlclient-dev \
    gcc \
    build-essential \
    curl \
    unixodbc \
    unixodbc-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY ./requirements.txt /app
RUN pip install --no-cache-dir --requirement /app/requirements.txt
COPY . /app

EXPOSE 5000

CMD ["python3", "server.py"]
