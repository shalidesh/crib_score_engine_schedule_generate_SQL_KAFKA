FROM python:3.10-slim-bullseye

RUN apt-get clean \
  && apt-get update --fix-missing \
  && apt-get install -y \
    unixodbc \
    unixodbc-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY ./requirements.txt /app
RUN pip install --no-cache-dir --requirement /app/requirements.txt
COPY . /app

CMD ["python", "producer.py"]
