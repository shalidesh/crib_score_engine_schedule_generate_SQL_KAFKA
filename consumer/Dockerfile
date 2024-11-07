FROM python:3.10-slim-bullseye

RUN apt-get clean \
  && apt-get update --fix-missing 

WORKDIR /app
COPY ./requirements.txt /app
RUN pip install --no-cache-dir --requirement /app/requirements.txt
COPY . /app

CMD ["python", "consumer.py"]
