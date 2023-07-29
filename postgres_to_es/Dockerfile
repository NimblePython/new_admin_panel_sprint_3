FROM python:3.10-slim

WORKDIR /opt/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.txt requirements.txt

RUN apt update && apt install -y gcc
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . /opt/app

CMD ["python", "main.py"]
