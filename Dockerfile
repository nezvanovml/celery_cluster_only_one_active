FROM python:3.12-bullseye

RUN mkdir -p /srv
COPY requirements.txt /srv/requirements.txt
WORKDIR /srv
RUN pip3 install -r requirements.txt
COPY ./app /srv/app
WORKDIR /srv
ENV PYTHONIOENCODING=utf8
ENV PYTHONPATH=/srv
