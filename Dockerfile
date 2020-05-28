FROM python:3
ENV PYTHONUNBUFFERED 1
RUN mkdir /code
WORKDIR /code
COPY requirements.txt /code/
RUN pip install -r requirements.txt
COPY . /code/
COPY sample.tsv /data/appsinstalled/
RUN gzip -c /data/appsinstalled/sample.tsv > /data/appsinstalled/sample.tsv.gz