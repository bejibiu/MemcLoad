FROM python:3
ENV PYTHONUNBUFFERED 1
RUN mkdir /code
WORKDIR /code
COPY requirements.txt /code/
RUN pip install -r requirements.txt
COPY memc_load.py /code/
COPY appsinstalled_pb2.py /code/
COPY appsinstalled.proto /code/
COPY sample.tsv /data/appsinstalled/
RUN gzip -c /data/appsinstalled/sample.tsv > /data/appsinstalled/sample.tsv.gz