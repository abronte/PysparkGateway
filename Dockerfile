FROM python:2.7-stretch

# install java
RUN apt-get update && \
  apt-get install -y wget openjdk-8-jdk-headless

# install scala
RUN wget -q -O /tmp/scala.deb https://downloads.lightbend.com/scala/2.12.6/scala-2.12.6.deb && \
  dpkg -i /tmp/scala.deb

RUN pip install pyspark==2.4.0

ADD . /srv

WORKDIR /srv

RUN pip install -e .[dev]

ENV PYTHONPATH=/srv
