FROM python:3.8
RUN apt-get update
RUN apt-get -y install vim
RUN mkdir /app
RUN pwd
RUN ls /bin/
WORKDIR /app
COPY . /app
RUN pip3 install --no-cache-dir -r requirements.txt
ENV PYTHONUNBUFFERED=1
#EXPOSE 5000
CMD [ "python", "main_kafka.py",                          \
      "-t", "ztf_20210317_public1",                       \
      "-s", "public.alerts.ztf.uw.edu:9092",              \
      "--target", "kafka-junction-kafka-bootstrap:9092",   \
      "-g1", "antares_junction",                          \
      "-n", "32" ]
