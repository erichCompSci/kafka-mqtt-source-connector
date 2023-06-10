FROM alpine:latest AS builder
COPY kafka_install.sh /bin/
RUN apk update \
  && apk add --no-cache curl bash jq openjdk8-jre
RUN chmod u+x /bin/kafka_install.sh
RUN /bin/kafka_install.sh
RUN mkdir -p /usr/share/java/kafka
COPY ./target/kafka-mqtt-source-connector-1.0-SNAPSHOT-jar-with-dependencies.jar /usr/share/java/kafka/
COPY ./src/main/resources/source-connect-mqtt.properties /opt/kafka/config
RUN sed -i 's/localhost/kafka_main/' /opt/kafka/config/connect-standalone.properties
RUN echo 'plugin.path=/usr/share/java/kafka' >> /opt/kafka/config/connect-standalone.properties


#COPY kafka_start.sh /bin/
#RUN chmod u+x /bin/kafka_start.sh
#RUN mkdir /res
#COPY res/ /res
#CMD [ "/bin/kafka_start.sh" ]
