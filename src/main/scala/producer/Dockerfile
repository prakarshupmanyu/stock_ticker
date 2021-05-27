FROM openjdk:8
ENV SCALA_VERSION 2.12.13
ENV SCALA_TARBALL http://www.scala-lang.org/files/archive/scala-$SCALA_VERSION.deb
RUN apt-get update && \
DEBIAN_FRONTEND=noninteractive apt-get install -y --force-yes curl && \
echo "===> install Scala"&& \
DEBIAN_FRONTEND=noninteractive \
apt-get install -y --force-yes libjansi-java && \
curl -sSL $SCALA_TARBALL -o scala.deb && \
dpkg -i scala.deb && \
\
\
\
echo "===> clean up..."&& \
rm -f *.deb && \
apt-get remove -y --auto-remove curl && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*

WORKDIR /root
COPY ../../../../target/my*.jar /root/producer.jar

CMD ["scala", "-classpath", "/root/producer.jar", "producer.StockProducer"]