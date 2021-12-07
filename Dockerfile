FROM openjdk:8-slim

ARG HADOOP_VERSION=3.3.1
ARG METASTORE_VERSION=3.0.0
ARG PG_CONNECTOR_VERSION=42.3.1

RUN apt-get update && apt-get install -y curl --no-install-recommends \
	&& rm -rf /var/lib/apt/lists/*

# Download and extract the Hadoop binary package.
RUN curl https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION-aarch64.tar.gz \
	| tar xvz -C /opt/  \
	&& ln -s /opt/hadoop-$HADOOP_VERSION /opt/hadoop \
	&& rm -r /opt/hadoop/share/doc

# Add S3a jars to the classpath using this hack.
RUN ln -s /opt/hadoop/share/hadoop/tools/lib/hadoop-aws* /opt/hadoop/share/hadoop/common/lib/ && \
	ln -s /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk* /opt/hadoop/share/hadoop/common/lib/

# Set necessary environment variables.
ENV HADOOP_HOME="/opt/hadoop"
ENV PATH="/opt/spark/bin:/opt/hadoop/bin:${PATH}"

# Download and install the standalone metastore binary.
RUN curl -L https://repo1.maven.org/maven2/org/apache/hive/hive-standalone-metastore/$METASTORE_VERSION/hive-standalone-metastore-$METASTORE_VERSION-bin.tar.gz  \
	| tar xvz -C /opt/ \
	&& ln -s /opt/apache-hive-metastore-$METASTORE_VERSION-bin /opt/hive-metastore

# Download and install the postgres connector.
RUN curl -L https://jdbc.postgresql.org/download/postgresql-$PG_CONNECTOR_VERSION.jar --output /opt/postgresql-$PG_CONNECTOR_VERSION.jar \
	&& ln -s /opt/postgresql-$PG_CONNECTOR_VERSION.jar /opt/hadoop/share/hadoop/common/lib/ \
	&& ln -s /opt/postgresql-$PG_CONNECTOR_VERSION.jar /opt/hive-metastore/lib/

ADD conf/metastore-site.xml /opt/hive-metastore/conf/
ADD conf/metastore-log4j2.properties /opt/hive-metastore/conf
ADD entrypoint.sh /opt/hive-metastore/bin/entrypoint.sh
RUN chmod 755 /opt/hive-metastore/bin/entrypoint.sh

CMD /opt/hive-metastore/bin/entrypoint.sh
