FROM ubuntu:18.04
RUN apt-get update
RUN  apt-get install -y ant
RUN  apt-get install -y \
       libboost-dev libboost-test-dev \
       libboost-program-options-dev \
       libboost-filesystem-dev \
       libboost-thread-dev \
       libevent-dev \ 
       automake \
       libtool \
       flex \ 
       bison \ 
       pkg-config \
       g++ \ 
       libssl-dev

RUN apt-get install -y curl
RUN curl -L https://dlcdn.apache.org/thrift/0.15.0/thrift-0.15.0.tar.gz \
      | tar -xvz -C /var/lib

WORKDIR /var/lib/thrift-0.15.0
RUN sh ./bootstrap.sh
RUN ./configure
RUN make
RUN make install

RUN apt-get install -y wget

RUN mkdir -p /root/hive-metastore
RUN mkdir -p /root/hive-metastore/share/fb303/if

WORKDIR /root/hive-metastore

RUN wget https://raw.githubusercontent.com/apache/hive/branch-3.0/standalone-metastore/src/main/thrift/hive_metastore.thrift
RUN wget https://raw.githubusercontent.com/apache/thrift/master/contrib/fb303/if/fb303.thrift
RUN cp fb303.thrift share/fb303/if

RUN thrift --gen py fb303.thrift
RUN thrift --gen py hive_metastore.thrift

RUN tar -cvf /root/hive-metastore-python.tar /root/hive-metastore/
