generate: build unpack clean

build:
	docker build -t hive-metastore-client/thrift-autogen:latest .

unpack: 
	docker run --rm --entrypoint cat hive-metastore-client/thrift-autogen  /root/hive-metastore-python.tar  > hive-metastore-python.tar && tar -xvf hive-metastore-python.tar && cp -r root/hive-metastore/gen-py/* . 

clean:
	rm -rf root/ hive-metastore-python.tar
