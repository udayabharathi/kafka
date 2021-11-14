docker exec -it zookeeper \
	/opt/bitnami/zookeeper/bin/zkCli.sh -server localhost:2181 \
	ls /brokers/ids | grep -v 'INFO' | grep -v 'Exiting JVM with code 0'
