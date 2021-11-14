docker exec -it kafka-broker \
	/bin/bash \
	/opt/bitnami/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--describe
