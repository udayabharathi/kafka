docker exec -it kafka-broker \
	/bin/bash \
	/opt/bitnami/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server localhost:29092 \
		--describe \
		--all-groups
