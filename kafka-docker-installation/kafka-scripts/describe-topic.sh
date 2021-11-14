usage() { printf "Usage: $0\nOptional:\n[-t <topic name>]\n" 1>&2; exit 1; }

topic=''

while getopts t: flag
do
    case "${flag}" in
        t) topic="--topic ${OPTARG}";;
	*) usage;;
    esac
done

shift $((OPTIND-1))

docker exec -it kafka-broker \
	/bin/bash \
	/opt/bitnami/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		${topic} \
		--describe
