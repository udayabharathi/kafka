usage() { echo "Usage: $0 [-t <topic name>]" 1>&2; exit 1; }

while getopts t: flag
do
    case "${flag}" in
        t) topic=${OPTARG};;
	*) usage;;
    esac
done

shift $((OPTIND-1))

if [ -z "${topic}" ]; then
	usage
fi

printf "Type messages and press the return key to publish the same to kafka topic: '${topic}'\nPress CTRL+C to quit!\n"

docker exec -it kafka-broker \
	/bin/bash \
	/opt/bitnami/kafka/bin/kafka-console-producer.sh \
            --bootstrap-server localhost:29092 \
            --topic ${topic}
