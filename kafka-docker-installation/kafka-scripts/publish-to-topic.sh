usage() { printf "Usage: $0 -t <topic name>\nOptional:\n[-k <key separator>] - Produce with keys\n" 1>&2; exit 1; }

produce_with_keys=''
key_separator=''

while getopts t:k: flag
do
    case "${flag}" in
        t) topic=${OPTARG};;
	k) produce_with_keys="--property parse.key=true --property key.separator=${OPTARG}";key_separator=${OPTARG};;
	*) usage;;
    esac
done

shift $((OPTIND-1))

if [ -z "${topic}" ]; then
	usage
fi

printf "Type messages and press the return key to publish the same to kafka topic: '${topic}'\nPress CTRL+C to quit!\n"
if [ -n "${key_separator}" ]; then
	printf "Message format: \"<key>${key_separator}<message>\"\n"
fi

docker exec -it kafka-broker \
	/bin/bash \
	/opt/bitnami/kafka/bin/kafka-console-producer.sh \
            --bootstrap-server localhost:29092 \
	    ${produce_with_keys} \
            --topic ${topic}
