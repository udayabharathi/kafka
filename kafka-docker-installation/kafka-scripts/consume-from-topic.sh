usage() { echo "Usage: $0 [-t <topic name>] Optional [-F - Flag to consume from beginning]" 1>&2; exit 1; }

from_beginning=''

while getopts t:F flag
do
    case "${flag}" in
        t) topic=${OPTARG};;
	F) from_beginning='--from-beginning';;
	*) usage;;
    esac
done

shift $((OPTIND-1))

if [ -z "${topic}" ]; then
	usage
fi

docker exec -it kafka-broker \
	/bin/bash \
	/opt/bitnami/kafka/bin/kafka-console-consumer.sh \
            --bootstrap-server localhost:29092 \
            --topic ${topic} ${from_beginning}
