usage() { printf "Usage: $0 -t <topic name>\n" 1>&2; exit 1; }

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

docker exec -it kafka-broker \
	/bin/bash \
	/opt/bitnami/kafka/bin/kafka-topics.sh \
            --bootstrap-server localhost:9092 \
	    --delete \
            --topic ${topic}
