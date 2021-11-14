usage() { echo "Usage: $0 [-t <topic name>] [-p <partitions>] [-r <replication factor>]" 1>&2; exit 1; }

while getopts t:p:r: flag
do
    case "${flag}" in
        t) topic=${OPTARG};;
        p) partitions=${OPTARG};;
        r) replication_factor=${OPTARG};;
	*) usage;;
    esac
done

shift $((OPTIND-1))

if [ -z "${topic}" ] || [ -z "${partitions}" ] || [ -z "${replication_factor}" ]; then
	usage
fi

docker exec -it kafka-broker \
	/bin/bash \
	/opt/bitnami/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create \
		--topic ${topic} \
		--partitions ${partitions} \
		--replication-factor ${replication_factor}
