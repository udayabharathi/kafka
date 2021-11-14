usage() { printf "Usage: $0 -t <topic name>\nOptional:\n[-F] - Consume from beginning\n[-g <consumer group name>]\n" 1>&2; exit 1; }

from_beginning=''
group=''

while getopts t:Fg: flag
do
    case "${flag}" in
        t) topic=${OPTARG};;
	F) from_beginning='--from-beginning';;
	g) group="--group ${OPTARG}";;
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
            --topic ${topic} \
	    ${from_beginning} \
	    ${group} \
	    --property print.key=true \
	    --property key.separator=" = "
