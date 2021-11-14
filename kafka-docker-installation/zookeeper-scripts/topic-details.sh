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

docker exec -it zookeeper \
        /opt/bitnami/zookeeper/bin/zkCli.sh -server localhost:2181 \
	get /brokers/topics/${topic} | grep -v 'INFO' | grep -v 'Exiting JVM with code 0'
