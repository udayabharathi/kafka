usage() { echo "Usage: $0 [-b <broker id>]" 1>&2; exit 1; }

while getopts b: flag
do
    case "${flag}" in
        b) broker=${OPTARG};;
	*) usage;;
    esac
done

shift $((OPTIND-1))

if [ -z "${broker}" ]; then
	usage
fi

docker exec -it zookeeper \
        /opt/bitnami/zookeeper/bin/zkCli.sh -server localhost:2181 \
	get /brokers/ids/${broker} | grep -v 'INFO' | grep -v 'Exiting JVM with code 0'
