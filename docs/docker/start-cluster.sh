#!/bin/bash

#parse input flags
DATA_DIR=""
IPS=""
CONTAINER_PREFIX="CAS"
while getopts 'd:hi:p:' flag; do
	case "${flag}" in
		d)
			DATA_DIR="${OPTARG}"
			;;
		i) 
			IPS="${OPTARG}"
			;;
		h)
			echo "Usage: $0 -d DATA_DIRECTORY -i IP_LIST [-p CONTAINER_PREFIX]"
			exit 0
			;;
		p)
			CONTAINER_PREFIX="${OPTARG}"
			;;
		\?)
			echo "Invalid option: -$OPTARG" >&2
			exit 1
			;;
		:)
			echo "Option -$OPTARG requires an argument." >&2
			exit 1
			;;
	esac
done

#check if data directory and ip lists were supplied
if [[ -z "$DATA_DIR" ]]
then
	echo "No data directory supplied. Provide one with -d option."
	exit 1
fi

if [[ -z "$IPS" ]]
then
	echo "No ip list supplied. Porvide one with -i option."
	exit 1
fi

echo "DATA DIRECTORY: $DATA_DIR"
echo "IP LIST: $IPS"

#loop over ip lists to create docker images
CONTAINER=1
SEED_IP=""
SEED_CID=""
IP_LISTS=($IPS)
for IP_LIST in ${IP_LISTS[@]}
do
	echo "CONTAINER: $CONTAINER_PREFIX$CONTAINER"

	#parse ip addresses and hash tokens
	IFS=$','
	LIST=($IP_LIST)
	TOKENS=""
	for IP in ${LIST[@]} 
	do 
		TOKEN=`./murmur-hash -ip_addr $IP -begin_date 2001-01-01T00:00:00Z00:00 -num_days 7305`
		echo "    IP:$IP TOKEN:$TOKEN"
		
		if [[ -z "$TOKENS" ]]
		then
			TOKENS=$TOKEN
		else
			TOKENS="$TOKENS,$TOKEN"
		fi
	done
	
	#echo "    TOKENS:$TOKENS"
	unset IFS

	#start docker container
	if [[ -z "$SEED_IP" ]]
	then
		SEED_IP="10.0.0.$CONTAINER"
		SEED_CID=$(docker run --name $CONTAINER_PREFIX$CONTAINER -v $DATA_DIR/$CONTAINER_PREFIX$CONTAINER:/var/lib/cassandra/data -e CASSANDRA_INITIAL_TOKEN=\"$TOKENS\" -e CASSANDRA_RANGE_MOVEMENT=false -e CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch -d -e CASSANDRA_BROADCAST_ADDRESS=$SEED_IP -p $SEED_IP:7000:7000 -p $SEED_IP:7001:7001 -p $SEED_IP:7199:7199 -p $SEED_IP:9042:9042 -p $SEED_IP:9160:9160 hamersaw/cassandra:1.2)
		#SEED_IP=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' $SEED_CID)

		echo "    CONTAINER_IP:$SEED_IP"
		echo "    CONTAINER_ID:$SEED_CID"

		#wait for seed cassandra to initialize
		while [ "`docker exec -ti $SEED_CID cqlsh $SEED_IP -e \"exit;\"`" != "" ] || [ "`docker exec -ti $SEED_CID nodetool status | grep UJ`" != "" ]
		do
			sleep 1
		done
	else
		IP="10.0.0.$CONTAINER"
		CID=$(docker run --name $CONTAINER_PREFIX$CONTAINER -v $DATA_DIR/$CONTAINER_PREFIX$CONTAINER:/var/lib/cassandra/data -e CASSANDRA_INITIAL_TOKEN=\"$TOKENS\" -e CASSANDRA_RANGE_MOVEMENT=false -e CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch -e CASSANDRA_SEEDS=$SEED_IP -d -e CASSANDRA_BROADCAST_ADDRESS=$IP -p $IP:7000:7000 -p $IP:7001:7001 -p $IP:7199:7199 -p $IP:9042:9042 -p $IP:9160:9160 hamersaw/cassandra:1.2)
		#IP=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' $CID)

		echo "    CONTAINER_IP:$IP"
		echo "    CONTAINER_ID:$CID"
	fi

	echo ""
	#increment container number
	let CONTAINER+=1
done

#wait for seed gossip to settle down
while [ "`docker exec -ti $SEED_CID cqlsh $SEED_IP -e \"exit;\"`" != "" ] || [ "`docker exec -ti $SEED_CID nodetool status | grep UJ`" != "" ]
do
	sleep 1
done

#create cassandra schema from file
echo "initializing cassandra schemas in cluster"
tar c init-cassandra.cql | docker exec -i $SEED_CID tar x -C /root
docker exec -i $SEED_CID cqlsh $SEED_IP 9042 -f /root/init-cassandra.cql
