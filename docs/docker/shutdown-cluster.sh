#!/bin/bash

#parse input flags
DATA_DIR=""
CONTAINER_PREFIX="CAS"
while getopts 'd:hp:' flag; do
	case "${flag}" in
		d)
			DATA_DIR="${OPTARG}"
			;;
		p)
			CONTAINER_PREFIX="${OPTARG}"
			;;
		h)
			echo "Usage: $0 -d DATA_DIRECTORY [-p CONTAINER_PREFIX]"
			exit 0
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

echo "DATA DIRECTORY: $DATA_DIR"

#loop through container names cas*
for VAR in $(ls -d $DATA_DIR/$CONTAINER_PREFIX* | xargs -n1 basename)
do
	echo "CLOSING $VAR"
	docker stop $VAR > /dev/null
	docker rm $VAR > /dev/null

	CID=$(docker run -ti -v $DATA_DIR:/raid_data debian rm -r /raid_data/$VAR)
done

sleep 2

docker ps -a | grep Exited | awk '{print $1}' | xargs --no-run-if-empty docker rm > /dev/null
