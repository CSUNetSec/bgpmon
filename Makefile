all: build

netsec-protobufs:
	git clone https://github.com/CSUNetSec/netsec-protobufs;

build-proto: netsec-protobufs
	mkdir -p pb;
	protoc --proto_path=netsec-protobufs/ --go_out=plugins=grpc,import_path=pb:netsec-protobufs/ netsec-protobufs/protocol/bgp/bgp.proto netsec-protobufs/common/common.proto netsec-protobufs/bgpmon/bgpmon.proto;
	find ./netsec-protobufs -name "*.go" -exec cp {} pb/ \;

bgpmon: build-proto
	cd cmd/bgpmon;\
	go build -o ../../bgpmon;\
	#go build -o ../../bgpmon || (echo "running go get"; go get; go get -u; go build -o ../../bgpmon);\
	cd ../../

bgpmond: build-proto
	cd cmd/bgpmond;\
	go build -o ../../bgpmond;\
	#go build -o ../../bgpmond || (echo "running go get"; go get; go get -u; go build -o ../../bgpmond);\
	cd ../../

build: bgpmon bgpmond

clean:
	rm -f bgpmon; \
	rm -f bgpmond;

distclean: clean
	rm -f pb/*;
