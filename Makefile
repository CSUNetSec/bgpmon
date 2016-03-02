all: build

protobuf/bgpmond.pb.go:
	protoc --go_out=plugins=grpc:. protobuf/bgpmond.proto

bgpmon: protobuf/bgpmond.pb.go
	cd cmd/bgpmon;\
	go build -o ../../bgpmon || (echo "running go get"; go get; go get -u; go build -o ../../bgpmon);\
	cd ../../

bgpmond: protobuf/bgpmond.pb.go
	cd cmd/bgpmond;\
	go build -o ../../bgpmond || (echo "running go get"; go get; go get -u; go build -o ../../bgpmond);\
	cd ../../

build: bgpmon bgpmond

clean:
	rm -f bgpmon; \
	rm -f bgpmond;

distclean: clean
	rm -f protobuf/bgpmond.pb.go;
