all: build

protobuf/bgpmond.pb.go:
	protoc --go_out=plugins=grpc:. protobuf/bgpmond.proto

cmd/bgpmon: protobuf/bgpmond.pb.go
	go build -o bgpmon cmd/bgpmon/*

cmd/bgpmond: protobuf/bgpmond.pb.go
	go build -o bgpmond cmd/bgpmond/*

build: cmd/bgpmon cmd/bgpmond

clean:
	rm -f bgpmon; \
	rm -f bgpmond;

distclean: clean
	rm -f protobuf/bgpmond.pb.go;
