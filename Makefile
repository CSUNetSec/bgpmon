all: build

proto/bgpmond/bgpmond.pb.go:
	protoc --go_out=plugins=grpc:. proto/bgpmond/bgpmond.proto

cmd/bgpmon: proto/bgpmond/bgpmond.pb.go
	go build -o bgpmon cmd/bgpmon/*

cmd/bgpmond: proto/bgpmond/bgpmond.pb.go
	go build -o bgpmond cmd/bgpmond/*

build: cmd/bgpmon cmd/bgpmond

clean:
	rm -f bgpmon; \
	rm -f bgpmond;

distclean: clean
	rm -f proto/bgpmond/bgpmond.pb.go;
