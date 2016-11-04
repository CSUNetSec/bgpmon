BINOUT=bgpmon
BINDOUT=bgpmond

all: build

bgpmon: 
	cd cmd/bgpmon;\
	go build -o ../../${BINOUT} || (echo "running go get"; go get; go get -u; go build -o ../../${BINOUT});\
	cd ../../;

bgpmond:
	cd cmd/bgpmond;\
	go build -o ../../${BINDOUT} || (echo "running go get"; go get; go get -u; go build -o ../../${BINDOUT});\
	cd ../../;

bgpmon-linux: 
	cd cmd/bgpmon;\
	GOOS="linux" go build -o ../../${BINOUT}-linux || (echo "running go get"; go get; go get -u; go build -o ../../${BINOUT}-linux);\
	cd ../../;

bgpmond-linux:
	cd cmd/bgpmond;\
	GOOS="linux" go build -o ../../${BINDOUT}-linux || (echo "running go get"; go get; go get -u; go build -o ../../${BINDOUT}-linux);\
	cd ../../;

build: bgpmon bgpmond

linux: bgpmon-linux bgpmond-linux

clean:
	rm -f bgpmon; \
	rm -f bgpmond;
