bgpmon: 
	cd cmd/bgpmon;\
	go build -o ../../bgpmon || (echo "running go get"; go get; go get -u; go build -o ../../bgpmon);\
	cd ../../;

bgpmond:
	cd cmd/bgpmond;\
	go build -o ../../bgpmond || (echo "running go get"; go get; go get -u; go build -o ../../bgpmond);\
	cd ../../;

build: bgpmon bgpmond

clean:
	rm -f bgpmon; \
	rm -f bgpmond;
