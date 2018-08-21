BINDIR=bin
BGPMON-BIN=bgpmon
BGPMOND-BIN=bgpmond
BGPMONSRC=cmd/bgpmon/
BGPMONDSRC=cmd/bgpmond/

all: build


makebindir:
	mkdir -p ${BINDIR}

${BINDIR}/${BGPMON-BIN}:
	cd ${BGPMONSRC}; \
	vgo build; \
	mv ${BGPMON-BIN} ../../${BINDIR}; \
	cd ../..

${BINDIR}/${BGPMOND-BIN}:
	cd ${BGPMONDSRC}; \
	vgo build; \
	mv ${BGPMOND-BIN} ../../${BINDIR}; \
	cd ../..

${BINDIR}/${BGPMON-BIN}-linux:
	cd ${BGPMONSRC}; \
	GOOS="linux" vgo build; \
	mv ${BGPMON-BIN} ../../${BINDIR}/${BGPMON-BIN}-linux; \
	cd ../..

${BINDIR}/${BGPMOND-BIN}-linux:
	cd ${BGPMONDSRC}; \
	GOOS="linux" vgo build; \
	mv ${BGPMOND-BIN} ../../${BINDIR}/${BGPMOND-BIN}-linux; \
	cd ../..

build: makebindir ${BINDIR}/${BGPMON-BIN} ${BINDIR}/${BGPMOND-BIN}

linux: makebindir ${BINDIR}/${BGPMON-BIN}-linux ${BINDIR}/${BGPMOND-BIN}-linux

clean:
	rm -rf ${BINDIR}
