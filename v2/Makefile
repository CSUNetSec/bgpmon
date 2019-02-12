BINDIR=bin
BGPMON-BIN=bgpmon
BGPMOND-BIN=bgpmond
BGPMOND-LITE-BIN=bgpmond-lite
BGPMONSRC=cmd/bgpmon/
BGPMONDSRC=cmd/bgpmond/
BGPMOND-LITESRC=cmd/bgpmond-lite/
GO111MODULE=on

all: build


makebindir:
	mkdir -p ${BINDIR}

${BINDIR}/${BGPMON-BIN}:
	cd ${BGPMONSRC}; \
	GO111MODULE=on go build; \
	mv ${BGPMON-BIN} ../../${BINDIR}; \
	cd ../..

${BINDIR}/${BGPMOND-BIN}:
	cd ${BGPMONDSRC}; \
	GO111MODULE=on go build; \
	mv ${BGPMOND-BIN} ../../${BINDIR}; \
	cd ../..

${BINDIR}/${BGPMOND-LITE-BIN}:
	cd ${BGPMOND-LITESRC}; \
	GO111MODULE=on go build; \
	mv ${BGPMOND-LITE-BIN} ../../${BINDIR}; \
	cd ../..

${BINDIR}/${BGPMON-BIN}-linux:
	cd ${BGPMONSRC}; \
	GOOS="linux" go build; \
	mv ${BGPMON-BIN} ../../${BINDIR}/${BGPMON-BIN}-linux; \
	cd ../..

${BINDIR}/${BGPMOND-BIN}-linux:
	cd ${BGPMONDSRC}; \
	GOOS="linux" go build; \
	mv ${BGPMOND-BIN} ../../${BINDIR}/${BGPMOND-BIN}-linux; \
	cd ../..

build: makebindir ${BINDIR}/${BGPMON-BIN} ${BINDIR}/${BGPMOND-BIN} ${BINDIR}/${BGPMOND-LITE-BIN}

linux: makebindir ${BINDIR}/${BGPMON-BIN}-linux ${BINDIR}/${BGPMOND-BIN}-linux

clean:
	rm -rf ${BINDIR}
