BINDIR=bin
BGPMON-BIN=bgpmon
BGPMOND-BIN=bgpmond
BGPMONSRC=cmd/bgpmon/bgpmon.go
BGPMONDSRC=cmd/bgpmond/bgpmond.go

all: build


makebindir:
	mkdir -p ${BINDIR}

${BINDIR}/${BGPMON-BIN}:
	vgo build -o ${BINDIR}/${BGPMON-BIN} ${BGPMONSRC}

${BINDIR}/${BGPMOND-BIN}:
	vgo build -o ${BINDIR}/${BGPMOND-BIN} ${BGPMONDSRC}

${BINDIR}/${BGPMON-BIN}-linux:
	GOOS="linux" vgo build -o ${BINDIR}/${BGPMON-BIN}-linux ${BGPMONSRC}

${BINDIR}/${BGPMOND-BIN}-linux:
	GOOS="linux" vgo build -o ${BINDIR}/${BGPMOND-BIN}-linux ${BGPMONDSRC}

build: makebindir ${BINDIR}/${BGPMON-BIN} ${BINDIR}/${BGPMOND-BIN}

linux: makebindir ${BINDIR}/${BGPMON-BIN}-linux ${BINDIR}/${BGPMOND-BIN}-linux

clean:
	rm -rf ${BINDIR}
