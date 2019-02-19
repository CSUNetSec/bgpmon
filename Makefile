BINDIR=bin
BGPMON-BIN=$(BINDIR)/bgpmon
BGPMOND-BIN=$(BINDIR)/bgpmond
BGPMON-SRC=cmd/bgpmon
BGPMOND-SRC=cmd/bgpmond

ENV=GO111MODULE=on
LINUX-ENV=GO111MODULE=on GOOS="linux"
CC=go build

all: build

makebindir:
	mkdir -p ${BINDIR}

bgpmon:
	$(ENV) $(CC) -o $(BGPMON-BIN) $(BGPMON-SRC)/*.go

bgpmond:
	$(ENV) $(CC) -o $(BGPMOND-BIN) $(BGPMOND-SRC)/*.go

bgpmon-linux:
	$(LINUX-ENV) $(CC) -o $(BGPMON-BIN)-linux $(BGPMON-SRC)/*.go

bgpmond-linux:
	$(LINUX-ENV) $(CC) -o $(BGPMOND-BIN)-linux $(BGPMOND-SRC)/*.go

%_test: %
	go test $</*.go

build: makebindir bgpmon bgpmond

linux: makebindir bgpmon-linux bgpmond-linux

clean:
	rm -rf ${BINDIR}

test: db_test util_test
