.PHONY: all build clean

all: clean build

build:
	go build ./...


clean:
	rm -f ./api-server ./graphite ./systemd-monitor
