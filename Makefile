.PHONY: all build clean

all: clean build

build:
	go build github.com/mesosphere/performance/api/cmd/api-server
	go build github.com/mesosphere/performance/monitor/cmd/systemd-monitor
	go build github.com/mesosphere/performance/connectors/graphite


clean:
	rm -rf ./build
