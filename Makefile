.PHONY: all build clean

all: clean build 

build:
	bash -c "./scripts/build.sh supervisor"
	bash -c "./scripts/build.sh journald_scale_test"

clean:
	rm -rf ./build
	rm -rf ./schema/metrics_schema
