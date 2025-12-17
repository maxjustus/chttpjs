.PHONY: build build-full build-lz4 test fuzz bench bench-formats bench-profile publish

build: build-full build-lz4

build-full:
	npm run build:full

build-lz4:
	npm run build:lz4

test:
	npm test

fuzz:
	npm run test:fuzz

bench:
	npm run bench

bench-formats:
	node --experimental-strip-types bench/formats.ts

# Profile with CPU sampling: make bench-profile ARGS="-f native -o encode -d complex"
# Run ./scripts/profile.ts -h for options
ARGS ?=

bench-profile:
	@rm -f bench.cpuprofile
	@node --experimental-strip-types --cpu-prof --cpu-prof-name=bench.cpuprofile scripts/profile.ts $(ARGS)
	@node --experimental-strip-types scripts/profile-hotspots.ts bench.cpuprofile
	@rm -f bench.cpuprofile

publish:
	npm publish --access=public
