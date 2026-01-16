.PHONY: build test test-tcp fuzz fuzz-quick bench bench-formats bench-profile profile-complex profile-variant profile-dynamic profile-json publish update-settings

build:
	npm run build

test: build
	npm test

test-tcp: build
	npm run test:tcp

fuzz: build
	npm run test:fuzz

fuzz-quick: build
	npm run test:fuzz:quick

format:
	npm run format

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

profile-complex:
	$(MAKE) bench-profile ARGS="-f native -o encode -d bench-complex"

profile-variant:
	$(MAKE) bench-profile ARGS="-f native -o encode -d variant"

profile-dynamic:
	$(MAKE) bench-profile ARGS="-f native -o encode -d dynamic"

profile-json:
	$(MAKE) bench-profile ARGS="-f native -o encode -d json"

publish:
	npm publish --access=public

# Update ClickHouse settings types from official source
# Re-run periodically to pick up new ClickHouse releases
update-settings:
	node --experimental-strip-types scripts/generate-settings-types.ts
