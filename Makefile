.PHONY: build build-full build-lz4 test fuzz bench publish

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

publish:
	npm publish --access=public
