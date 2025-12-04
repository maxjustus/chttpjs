.PHONY: build build-full build-lz4 test publish

build: build-full build-lz4

build-full:
	npm run build:full

build-lz4:
	npm run build:lz4

test:
	npm test

publish:
	npm publish --access=public
