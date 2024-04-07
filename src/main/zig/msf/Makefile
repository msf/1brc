all: test build

test:
	zig build test

build:
	zig build --release=fast
	cp -f zig-out/bin/msf msf1brc
