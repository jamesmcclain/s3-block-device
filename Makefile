CC = gcc
CFLAGS ?= -Wall -O0 -ggdb3


all: bin/s3bd lib/libs3bd_local.so

src/%.o: src/%.c src/%.h
	$(CC) $(CFLAGS) $< `pkg-config fuse --cflags` -c -o $@

src/%.o: src/%.c
	$(CC) $(CFLAGS) $< `pkg-config fuse --cflags` -c -o $@

bin/s3bd: src/s3bd.o src/cmdline.o
	$(CC) $(LDFLAGS) $^ -ldl `pkg-config fuse --cflags --libs` -o $@

src/local/%.o: src/local/%.c src/local/%.h
	$(CC) $(CFLAGS) $< -fpic -fPIC `pkg-config fuse --cflags` -c -o $@

lib/libs3bd_local.so: src/local/callbacks.o
	$(CC) $(CFLAGS) $< -shared -o $@

clean:
	rm -f src/*.o src/local/*.o

cleaner: clean
	rm -f bin/s3bd lib/*.so

cleanest: cleaner
