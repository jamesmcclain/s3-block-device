CC = gcc
CFLAGS ?= -Wall -O0 -ggdb3


src/%.o: src/%.c src/%.h
	$(CC) $(CFLAGS) $< `pkg-config fuse --cflags` -c -o $@

src/%.o: src/%.c
	$(CC) $(CFLAGS) $< `pkg-config fuse --cflags` -c -o $@

bin/s3bd: src/s3bd.o src/cmdline.o src/callbacks.o
	$(CC) $(LDFLAGS) $^ `pkg-config fuse --cflags --libs` -o $@ 

clean:
	rm -f src/*.o

cleaner: clean
	rm -f bin/s3bd

cleanest: cleaner
