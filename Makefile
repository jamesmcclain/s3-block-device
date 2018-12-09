CC = gcc
CFLAGS ?= -O0 -ggdb3


src/%.o: src/%.c
	$(CC) $(CFLAGS) $< `pkg-config fuse --cflags` -c -o $@

bin/s3bd: src/s3bd.o
	$(CC) $(LDFLAGS) $^ `pkg-config fuse --cflags --libs` -o $@ 

clean:
	rm -f src/*.o

cleaner: clean
	rm -f bin/s3bd

cleanest: cleaner
