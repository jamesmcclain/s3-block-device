CC = gcc
CFLAGS ?= -O0 -ggdb3


%.o: %.c
	$(CC) $(CFLAGS) $< `pkg-config fuse --cflags` -c -o $@

s3bd: s3bd.o
	$(CC) $(LDFLAGS) $^ `pkg-config fuse --cflags --libs` -o $@ 

clean:
	rm -f *.o

cleaner: clean
	rm -f s3bd

cleanest: cleaner
