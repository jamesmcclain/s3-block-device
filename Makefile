CC = gcc
CFLAGS ?= -O0 -ggdb3


%.o: %.c
	$(CC) $(CFLAGS) $< -c -o $@

s3bd: s3bd.o
	$(CC) $(LDFLAGS) $^ -o $@

clean:
	rm -f *.o

cleaner: clean
	rm -f s3bd

cleanest: cleaner
