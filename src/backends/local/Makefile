CFLAGS ?= -Wall -Werror -Og -ggdb3

all: libs3bd_local.so

%.o: %.c ../backend.h
	$(CC) $(CFLAGS) $< -fPIC `pkg-config fuse --cflags` -c -o $@

libs3bd_local.so: callbacks.o
	$(CC) $(CFLAGS) $< -shared -o $@

clean:
	rm -f *.o

cleaner: clean
	rm -f *.so

cleanest: cleaner
