CFLAGS ?= -Wall -O0 -ggdb3


all: libs3bd_gdal.so

%.o: %.c ../backend.h
	$(CC) $(CFLAGS) $< -fPIC `pkg-config gdal --cflags` `pkg-config fuse --cflags` -c -o $@

libs3bd_gdal.so: callbacks.o
	$(CC) $(CFLAGS) $< `pkg-config gdal --libs` -shared -o $@

clean:
	rm -f *.o

cleaner: clean
	rm -f *.so

cleanest: cleaner