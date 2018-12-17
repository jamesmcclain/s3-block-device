CC = gcc
CFLAGS ?= -Wall -O0 -ggdb3


all: bin/s3bd lib/libs3bd_local.so

src/%.o: src/%.c src/%.h
	$(CC) $(CFLAGS) $< `pkg-config fuse --cflags` -c -o $@

src/%.o: src/%.c
	$(CC) $(CFLAGS) $< `pkg-config fuse --cflags` -c -o $@

bin/s3bd: src/s3bd.o src/cmdline.o
	$(CC) $(LDFLAGS) $^ -ldl `pkg-config fuse --cflags --libs` -o $@

lib/libs3bd_%.so: src/backends/%
	CC=$(CC) CFLAGS="$(CFLAGS)" make -C src/backends/$*
	cp -f src/backends/$*/libs3bd_$*.so $@

clean:
	rm -f src/*.o
	make -C src/backends/local clean
	make -C src/backends/gdal clean

cleaner: clean
	rm -f bin/s3bd lib/*.so
	make -C src/backends/local cleaner
	make -C src/backends/gdal cleaner

cleanest: cleaner
	make -C src/backends/local cleanest
	make -C src/backends/gdal cleanest
