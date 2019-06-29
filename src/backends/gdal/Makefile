CFLAGS ?= -Wall -O0 -ggdb3
BOOST_ROOT ?= /usr/include


all: libs3bd_gdal.so unit_tests
	unit_tests

%.o: %.c %.h
	$(CC) $(CFLAGS) $< -fPIC -c -o $@

%.o: %.cpp
	$(CXX) $(CFLAGS) $(CXXFLAGS) -I$(BOOST_ROOT) $< -fPIC `pkg-config gdal --cflags` `pkg-config fuse --cflags` -c -o $@

callbacks.o: callbacks.c ../backend.h
	$(CC) $(CFLAGS) -D_FILE_OFFSET_BITS=64 $< -fPIC -c -o $@

libs3bd_gdal.so: callbacks.o fullio.o storage.o
	$(CC) $(CFLAGS) $^ `pkg-config gdal --libs` -lpthread -lstdc++ -shared -o $@

unit_tests: unit_tests.o fullio.o storage.o
	$(CC) $(CFLAGS) $^ -lm `pkg-config gdal --libs` -lpthread -lstdc++ -o $@

clean:
	rm -f *.o

cleaner: clean
	rm -f *.so

cleanest: cleaner
	rm -f unit_tests
