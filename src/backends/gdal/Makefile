CFLAGS ?= -Wall -O0 -ggdb3
BOOST_ROOT ?= /usr/include

all: libs3bd_gdal.so unit_tests
	unit_tests

%.o: %.cpp
	$(CXX) $(CFLAGS) $(CXXFLAGS) -I$(BOOST_ROOT) $< -fPIC -c -o $@

%.o: %.c ../backend.h
	$(CC) $(CFLAGS) $< -fPIC `pkg-config gdal --cflags` `pkg-config fuse --cflags` -c -o $@

libs3bd_gdal.so: callbacks.o rtree.o
	$(CC) $(CFLAGS) $^ `pkg-config gdal --libs` -lstdc++ -shared -o $@

unit_tests: unit_tests.o rtree.o
	$(CC) $(CFLAGS) $^ -lm -lstdc++ -o $@

clean:
	rm -f *.o

cleaner: clean
	rm -f *.so

cleanest: cleaner
