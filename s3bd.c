#define FUSE_USE_VERSION (26)

#include <stdio.h>
#include <fuse.h>


static struct fuse_operations ops = {
};

int main(int argc, char ** argv) {
  return fuse_main(argc, argv, &ops, NULL);
}
