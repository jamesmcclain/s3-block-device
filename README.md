# s3-block-device #

This project uses [FUSE](https://en.wikipedia.org/wiki/Filesystem_in_Userspace) to expose a block "device" (actually just a file), possibly backed by remote resources, on which a filesystem with full semantics can be created.
That allows one to create a normal XFS or ext4 filesystem (as well as many others) on [S3](https://en.wikipedia.org/wiki/Amazon_S3) that is locally mountable and behaves normally in all respects (except for speed).

## To Build ##

Almost any versions of [GCC](https://gcc.gnu.org/) of [GNU Make](https://www.gnu.org/software/make/) should be sufficient.

### Dependencies ###

A version of [`libfuse`](https://github.com/libfuse/libfuse) that supports the 2.6 API is required to build the basic `libs3bd_local` backend.

Additional dependencies are required for additional backends.
A recent version of [GDAL](https://gdal.org/) is required to build the `libs3bd_gdal` backend that provides support for S3, Azure, and Google Cloud Storage.
Version 2.4.0 of GDAL has been tested.

### Compiling ###

To build the executable and the local backend, type the following.
```bash
make
```
That will produce an executable `bin/s3bd` and shared library called `lib/libs3bd_local.so` containing the local backend.

To build the GDAL backend, type the following.
```bash
make lib/libs3bd_gdal.so
```
That will produce a shared library called `lib/libs3bd_gdal.so` containing the GDAL backend.
That backend, in turn, uses various backends provided by GDAL's VSI mechanism to provide access to [a number of cloud providers and arrangements of local and remote files](https://www.gdal.org/gdal_virtual_file_systems.html).
By "arrangements", I mean that is is possible to chain VSI backends together so that one can store a file system in a (read-only) tarball on S3 rather than as a loose collection of block files in an S3 "directory".

## To Use ##

To test the local backend (backed by local files), type something like the following.
```bash
mkdir -p /tmp/blockdir /tmp/mnt /tmp/mnt2
bin/s3bd lib/libs3bd_local.so /tmp/blockdir /tmp/mnt
mkfs.ext2 -b 4096 /tmp/mnt/blocks
sudo mount -o loop /tmp/mnt/blocks /tmp/mnt2
```

The first command create three directories: a directory to contain block files, a directory on-which to mount our FUSE file system, and a directory on-which to mount our ext2 filesystem.

The second command mounts our FUSE file system at `/tmp/mnt`.
There is a file under that directory called `/tmp/mnt/blocks`, and those blocks are backed by the local directory `/tmp/blockdir`.
The file has a fixed size, but the contents of the `blockdir` are created on an as-needed basis.

The third command formats the exposed block device.

The fourth command mounts the exposed block device at `/tmp/mnt2`.

```
% df -hT | grep mnt
/tmp/blockdir  fuse      1.0G  1.0G     0 100% /tmp/mnt
/dev/loop0     ext2     1008M  217M  741M  23% /tmp/mnt2
% ls /tmp/blockdir | head -10
0x000000000000
0x000000000001
0x000000000002
0x000000000003
0x000000000004
0x000000000005
0x000000000006
0x000000000007
0x000000000008
0x000000000009
%
```

### Other Examples ###

#### Read-Only Tarball ####

With the `/tmp/blockdir` directory created above still present, type the following in a differnet terminal.
```bash
cd /tmp
tar cvf blockdir.tar blockdir/
exit
```
That will create a file called `/tmp/blockdir.tar` that contains the contents of the `/tmp/blockdir` directory.

That tarball can be mounted as a local block device (which is in-turn mountable) by typing the following.
```bash
bin/s3bd lib/libs3bd_gdal.so /vsitar/tmp/blockdir.tar/blockdir /tmp/mnt
```

#### S3 ####

```bash
bin/s3bd lib/libs3bd_gdal.so /vsis3/my-bucket/blockdir /tmp/mnt
```

## Future Work ##

Performance improvements.

## Copying ##

See [here](LICENSE).
