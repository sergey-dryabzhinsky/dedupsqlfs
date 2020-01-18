DedupSQLfs
==========

Deduplicating filesystem via FUSE and SQLite written in Python

Based on code written by Peter Odding: http://github.com/xolox/dedupfs/

Rewriten to use Python3 (3.2+), new compression methods, snapshots / subvolumes.

I know about ZFS and Btrfs. But they are still complicated to use under linux and has disadvantages
 like need in block device, weak block hash algorithms, very little variants of compression methods.

## Usage

The following shell commands show how to install and use the DedupFS file system on [Ubuntu](http://www.ubuntu.com/)
 (where it was developed):

    $ sudo apt-get install python3-pip libfuse-dev
    #
    $ sudo pip3 install llfuse
    #
    # llfuse must be version 1.3.6
    #
    $ git clone https://github.com/sergey-dryabzhinsky/dedupsqlfs.git
    #
    $ mkdir mount_point
    $ ./bin/mount.dedupsqlfs --mountpoint mount_point
    # Now copy some files to mount_point/ and observe that the size of the two
    # databases doesn't grow much when you copy duplicate files again :-)
    # The databases are by default stored in the following locations:
    # ~/data/dedupsqlfs/*.sqlite3 contains the tree, meta and blocks data
    # You can choose another location by --data option.
    #
    # As of 1.2.919 version cache_flusher helper starts
    # and touches hidden file in mount_point directory.
    # So umount may fail in that time. Call it repeated with some lag:
    $ umount mount_point || sleep 0.5 && umount mount_point

## Status

Development on DedupSqlFS began as a proof of concept to find out how much disk space the author could free
 by employing deduplication to store his daily backups. Since then it's become more or less usable as a way
 to archive old backups, i.e. for secondary storage deduplication. It's not recommended to use the file system
 for primary storage though, simply because the file system is too slow.
 I also wouldn't recommend depending on DedupFS just yet, at least until a proper set of automated tests
 has been written and successfully run to prove the correctness of the code.

The file system initially stored everything in a multiple [SQLite](http://www.sqlite.org/) databases.
 It turned out that in single file database after the database grew beyond 8 GB the write speed would drop
 from 8-12 MB/s to 2-3 MB/s. In multiple files it drops to 6-8 MB/s with other changes applied even after 150 GB.

### What's new

 * Filesystem data stored in multiple SQLite files.
 * Tuned SQLite connections.
 * Delayed writes for blocks (hashing and compression too).
 * Use "stream"-like writes and read of data blocks, don't store complete files in memory.
 * Cached filesystem tree nodes, inodes and data blocks.
 * Many compression methods: zlib, bzip2, lzma, lzo, lz4, quicklz, zstd, snappy.
 * Support for data storage in localy started MySQL server.

### Limitations

In the current implementation a file's content DON'T needs to fit in a [cStringIO](http://docs.python.org/library/stringio.html#module-cStringIO)
 instance, which limits the maximum file size to your free RAM. But sometimes you need to tune caching timeouts to
 drop caches more friquently, on massive reads.

And there is limit of SQLite database size: about 2 TB with default settings of pages_count (2**31) * page_size (1024).

Note: dynamic subvolume and snapshot creation available only with MySQL storage enagine.
 SQLite is keeping database locked.
 Though dynamic working subvolume switching not available.
 For now MySQL table engine is MyISAM by default - it's fast and not bloated.
 InnoDB working strange:
 - I get about twice sized database: 2.8Gb data + ~1.6Gb something with indexes,
 - while MyISAM working predictable: 2.8Gb data + ~100Mb indexes.

 MariaDB's Aria working slowly than MyISAM - doing too much logging...

 MariaDB's TokuDB looks interesting and promising. Compression over data and indexes.

#### Engines Tests:

Real data:

 - 50.40 Gb data of web-developers virtual machine
 - 1 snapshot after sync

FS after backup:

 * Sqlite engine:
    * Databases: 25.23 Gb
        * Indexes and metadata: 1.24 Gb
    * Sync time: ~ 10h:12m 
 * MyISAM:
    * Databases: 25.65 Gb
        * Indexes and metadata: 1.66 Gb
    * Sync time: ~ 16h:40m 
 * InnoDB:
    * Databases: 27.66 Gb
        * Indexes and metadata: 3.68 Gb
    * Sync time: ~ 16h:00m 
 * TokuDB:
    * Databases: 25.37 Gb
        * Indexes and metadata: 1.38 Gb
    * Sync time: ~ 16h:30m

And Sqlite wins!


## Dependencies

DedupSQLfs was developed using Python 3.2, though it also work with newer versions. 
 It definitely doesn't work with Python 2.
 It requires the [Python llFUSE binding](http://www.rath.org/llfuse-docs/example.html) in addition
 to several Python standard libraries like [sqlite3](http://docs.python.org/library/sqlite3.html), [hashlib](http://docs.python.org/library/hashlib.html).

Additional compression modules can be builded with commands:

    $ sudo apt-get install build-essential python3-dev liblzo2-dev libsnappy-dev liblz4-dev liblzma-dev
    $ cd lib-dynload/lzo
    $ python3 setup.py clean -a
    $ python3 setup.py build_ext clean
    ## ... same for lz4, snappy,..
    # If you need extra optimization - tune for your CPU for example - then call
    $ python3 setup.py clean -a
    $ python3 setup.py build_ext --extra-optimization clean

Additional storage engine via MySQL can be accessed with commands:

    $ sudo pip3 install pymysql

Additional performance gain about 1-5% via Cython:

    ## Setup tools If not installed
    $ sudo pip3 install setuptools
    $ sudo pip3 install cython
    $ python3 setup.py build_ext --cython-build
    $ python3 setup.py stripall
    ## Warning! This deletes all .py files
    $ python3 setup.py cleanpy

### Notes about Cython

1. Profiling via cProfile not working for compiled code.
2. Always keep copy of dedupsqlfs directory if you will run `cleanpy` command.

## Contact

If you have questions, bug reports, suggestions, etc. the author can be contacted at <sergey.dryabzhinsky@gmail.com> and
github [issues list](https://github.com/sergey-dryabzhinsky/dedupsqlfs/issues).
The latest version of DedupSqlFS is available at <https://github.com/sergey-dryabzhinsky/dedupsqlfs>.

## License

This software is licensed under the MIT license.

© 2013-2020 Sergey Dryabzhinsky &lt;sergey.dryabzhinsky@gmail.com&gt;.

© 2010 Peter Odding &lt;peter@peterodding.com&gt;.
