DedupSQLfs
==========

Deduplicating filesystem via FUSE and SQLite written in Python

Based on code written by Peter Odding: http://github.com/xolox/dedupfs/

Rewriten to use Python3 (3.2+), new compression methods, snapshots / subvolumes.

I know about ZFS and Btrfs but them are still complicated to use under linux and has disadvantages
 like need in block device, weak block hash algorithms, very little variants of compression methods.

## Usage

The following shell commands show how to install and use the DedupFS file system on [Ubuntu](http://www.ubuntu.com/)
 (where it was developed):

    $ sudo pip3 install llfuse
    $ git clone https://github.com/sergey-dryabzhinsky/dedupsqlfs.git
    $ mkdir mount_point
    $ ./bin/mount.dedupsqlfs --mountpoint mount_point
    # Now copy some files to mount_point/ and observe that the size of the two
    # databases doesn't grow much when you copy duplicate files again :-)
    # The databases are by default stored in the following locations:
    # ~/data/dedupsqlfs/*.sqlite3 contains the tree, meta and blocks data
    # You can choose another location by --data option.

## Status

Development on DedupFS began as a proof of concept to find out how much disk space the author could free by employing deduplication to store his daily backups. Since then it's become more or less usable as a way to archive old backups, i.e. for secondary storage deduplication. It's not recommended to use the file system for primary storage though, simply because the file system is too slow. I also wouldn't recommend depending on DedupFS just yet, at least until a proper set of automated tests has been written and successfully run to prove the correctness of the code.

The file system initially stored everything in a multiple [SQLite](http://www.sqlite.org/) databases.
 It turned out that in single file database after the database grew beyond 8 GB the write speed would drop
 from 8-12 MB/s to 2-3 MB/s. In multiple files it drops to 6-8 MB/s with other changes applied even after 150 GB.

### What's new

 * Filesystem data stored in multiple SQLite files.
 * Tuned SQLite connections.
 * Delayed writes for blocks (hashing and compression too).
 * Use "stream"-like writes and read of data blocks, don't store complete files in memory.
 * Cached filesystem tree nodes, inodes and data blocks.
 * New compression methods (some ported for python3): lzo (ported), lz4 (ported), lzma, snappy.

### Limitations

In the current implementation a file's content DON'T needs to fit in a [cStringIO](http://docs.python.org/library/stringio.html#module-cStringIO)
 instance, which limits the maximum file size to your free RAM. But sometimes you need to tune caching timeouts to
 drop caches more friquently, on massive reads.

And there is limit of SQLite database size: about 2 TB with default settings of pages_count (2**31) * page_size (1024).

## Dependencies

DedupSQLfs was developed using Python 3.2, though it might also work on newer versions. It definitely doesn't work
 with Python 2. It requires the [Python llFUSE binding](http://www.rath.org/llfuse-docs/example.html) in addition
 to several Python standard libraries like [sqlite3](http://docs.python.org/library/sqlite3.html), [hashlib](http://docs.python.org/library/hashlib.html).

Additional compression modules can be builded with commands:

    $ sudo apt-get install build-essential python3-dev liblzo2-dev libsnappy-dev
    $ cd lzo
    $ python3 setup.py clean -a
    $ python3 setup.py build
    # ... same for lz4, snappy

Additional storage engine via MySQL can be accessed with commands:
    $ suto pip install pymysql

## Contact

If you have questions, bug reports, suggestions, etc. the author can be contacted at <sergey.dryabzhinsky@gmail.com> and
github [issues list](https://github.com/sergey-dryabzhinsky/dedupsqlfs/issues).
The latest version of DedupFS is available at <https://github.com/sergey-dryabzhinsky/dedupsqlfs>.

## License

This software is licensed under the MIT license.

© 2013 Sergey Dryabzhinsky &lt;<sergey.dryabzhinsky@gmail.com>&gt;.

© 2010 Peter Odding &lt;<peter@peterodding.com>&gt;.
