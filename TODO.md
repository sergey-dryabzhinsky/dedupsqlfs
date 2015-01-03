TODO list
==========

Here are some things on to-do list, in no particular order:

 * Implement thin snapshots - that stores only inode and data changes.
   Current snapshots copying all metadata information.
   This need COW.

 * Try to store blocks in several database files or tables. Something
   like partitioning. Hide it in Block storage class.

 * Implement functions for work with blocks, hashes, subvolumes as separate
   commands (options) in do.dedupsqlfs. This should reduce code size of
   FUSE operations file.

 * Implement creation of subvolumes, snapshots on-the-fly via commands through
   socket interface. And other commands too.

 * Multiprocess block hashing and compression.
   Not always needed. On writing many small files SQLite database writes
   is slow more then data compression or hashing process.

 * Automatically switch to a larger block size to reduce the overhead for files
   that rarely change after being created (like >= 100MB video files :-)
   Or use defragmentation tool for that.
   Block size must be stored for every file if it differs from default.

 * Implement adaptive compression for different file types:
   - no compression for audio/video/images/archives - *.mp3/*.mp4/*.jpg/*.zip
   - fast compression for some types - *.wav/*.pdf
   - best for texts and some other types - *.txt/*.html/*.bmp
   There must be variants for pairs - types:compression. Even custom compression
   methods: "*.wav,*.pdf:custom:zlib=fast,snappy".
   Compression for file types must be redefinable.
   Compression can be overrided for every file.
   Need recompression tool. And fsck support for old-new compression options.

 * Use configuration file with defined default compression, hash algo, and other options.

 * Implement rename() independently of link()/unlink() to improve performance?
   Need benchmarks.

 * Implement `--verify-reads` option that recalculates hashes when reading to
   check for data block corruption?

 * Tag databases with a version number and implement automatic upgrades because
   I've grown tired of upgrading my database by hand :-)

 * Support directory hard links without upsetting FUSE and add a command-line
   option that search for identical subdirectories and replace them with
   directory hard links.
