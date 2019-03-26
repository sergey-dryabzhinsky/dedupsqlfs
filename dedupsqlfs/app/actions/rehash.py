# -*- coding: utf8 -*-

"""
Special action to rehash all data
"""

__author__ = 'sergey'

import sys


def do_rehash(options, _fuse):
    """
    @param options: Commandline options
    @type  options: object

    @param _fuse: FUSE wrapper
    @type  _fuse: dedupsqlfs.fuse.dedupfs.DedupFS
    """

    tableOption = _fuse.operations.getTable("option")

    curHashFunc = tableOption.get("hash_function")
    if curHashFunc == options.rehash_function:
        if _fuse.getOption("verbosity") > 0:
            print("Already using %s hash function for filesystem! Do not rehashing." % curHashFunc)
        return True

    tableHash = _fuse.operations.getTable("hash")
    tableHashCT = _fuse.operations.getTable("hash_compression_type")
    tableBlock = _fuse.operations.getTable("block")

    _fuse.operations.hash_function = options.rehash_function

    hashCount = tableHash.get_count()
    if _fuse.getOption("verbosity") > 0:
        print("Ready to rehash %s blocks." % hashCount)

    cur = tableHash.getCursor(True)
    cur.execute("SELECT `id` FROM `%s`" % tableHash.getName())

    cnt = upd = 0
    lastPrc = ""

    _fuse.operations.getManager().setAutocommit(False)
    tableHash.begin()
    _fuse.operations.getManager().setAutocommit(True)

    for hashItem in iter(cur.fetchone, None):

        cnt += 1

        blockItem = tableBlock.get(hashItem["id"])
        hashCT = tableHashCT.get(hashItem["id"])
        blockData = _fuse.decompressData(_fuse.operations.getCompressionTypeName(hashCT["type_id"]), blockItem["data"])
        newHash = _fuse.operations.do_hash(blockData)
        res = tableHash.update(hashItem["id"], newHash)

        if res:
            upd += 1
        prc = "%0.2f%%" % (cnt*100.0/hashCount)
        if prc != lastPrc:
            lastPrc = prc
            if _fuse.getOption("verbosity") > 0:
                sys.stdout.write("\r%s   " % prc)
                sys.stdout.flush()

    if _fuse.getOption("verbosity") > 0:
        sys.stdout.write("\n")
        sys.stdout.flush()

    if _fuse.getOption("verbosity") > 0:
        print("Processed %s hashes, rehashed %s blocks." % (cnt, upd,))

    if hashCount == cnt:
        _fuse.operations.getManager().setAutocommit(False)
        tableHash.commit()
        _fuse.operations.getManager().setAutocommit(True)

        tableOption.update("hash_function", options.rehash_function)

        tableHash.vacuum()
    else:
        _fuse.operations.getManager().setAutocommit(False)
        tableHash.rollback()
        _fuse.operations.getManager().setAutocommit(True)
        print("Something went wrong? Changes are rolled back!")
        return 1

    return 0
