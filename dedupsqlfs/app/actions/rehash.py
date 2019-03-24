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

    cur = tableHash.getCursor()
    cur.execute("SELECT `id` FROM `%s`" % tableHash.getName())

    newHashTypeId = _fuse.operations.getCompressionTypeId(options.rehash_function)

    cnt = 0
    prc = lastPrc = ""
    for hashItem in iter(cur.fetchone, None):

        blockItem = tableBlock.get(hashItem["id"])
        hashCT = tableHashCT.get(hashItem["id"])
        blockData = _fuse.decompressData(_fuse.operations.getCompressionTypeName(hashCT["type_id"]), blockItem["data"])
        newHash = _fuse.operations.do_hash(blockData)
        tableHash.update(hashItem["id"], newHash)
        tableHashCT.update(hashItem["id"], newHashTypeId)

        cnt += 1
        prc = "%0.2f%%" % (cnt*100.0/hashCount)
        if prc != lastPrc:
            lastPrc = prc
            if _fuse.getOption("verbosity") > 0:
                sys.stdout.write("\r%s   " % prc)
                sys.stdout.flush()

    if _fuse.getOption("verbosity") > 0:
        sys.stdout.write("\n")
        sys.stdout.flush()

    tableHash.commit()
    tableHashCT.commit()

    tableOption.update("hash_function", options.rehash_function)
    tableOption.commit()

    return
