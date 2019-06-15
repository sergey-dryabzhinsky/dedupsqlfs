# -*- coding: utf8 -*-

"""
Special action to verify all data hashes
"""

__author__ = 'sergey'

import sys


def do_verify(options, _fuse):
    """
    @param options: Commandline options
    @type  options: object

    @param _fuse: FUSE wrapper
    @type  _fuse: dedupsqlfs.fuse.dedupfs.DedupFS
    """

    tableOption = _fuse.operations.getTable("option")

    curHashFunc = tableOption.get("hash_function")

    tableHash = _fuse.operations.getTable("hash")
    tableHashCT = _fuse.operations.getTable("hash_compression_type")
    tableBlock = _fuse.operations.getTable("block")

    _fuse.operations.hash_function = curHashFunc

    hashCount = tableHash.get_count()
    if _fuse.getOption("verbosity") > 0:
        print("Ready to verify %s blocks." % hashCount)

    cur = tableHash.getCursor(True)
    cur.execute("SELECT `id` FROM `%s`" % tableHash.getName())

    cnt = equal = 0
    lastPrc = ""

    for hashItem in iter(cur.fetchone, None):

        cnt += 1

        blockItem = tableBlock.get(hashItem["id"])
        hashCT = tableHashCT.get(hashItem["id"])
        blockData = _fuse.decompressData(_fuse.operations.getCompressionTypeName(hashCT["type_id"]), blockItem["data"])

        newHash = _fuse.operations.do_hash(blockData)

        if newHash == hashItem["hash"]:
            equal += 1

        prc = "%6.2f%%" % (cnt*100.0/hashCount)
        if prc != lastPrc:
            lastPrc = prc
            if _fuse.getOption("verbosity") > 0:
                sys.stdout.write("\r%s " % prc)
                sys.stdout.flush()

    if _fuse.getOption("verbosity") > 0:
        sys.stdout.write("\n")
        sys.stdout.flush()

    if _fuse.getOption("verbosity") > 0:
        print("Processed %s hashes, equal %s blocks." % (cnt, equal,))

    if hashCount != cnt:
        print("Something went wrong?")
        return 1

    if cnt != equal:
        print("Data corrupted?! %s block hashes not equals!" % (cnt - equal))
        return 1

    return 0
