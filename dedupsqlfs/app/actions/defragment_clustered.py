# -*- coding: utf8 -*-

"""
Special action to collect clustered garbage and remove
"""

__author__ = 'sergey'

from time import time
from math import floor

from dedupsqlfs.my_formats import format_timespan
from dedupsqlfs.lib import constants
from dedupsqlfs.fuse.subvolume import Subvolume


def __collect_garbage(app):
    """
    @param app:
    @type app: dedupsqlfs.fuse.dedupfs.DedupFS
    @return: None
    """
    if app.isReadonly():
        return

    start_time = time()
    app.getLogger().info("Performing garbage collection (this might take a while) ..")
    clean_stats = False
    gc_funcs = [
        __collect_blocks
    ]

    cnt_sum = 0
    for method in gc_funcs:
        sub_start_time = time()
        cnt, msg = method(app)
        if cnt:
            clean_stats = True
            elapsed_time = time() - sub_start_time
            if not app.getOption("parsable"):
                app.getLogger().info(msg, format_timespan(elapsed_time))
            cnt_sum += cnt

    if clean_stats:
        subv = Subvolume(app.operations)
        subv.clean_stats(app.operations.mounted_subvolume_name)

        if app.operations.mounted_subvolume_name == constants.ROOT_SUBVOLUME_NAME:
            subv.clean_non_root_subvol_diff_stats()

    elapsed_time = time() - start_time
    if app.getOption("parsable"):
        app.getLogger().info("Count: %s", cnt_sum)
        app.getLogger().info("Time: %s", format_timespan(elapsed_time))
    else:
        app.getLogger().info("Finished garbage collection in %s.", format_timespan(elapsed_time))
    return


def __collect_blocks(app):
    """
    Collect all hashes not linked to inode-blocks
    Across all subvolumes
    And remove them

    @param app:
    @type app: dedupsqlfs.fuse.dedupfs.DedupFS
    @return: string
    """

    tableHash = app.operations.getTable("hash")
    tableBlock = app.operations.getTable("block")
    tableHCT = app.operations.getTable("hash_compression_type")
    tableHSZ = app.operations.getTable("hash_sizes")
    tableHCnt = app.operations.getTable("hash_count")
    tableHOwn = app.operations.getTable("hash_owner")

    if not tableHash.getClustered():
        app.getLogger().warning("Hashes and blocks are not clustered! Skip")
        return 0, ""

    count2 = tableHCnt.count_unused_hashes()
    app.getLogger().info("Found %d unused data blocks and hashes by index" % count2)
    if count2:
        app.getLogger().info("Clean unused data blocks and hashes by index: %d" % count2)
        hashes = tableHCnt.get_unused_hashes()
        id_str =",".join(hashes)

        cnt=tableHash.remove_by_ids(id_str)
        app.getLogger().info("Clean %d unused hashes" % cnt)

        cnt=tableBlock.remove_by_ids(id_str)
        app.getLogger().info("Clean %d unused blocks" % cnt)

        cnt=tableHCT.remove_by_ids(id_str)
        app.getLogger().info("Clean %d unused compression types" % cnt)

        cnt=tableHOwn.remove_by_ids(id_str)
        app.getLogger().info("Clean %d unused owners" % cnt)

        cnt=tableHSZ.remove_by_ids(id_str)
        app.getLogger().info("Clean %d unused sizes" % cnt)


    msg = ""
    if count2 > 0:
        tableHash.commit()
        tableBlock.commit()
        tableHCT.commit()
        tableHOwn.commit()
        tableHSZ.commit()
        msg = "Cleaned up %i unused data block%s and hashes in %%s." % (
            count2, count2 != 1 and 's' or '',
        )
    return count2, msg


def do_defragment_clustered(options, _fuse):
    """
    Defragment only selected Subvolume

    @param options: Commandline options
    @type  options: object

    @param _fuse: FUSE wrapper
    @type  _fuse: dedupsqlfs.fuse.dedupfs.DedupFS
    """
    __collect_garbage(_fuse)
    __collect_blocks(_fuse)
    return 0
