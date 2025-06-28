# -*- coding: utf8 -*-

"""
Special action to collect all garbage and remove
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
        __collect_strings,
        __collect_inodes_all,
        __collect_xattrs,
        __collect_links,
        __collect_indexes,
        __collect_owner,
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


def __collect_strings(app):
    """
    Collect all file names and check fs tree
    And cleanup removed

    @param app:
    @type app: dedupsqlfs.fuse.dedupfs.DedupFS
    @return: str
    """

    tableName = app.operations.getTable("name")

    if tableName.getClustered():
        app.getLogger().debug("Table with path segments is clustered! Skip, @todo")
        return 0, ""

    subv = Subvolume(app.operations)
    treeNameIds = subv.prepareTreeNameIds()

    app.getLogger().debug("Clean unused path segments...")

    countNames = tableName.get_count()
    app.getLogger().debug(" path segments: %d", countNames)

    count = 0
    current = 0
    proc = ""

    maxCnt = 10000
    curBlock = 0

    while True:

        if current == countNames:
            break

        nameIds = tableName.get_name_ids(curBlock, curBlock + maxCnt)

        current += len(nameIds)

        curBlock += maxCnt
        if not nameIds:
            continue

        # SET magick
        to_delete = nameIds - treeNameIds

        id_str = ",".join((str(_id) for _id in to_delete))
        count += tableName.remove_by_ids(id_str)

        p = "%6.2f%%" % (100.0 * current / countNames)
        if p != proc:
            proc = p
            app.getLogger().debug("%s (count=%d)", proc, count)

    msg = ""
    if count > 0:
        tableName.commit()
        msg = "Cleaned up %i unused path segment%s in %%s." % (count, count != 1 and 's' or '')
    return count, msg


def __collect_inodes_all(app):
    """
    Collect all inodes missing in fs tree
    And remove them

    @param app:
    @type app: dedupsqlfs.fuse.dedupfs.DedupFS
    @return: string
    """

    tableInode = app.operations.getTable("inode")
    tableTree = app.operations.getTable("tree")

    app.getLogger().debug("Clean unused inodes (all)...")

    countInodes = tableInode.get_count()
    app.getLogger().debug(" inodes: %d", countInodes)

    count = 0
    current = 0
    proc = ""

    curBlock = 0
    maxCnt = 10000

    while True:

        if current == countInodes:
            break

        inodeIds = tableInode.get_inode_ids(curBlock, curBlock + maxCnt)
        current += len(inodeIds)

        curBlock += maxCnt
        if not len(inodeIds):
            continue

        treeInodeIds = tableTree.get_inodes_by_inodes(inodeIds)

        # SET magick
        to_delete = inodeIds - treeInodeIds

        count += tableInode.remove_by_ids(to_delete)

        p = "%6.2f%%" % (100.0 * current / countInodes)
        if p != proc:
            proc = p
            app.getLogger().debug("%s (count=%d)", proc, count)

    msg = ""
    if count > 0:
        tableInode.commit()
        msg = "Cleaned up %i unused inode%s in %%s." % (count, count != 1 and 's' or '')
    return count, msg


def __collect_xattrs(app):
    """
    Collect all xattrs not linked to inodes
    And remove them

    @param app:
    @type app: dedupsqlfs.fuse.dedupfs.DedupFS
    @return: string
    """

    tableXattr = app.operations.getTable("xattr")
    tableInode = app.operations.getTable("inode")

    app.getLogger().debug("Clean unused xattrs...")

    countXattrs = tableXattr.get_count()
    app.getLogger().debug(" xattrs: %d", countXattrs)

    count = 0
    current = 0
    proc = ""

    curBlock = 0
    maxCnt = 10000

    while True:

        if current == countXattrs:
            break

        inodeIds = tableXattr.get_inode_ids(curBlock, curBlock + maxCnt)
        current += len(inodeIds)

        curBlock += maxCnt
        if not inodeIds:
            continue

        xattrInodeIds = tableInode.get_inodes_by_inodes(inodeIds)

        # SET magick
        to_delete = inodeIds - xattrInodeIds

        count += tableXattr.remove_by_ids(to_delete)

        p = "%6.2f%%" % (100.0 * current / countXattrs)
        if p != proc:
            proc = p
            app.getLogger().debug("%s (count=%d)", proc, count)

    msg = ""
    if count > 0:
        tableXattr.commit()
        msg = "Cleaned up %i unused xattr%s in %%s." % (count, count != 1 and 's' or '')
    return count, msg


def __collect_links(app):
    """
    Collect all links not linked to inodes
    And remove them

    @param app:
    @type app: dedupsqlfs.fuse.dedupfs.DedupFS
    @return: string
    """

    tableLink = app.operations.getTable("link")
    tableInode = app.operations.getTable("inode")

    app.getLogger().debug("Clean unused links...")

    countLinks = tableLink.get_count()
    app.getLogger().debug(" links: %d", countLinks)

    count = 0
    current = 0
    proc = ""

    curBlock = 0
    maxCnt = 10000

    while True:

        if current == countLinks:
            break

        inodeIds = tableLink.get_inode_ids(curBlock, curBlock + maxCnt)

        current += len(inodeIds)

        curBlock += maxCnt
        if not inodeIds:
            continue

        linkInodeIds = tableInode.get_inodes_by_inodes(inodeIds)

        # SET magick
        to_delete = inodeIds - linkInodeIds

        count += tableLink.remove_by_ids(to_delete)

        p = "%6.2f%%" % (100.0 * current / countLinks)
        if p != proc:
            proc = p
            app.getLogger().debug("%s (count=%d)", proc, count)

    msg = ""
    if count > 0:
        tableLink.commit()
        msg = "Cleaned up %i unused link%s in %%s." % (count, count != 1 and 's' or '')
    return count, msg


def __collect_indexes(app):
    """
    Collect all inode-blocks not linked to inodes
    And remove them

    @param app:
    @type app: dedupsqlfs.fuse.dedupfs.DedupFS
    @return: string
    """

    tableIndex = app.operations.getTable("inode_hash_block")
    tableInode = app.operations.getTable("inode")
    tableHashCount = app.operations.getTable("hash_count")

    app.getLogger().debug("Clean unused block indexes...")

    countInodes = tableIndex.get_count_uniq_inodes()
    app.getLogger().debug(" block inodes: %d", countInodes)

    count = 0
    countTrunc = 0
    current = 0
    proc = ""

    curBlock = 0
    maxCnt = 10000

    while True:

        if current == countInodes:
            break

        inodeIds = tableIndex.get_inode_ids(curBlock, curBlock + maxCnt)

        current += len(inodeIds)

        curBlock += maxCnt
        if not len(inodeIds):
            continue

        indexInodeIds = tableInode.get_inodes_by_inodes(inodeIds)

        # SET magick
        to_delete = inodeIds - indexInodeIds
        to_trunc = inodeIds - to_delete

        hashes = tableIndex.get_hashid_by_inodes(to_delete)
        for hash_id in hashes:
            tableHashCount.dec(hash_id)

        count += tableIndex.remove_by_inodes(to_delete)

        # Slow?
        inodeSizes = tableInode.get_sizes_by_id(to_trunc)
        for inode_id in to_trunc:
            size = inodeSizes.get(inode_id, -1)
            if size < 0:
                continue

            inblock_offset = size % app.operations.block_size
            max_block_number = int(floor(1.0 * (size - inblock_offset) / app.operations.block_size))

            trunced = tableIndex.delete_by_inode_number_more(inode_id, max_block_number)
            countTrunc += len(trunced)

        p = "%6.2f%%" % (100.0 * current / countInodes)
        if p != proc:
            proc = p
            app.getLogger().debug("%s (count=%d, trunced=%d)", proc, count, countTrunc)

    count += countTrunc

    msg = ""
    if count > 0:
        tableIndex.commit()
        msg = "Cleaned up %i unused index entr%s in %%s." % (count, count != 1 and 'ies' or 'y')
    return count, msg


def __collect_owner(app):
    """
    Collect all ihases not linked to inodes
    And remove link to them

    @param app:
    @type app: dedupsqlfs.fuse.dedupfs.DedupFS
    @return: string
    """

    tableHCnt = app.operations.getTable("hash_count")
    tableHOwn = app.operations.getTable("hash_owner")

    app.getLogger().debug("Get unused hashes...")

    if tableHash.getClustered():
        app.getLogger().warning("Hashes and blocks are clustered! Skip, @todo")
        return 0, ""

    hashes = tableHCnt.get_unused_hashes()
    id_str =",".join(hashes)

    app.getLogger().debug("Cleaup uuids table...")
    count += tableHOwn.remove_by_ids(id_str)

    msg = ""
    if count > 0:
        tableHOwn.commit()
        msg = "Cleaned up %i unused uuid entr%s in %%s." % (count, count != 1 and 'ies' or 'y')
    return count, msg


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

    if tableHash.getClustered():
        app.getLogger().warning("Hashes and blocks are clustered! Skip, @todo")
        return 0, ""

    subv = Subvolume(app.operations)
    indexHashIds = subv.prepareIndexHashIds()

    app.getLogger().debug("Clean unused data blocks and hashes...")

    countHashes = tableHash.get_count()
    app.getLogger().debug(" hashes: %d", countHashes)

    count = 0
    current = 0
    proc = ""

    _curBlock = 0
    maxCnt = 10000

    while True:

        if current == countHashes:
            break

        hashIds = tableHash.get_hash_ids(_curBlock, _curBlock + maxCnt)

        current += len(hashIds)

        _curBlock += maxCnt
        if not hashIds:
            continue

        # SET magick
        to_delete = hashIds - indexHashIds

        id_str = ",".join((str(_id) for _id in to_delete))
        count += tableHash.remove_by_ids(id_str)
        tableBlock.remove_by_ids(id_str)
        tableHCT.remove_by_ids(id_str)
        tableHSZ.remove_by_ids(id_str)

        p = "%6.2f%%" % (100.0 * current / countHashes)
        if p != proc:
            proc = p
            app.getLogger().debug("%s (count=%d)", proc, count)

    msg = ""
    if count > 0:
        tableHash.commit()
        tableBlock.commit()
        tableHCT.commit()
        tableHSZ.commit()
        msg = "Cleaned up %i unused data block%s and hashes in %%s." % (
            count, count != 1 and 's' or '',
        )
    return count, msg


def do_defragment(options, _fuse):
    """
    Defragment only selected Subvolume

    @param options: Commandline options
    @type  options: object

    @param _fuse: FUSE wrapper
    @type  _fuse: dedupsqlfs.fuse.dedupfs.DedupFS
    """
    __collect_garbage(_fuse)
    return 0
