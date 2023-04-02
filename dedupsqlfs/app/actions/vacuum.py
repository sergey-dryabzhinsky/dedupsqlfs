# -*- coding: utf8 -*-

"""
Special action to vacuum all databases
"""

__author__ = 'sergey'

from time import time
from datetime import datetime

from dedupsqlfs.my_formats import format_timespan, format_size


def __vacuum_datatable(app, tableName):  # {{{4
    """
    @param app:
    @type app: dedupsqlfs.fuse.dedupfs.DedupFS
    @return: None
    """

    sub_start_time = time()
    app.getLogger().debug(" vacuum %s table", tableName)
    t = app.operations.getTable(tableName)
    if t.getClustered():
        app.getLogger().debug(" %s table is clustered! skip, @todo", tableName)
    else:
        sz = app.operations.getTable(tableName).vacuum()
    msg = " vacuumed SQLite data store in %s."
    elapsed_time = time() - sub_start_time
    app.getLogger().debug(msg, format_timespan(elapsed_time))
    return sz

def forced_vacuum(app, options):
    """
    @param app:
    @type app: dedupsqlfs.fuse.dedupfs.DedupFS
    @return: None
    """
    start_time = time()
    app.getLogger().info("Performing data vacuum (this might take a while) ..")
    sz = 0
    dbsz = 0

    opts = app.operations.getTable('option')
    lastV = opts.get("last_vacuum")
    curDT = datetime.now()
    curDTiso = curDT.isoformat()
    if lastV:
        lastDT = datetime.strptime(lastV, "%Y-%m-%dT%H:%M:%S.%f")
        dt = curDT - lastDT
        if options.vacuum_older_than > 0:
            if dt.days < options.vacuum_older_than:
                app.getLogger().info("Last vacuum was only %s days ago. Skip action..." % dt.days)
                return

    hsz = app.operations.getTable('hash_sizes')
    pagesz = hsz.get_median_compressed_size()
    bt = app.operations.getTable('block')
    bt.setPageSize(pagesz)

    app.getLogger().info("Selected block table page size is: %s" % format_size(bt.getPageSize()))

    for table_name in app.operations.getManager().tables:
        dbsz += app.operations.getTable(table_name).getFileSize()
    for table_name in app.operations.getManager().tables:
        sz += __vacuum_datatable(app, table_name)

    if not lastV:
        opts.insert("last_vacuum", curDTiso)
    else:
        opts.update("last_vacuum", curDTiso)
    opts.commit()

    elapsed_time = time() - start_time

    diffSign = ''
    if sz > 0:
        diffSign = '+'
    elif sz < 0:
        diffSign = '-'

    prsz = format_size(abs(sz))

    if app.getOption("parsable"):
        app.getLogger().info("Diff bytes: %s%s", diffSign, prsz)
        app.getLogger().info("Diff percent: %s%.2f%%", diffSign, abs(sz) * 100.0 / dbsz)
        app.getLogger().info("Time: %s", format_timespan(elapsed_time))
    else:
        app.getLogger().info("Total DB size change after vacuum: %s%.2f%% (%s%s)",
                         diffSign, abs(sz) * 100.0 / dbsz, diffSign, prsz)
        app.getLogger().info("Finished data vacuum in %s.", format_timespan(elapsed_time))

    return

def do_vacuum(options, _fuse):
    """
    Defragment only selected Subvolume

    @param options: Commandline options
    @type  options: object

    @param _fuse: FUSE wrapper
    @type  _fuse: dedupsqlfs.fuse.dedupfs.DedupFS
    """
    forced_vacuum(_fuse, options)
    return 0
