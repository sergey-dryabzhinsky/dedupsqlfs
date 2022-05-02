# -*- coding: utf8 -*-

__author__ = "sergey"


from time import time
import logging

from dedupsqlfs.my_formats import format_size, format_timespan
from dedupsqlfs.get_memory_usage import get_real_memory_usage, get_memory_usage


class ReportHelper:

    # Public properties

    report_interval = 60        # in seconds

    def __init__(self, app):
        """
        @param app: Класс приложения или операций
        @type app: dedupsqlfs.fuse.DedupOperations
        """

        self.application = app

        self.bytes_read = 0
        self.bytes_deduped = 0
        self.bytes_deduped_last = 0
        self.bytes_written = 0
        self.bytes_written_compressed = 0

        self.compressed_ratio = 0

        self.timing_report_last_run = time()
        self.time_spent_logging = 0
        self.time_spent_caching_items = 0
        self.time_spent_hashing = 0
        self.time_spent_interning = 0
        self.time_spent_querying_tree = 0
        self.time_spent_reading = 0
        self.time_spent_traversing_tree = 0
        self.time_spent_writing = 0
        self.time_spent_writing_meta = 0
        self.time_spent_writing_blocks = 0
        self.time_spent_commiting = 0

        self.time_spent_flushing_block_cache = 0
        self.time_spent_flushing_writed_block_cache = 0
        self.time_spent_flushing_readed_block_cache = 0
        self.time_spent_flushing_writedByTime_block_cache = 0
        self.time_spent_flushing_readedByTime_block_cache = 0
        self.time_spent_flushing_writedBySize_block_cache = 0
        self.time_spent_flushing_readedBySize_block_cache = 0

        self.time_spent_compressing = 0
        self.time_spent_decompressing = 0

        self.memory_usage = get_memory_usage()
        self.memory_usage_real = get_real_memory_usage()

        pass

    # Public methods

    def get_logger(self):
        return self.application.getLogger()

    def get_manager(self):
        return self.application.getManager()

    def get_table(self, name):
        return self.application.getTable(name)

    def get_option(self, key):
        return self.application.getOption(key)

    def do_print_stats_ontime(self, force=False):
        t_now = time()
        if (t_now - self.timing_report_last_run >= self.report_interval) or force:
            self.timing_report_last_run = t_now
            self.__print_stats()
        return

    # Private methods

    def __print_stats(self):
        if self.get_logger().isEnabledFor(logging.INFO) and self.get_option("verbose_stats"):
            self.get_logger().info('-' * 79)
            self.__report_memory_usage()
            self.__report_memory_usage_real()
            self.__report_compressed_usage()
            self.__report_deduped_usage()
            self.__report_throughput()
            if self.get_option("verbose_stats_detailed"):
                self.__report_timings()
                self.__report_database_timings()
                self.__report_database_operations()
                self.__report_cache_timings()
                self.__report_cache_operations()
            self.get_logger().info(' ' * 79)
        return

    def __report_timings(self):  # {{{3
        self.time_spent_logging = self.get_logger().getTimeIn()
        self.time_spent_caching_items = self.application.cached_attrs.getAllTimeSpent() + \
                                        self.application.cached_xattrs.getAllTimeSpent() + \
                                        self.application.cached_nodes.getAllTimeSpent() + \
                                        self.application.cached_names.getAllTimeSpent() + \
                                        self.application.cached_name_ids.getAllTimeSpent() + \
                                        self.application.cached_indexes.getAllTimeSpent() + \
                                        self.application.cached_blocks.getAllTimeSpent() + \
                                        self.application.cached_hash_sizes.getAllTimeSpent() + \
                                        self.application.cached_hash_compress.getAllTimeSpent()
        timings = [
            (self.time_spent_logging, 'Logging debug info'),
            (self.time_spent_caching_items,
                'Caching all items - inodes, tree-nodes, names, xattrs, indexes, blocks ...'),
            (self.time_spent_interning, 'Interning path components'),
            (self.time_spent_reading, 'Reading data stream'),
            (self.time_spent_writing, 'Writing data stream (cumulative: meta + blocks)'),
            (self.time_spent_writing_meta, 'Writing inode metadata'),
            (self.time_spent_writing_blocks, 'Writing data blocks (cumulative)'),
            (self.time_spent_writing_blocks - self.time_spent_compressing - self.time_spent_hashing,
                'Writing blocks to database'),
            (self.get_manager().getTimeSpent(), 'Database operations'),
            (self.time_spent_commiting, 'Commiting all changes to database'),
            (self.time_spent_flushing_writed_block_cache - self.time_spent_writing_blocks,
                'Flushing writed block cache'),
            (self.time_spent_flushing_readed_block_cache, 'Flushing readed block cache (cumulative)'),
            (self.time_spent_flushing_writed_block_cache, 'Flushing writed block cache (cumulative)'),
            (self.time_spent_flushing_writedByTime_block_cache, 'Flushing writed block cache (by Time)'),
            (self.time_spent_flushing_writedBySize_block_cache, 'Flushing writed block cache (by Size)'),
            (self.time_spent_flushing_readedByTime_block_cache, 'Flushing readed block cache (by Time)'),
            (self.time_spent_flushing_readedBySize_block_cache, 'Flushing readed block cache (by Size)'),
            (self.time_spent_flushing_block_cache, 'Flushing block cache (cumulative)'),
            (self.time_spent_hashing, 'Hashing data blocks'),
            (self.time_spent_compressing, 'Compressing data blocks'),
            (self.time_spent_decompressing, 'Decompressing data blocks'),
            (self.time_spent_querying_tree, 'Querying the tree')
        ]
        maxdescwidth = max([len(l) for t, l in timings]) + 3
        timings.sort(reverse=True)

        uptime = time() - self.application.fs_mounted_at
        self.get_logger().info("Filesystem mounted: %s", format_timespan(uptime))

        printed_heading = False
        for timespan, description in timings:
            percentage = 100.0 * timespan / uptime
            if percentage >= 0.1:
                if not printed_heading:
                    self.get_logger().info("Cumulative timings of slowest operations:")
                    printed_heading = True
                self.get_logger().info(
                    " - %-*s%s (%.1f%%)", maxdescwidth, description + ':', format_timespan(timespan), percentage)

    def __report_database_timings(self):  # {{{3
        if self.get_logger().isEnabledFor(logging.INFO):
            timings = []
            for tn in self.get_manager().tables:
                t = self.get_table(tn)

                opTimes = t.getTimeSpent()
                for op, timespan in opTimes.items():
                    timings.append((timespan, 'Table %r - operation %r timings' % (tn, op,),))

            maxdescwidth = max([len(l) for t, l in timings]) + 3
            timings.sort(reverse=True)

            alltime = self.get_manager().getTimeSpent()
            self.get_logger().info("Database all operations timings: %s", format_timespan(alltime))

            printed_heading = False
            for timespan, description in timings:
                percentage = 100.0 * timespan / alltime
                if percentage >= 0.1:
                    if not printed_heading:
                        self.get_logger().info("Cumulative timings of slowest tables:")
                        printed_heading = True
                    self.get_logger().info(
                        " - %-*s%s (%.1f%%)", maxdescwidth, description + ':', format_timespan(timespan), percentage)

    def __report_database_operations(self):  # {{{3
        if self.get_logger().isEnabledFor(logging.INFO):
            counts = []
            allcount = 0
            for tn in self.get_manager().tables:
                t = self.get_table(tn)

                opCount = t.getOperationsCount()
                for op, count in opCount.items():
                    counts.append((count, 'Table %r - operation %r count' % (tn, op,),))
                    allcount += count

            maxdescwidth = max([len(l) for t, l in counts]) + 3
            counts.sort(reverse=True)

            self.get_logger().info("Database all operations: %s", allcount)

            printed_heading = False
            for count, description in counts:
                percentage = 100.0 * count / allcount
                if percentage >= 0.1:
                    if not printed_heading:
                        self.get_logger().info("Cumulative count of operations:")
                        printed_heading = True
                    self.get_logger().info(
                        " - %-*s%s (%.1f%%)", maxdescwidth, description + ':', count, percentage)

    def __report_cache_timings(self):  # {{{3
        if self.get_logger().isEnabledFor(logging.INFO):
            timings = []
            for cn in "cached_attrs", "cached_xattrs", "cached_nodes", "cached_names", "cached_name_ids", \
                "cached_indexes", "cached_blocks", "cached_hash_sizes", "cached_hash_compress":
                c = getattr(self.application, cn)

                opTimes = c.getTimeSpent()
                for op, timespan in opTimes.items():
                    timings.append((timespan, 'Cache %r - operation %r timings' % (cn, op,),))

            maxdescwidth = max([len(l) for t, l in timings]) + 3
            timings.sort(reverse=True)

            alltime = self.time_spent_caching_items
            self.get_logger().info("Cache all operations timings: %s", format_timespan(alltime))

            printed_heading = False
            for timespan, description in timings:
                percentage = 100.0 * timespan / alltime
                if percentage >= 0.1:
                    if not printed_heading:
                        self.get_logger().info("Cumulative timings of slowest caches:")
                        printed_heading = True
                    self.get_logger().info(
                        " - %-*s%s (%.1f%%)", maxdescwidth, description + ':', format_timespan(timespan), percentage)

    def __report_cache_operations(self):  # {{{3
        if self.get_logger().isEnabledFor(logging.INFO):
            counts = []
            allcount = 0
            for cn in "cached_attrs", "cached_xattrs", "cached_nodes", "cached_names", "cached_name_ids", \
                "cached_indexes", "cached_blocks", "cached_hash_sizes", "cached_hash_compress":
                c = getattr(self.application, cn)

                opCount = c.getOperationsCount()
                for op, count in opCount.items():
                    counts.append((count, 'Cache %r - operation %r count' % (cn, op,),))
                    allcount += count

            maxdescwidth = max([len(l) for t, l in counts]) + 3
            counts.sort(reverse=True)

            self.get_logger().info("Cache all operations: %s", allcount)

            printed_heading = False
            for count, description in counts:
                percentage = 100.0 * count / allcount
                if percentage >= 0.1:
                    if not printed_heading:
                        self.get_logger().info("Cumulative count of operations:")
                        printed_heading = True
                    self.get_logger().info(
                        " - %-*s%s (%.1f%%)", maxdescwidth, description + ':', count, percentage)

    def __report_memory_usage(self):  # {{{3
        memory_usage = get_memory_usage()
        msg = "Current virtual memory usage is " + format_size(memory_usage)
        difference = abs(memory_usage - self.memory_usage)
        if self.memory_usage != 0 and difference:
            direction = self.memory_usage < memory_usage and 'up' or 'down'
            msg += " (%s by %s)" % (direction, format_size(difference))
        self.get_logger().info(msg + '.')
        self.memory_usage = memory_usage

    def __report_memory_usage_real(self):  # {{{3
        memory_usage = get_real_memory_usage()
        msg = "Current real memory usage is " + format_size(memory_usage)
        difference = abs(memory_usage - self.memory_usage_real)
        if self.memory_usage_real != 0 and difference:
            direction = self.memory_usage_real < memory_usage and 'up' or 'down'
            msg += " (%s by %s)" % (direction, format_size(difference))
        self.get_logger().info(msg + '.')
        self.memory_usage_real = memory_usage

    def __report_deduped_usage(self):  # {{{3
        msg = "Current deduped stream bytes is " + format_size(self.bytes_deduped)
        difference = abs(self.bytes_deduped - self.bytes_deduped_last)
        if self.bytes_deduped_last != 0 and difference:
            direction = self.bytes_deduped_last < self.bytes_deduped and 'up' or 'down'
            msg += " (%s by %s)" % (direction, format_size(difference))
        self.get_logger().info(msg + '.')
        self.bytes_deduped_last = self.bytes_deduped

    def __report_compressed_usage(self):  # {{{3
        if self.bytes_written:
            ratio = (self.bytes_written - self.bytes_written_compressed) * 100.0 / self.bytes_written
        else:
            ratio = 0
        msg = "Current stream bytes compression ratio is %.2f%%" % ratio
        difference = abs(ratio - self.compressed_ratio)
        if self.compressed_ratio != 0 and difference:
            direction = self.compressed_ratio < ratio and 'up' or 'down'
            msg += " (%s by %.2f%%)" % (direction, difference)
        msg += " (%s to %s)" % (format_size(self.bytes_written), format_size(self.bytes_written_compressed))
        self.get_logger().info(msg + '.')
        self.compressed_ratio = ratio

    def __report_throughput(self, nbytes=None, nseconds=None, label=None):  # {{{3
        if nbytes == None:
            self.__report_throughput(self.bytes_read, self.time_spent_reading, "read")
            self.__report_throughput((self.bytes_written + self.bytes_deduped), self.time_spent_writing, "write")
        else:
            if nbytes > 0:
                average = format_size(nbytes / max(1, nseconds))
                self.get_logger().info("Average %s stream speed is %s/s.", label, average)
                # Decrease the influence of previous measurements over time?
                # if nseconds > 60 and nbytes > 1024 ** 2:
                #    return nbytes / 2, nseconds / 2
            return nbytes, nseconds

    pass
