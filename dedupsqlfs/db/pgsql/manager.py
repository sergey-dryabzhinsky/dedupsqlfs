# -*- coding: utf8 -*-

__author__ = 'sergey'

import os
from time import sleep
from datetime import datetime
import subprocess

import psycopg2

class DbManager( object ):

    TYPE = "pgsql"

    _table = None

    _db_name = "dedupsqlfs"
    _base_path = "/dev/shm/db"
    _autocommit = True
    _synchronous = True

    _socket = None
    _notmeStarted = False
    _user = "postgres"
    _pass = ""

    _conn = None

    _log = None

    _pgsqld_proc = None
    """
    @ivar _pgsqld_proc:
    @type _pgsqld_proc: L{subprocess.Popen}
    """

    _buffer_size = 512*1024*1024
    """
    @ivar _buffer_size: buffer size
    """

    tables = (
        "option",
        "tree",
        "name",
        "name_pattern_option",
        "inode",
        "inode_option",
        "link",
        "block",
        "xattr",
        "compression_type",
        "hash",
        "hash_compression_type",
        "hash_sizes",
        "inode_hash_block",
        "subvolume",
    )

    def __init__( self, dbname = None, base_path=None, autocommit=None, synchronous=None ):
        if not (dbname is None):
            self._db_name = dbname
        if not (base_path is None):
            self._base_path = dbname
        if not (autocommit is None):
            self._autocommit = autocommit == True
        if not (synchronous is None):
            self._synchronous = synchronous == True
        self._table = {}
        pass

    def setLogger(self, logger):
        self._log = logger
        return self

    def getLogger(self):
        return self._log


    def setSynchronous(self, flag=True):
        """
        Change startup defaults
        """
        self._synchronous = flag == True
        if self._mysqld_proc:
            conn = self.getConnection(True)
            cur = conn.cursor()
            if flag:
                cur.execute("SET GLOBAL innodb_flush_log_at_trx_commit=1")
                cur.execute("SET GLOBAL flush=1")
            else:
                cur.execute("SET GLOBAL innodb_flush_log_at_trx_commit=2")
                cur.execute("SET GLOBAL flush=0")
            cur.close()
        return self

    def getSynchronous(self):
        return self._synchronous

    def setAutocommit(self, flag=True):
        """
        Change startup defaults
        """
        self._autocommit = flag == True
        if self._mysqld_proc:
            conn = self.getConnection(True)
            cur = conn.cursor()
            if flag:
                cur.execute("SET GLOBAL autocommit=1")
            else:
                cur.execute("SET GLOBAL autocommit=0")
            cur.close()
        return self

    def getAutocommit(self):
        return self._autocommit

    def setBasepath(self, base_path):
        self._base_path = base_path
        return self

    def setBufferSize(self, in_bytes):
        self._buffer_size = in_bytes
        return self

    def getBasePath(self):
        return self._base_path

    def getDbName(self):
        return self._db_name

    def getSocket(self):
        return self._socket

    def getUser(self):
        return self._user

    def getPassword(self):
        return self._pass


    def getIsMariaDB(self):
        return self._is_mariadb


    def getTable(self, name, nocreate=False):
        if name not in self._table:
            if name == "option":
                from dedupsqlfs.db.pgsql.table.option import TableOption
                self._table[ name ] = TableOption(self)
            elif name == "tree":
                from dedupsqlfs.db.pgsql.table.tree import TableTree
                self._table[ name ] = TableTree(self)
            elif name.startswith("tree_"):
                from dedupsqlfs.db.pgsql.table.tree import TableTree
                self._table[ name ] = TableTree(self)
                self._table[ name ].setName(name)
            elif name == "name":
                from dedupsqlfs.db.pgsql.table.name import TableName
                self._table[ name ] = TableName(self)
            elif name == "inode":
                from dedupsqlfs.db.pgsql.table.inode import TableInode
                self._table[ name ] = TableInode(self)
            elif name.startswith("inode_") \
                    and not name.startswith("inode_hash_block") \
                    and not name.startswith("inode_option"):
                from dedupsqlfs.db.pgsql.table.inode import TableInode
                self._table[ name ] = TableInode(self)
                self._table[ name ].setName(name)
            elif name == "link":
                from dedupsqlfs.db.pgsql.table.link import TableLink
                self._table[ name ] = TableLink(self)
            elif name.startswith("link_"):
                from dedupsqlfs.db.pgsql.table.link import TableLink
                self._table[ name ] = TableLink(self)
                self._table[ name ].setName(name)
            elif name == "block":
                from dedupsqlfs.db.pgsql.table.block import TableBlock
                self._table[ name ] = TableBlock(self)
            elif name == "xattr":
                from dedupsqlfs.db.pgsql.table.xattr import TableInodeXattr
                self._table[ name ] = TableInodeXattr(self)
            elif name.startswith("xattr_"):
                from dedupsqlfs.db.pgsql.table.xattr import TableInodeXattr
                self._table[ name ] = TableInodeXattr(self)
                self._table[ name ].setName(name)
            elif name == "compression_type":
                from dedupsqlfs.db.pgsql.table.compression_type import TableCompressionType
                self._table[ name ] = TableCompressionType(self)
            elif name == "hash_compression_type":
                from dedupsqlfs.db.pgsql.table.hash_compression_type import TableHashCompressionType
                self._table[ name ] = TableHashCompressionType(self)
            elif name == "hash_sizes":
                from dedupsqlfs.db.pgsql.table.hash_sizes import TableHashSizes
                self._table[ name ] = TableHashSizes(self)
            elif name == "name_pattern_option":
                from dedupsqlfs.db.pgsql.table.name_pattern_option import TableNamePatternOption
                self._table[ name ] = TableNamePatternOption(self)
            elif name == "inode_option":
                from dedupsqlfs.db.pgsql.table.inode_option import TableInodeOption
                self._table[ name ] = TableInodeOption(self)
            elif name.startswith("inode_option_"):
                from dedupsqlfs.db.pgsql.table.inode_option import TableInodeOption
                self._table[ name ] = TableInodeOption(self)
                self._table[ name ].setName(name)
            elif name == "hash":
                from dedupsqlfs.db.pgsql.table.hash import TableHash
                self._table[ name ] = TableHash(self)
            elif name == "inode_hash_block":
                from dedupsqlfs.db.pgsql.table.inode_hash_block import TableInodeHashBlock
                self._table[ name ] = TableInodeHashBlock(self)
            elif name.startswith("inode_hash_block_"):
                from dedupsqlfs.db.pgsql.table.inode_hash_block import TableInodeHashBlock
                self._table[ name ] = TableInodeHashBlock(self)
                self._table[ name ].setName(name)
            elif name == "subvolume":
                from dedupsqlfs.db.pgsql.table.subvolume import TableSubvolume
                self._table[ name ] = TableSubvolume(self)
            elif name == "tmp_ids":
                from dedupsqlfs.db.pgsql.table.tmp_ids import TableTmpIds
                self._table[ name ] = TableTmpIds(self)
            elif name == "tmp_id_count":
                from dedupsqlfs.db.pgsql.table.tmp_id_count import TableTmpIdCount
                self._table[ name ] = TableTmpIdCount(self)
            else:
                raise ValueError("Unknown database %r" % name)

            if not nocreate:
                if not self._table[ name ].hasTable():
                    self._table[ name ].create()

        return self._table[ name ]


    def unsetTable(self, name):
        if name in self._table:
            del self._table[name]
        return self


    def isSupportedStorage(self):
        s = False
        datadir = self.getBasePath() + "/pgsql-db-data"
        if os.path.isdir(datadir):
            s = True
        return s


    def startPgSqld(self):
        if self._pgsqld_proc is None:

            setupfile = self.getBasePath() + "/setup.log"
            outputfile = self.getBasePath() + "/console.log"
            errorfile = self.getBasePath() + "/error.log"
            slowlogfile = self.getBasePath() + "/slow.log"
            pidfile = self.getBasePath() + "/pgsql.pid"
            self._socket = self.getBasePath() + "/pgsql.sock"

            if os.path.exists(self._socket):
                self._notmeStarted = True
                return True

            tmpdir = self.getBasePath() + "/tmp"
            if not os.path.isdir(tmpdir):
                os.makedirs(tmpdir, 0o0750)

            is_new = False
            datadir = self.getBasePath() + "/pgsql-db-data"
            if not os.path.isdir(datadir):
                is_new = True
                os.makedirs(datadir, 0o0750)

            output = subprocess.check_output(["mysqld", "--verbose", "--help"], stderr=subprocess.DEVNULL)

            cmd_opts = [
                "--basedir=/usr",
                "--datadir=%s" % datadir,
                "--tmpdir=%s" % tmpdir,
                "--plugin-dir=/usr/lib/mysql/plugin",       # Linux / Debian specific?
                "--log-error=%s" % errorfile,
                "--slow-query-log",
                "--slow-query-log-file=%s" % slowlogfile,
                "--pid-file=%s" % pidfile,
                "--skip-grant-tables",                      # Grant root-access
                "--skip-bind-address",
                "--skip-networking",
                "--skip-name-resolve",
                "--socket=%s" % self.getSocket(),
                # TODO: options
                "--connect-timeout=10",
                "--interactive-timeout=3600",
                "--wait-timeout=3600",
                "--tmp-table-size=64M",
                "--max-heap-table-size=64M",
            ]

            if os.geteuid() == 0:
                cmd_opts.append("--user=mysql")
                for f in (self.getBasePath(), tmpdir, datadir, setupfile, errorfile, outputfile, slowlogfile, pidfile, self.getSocket(),):
                    if os.path.exists(f):
                        subprocess.Popen([
                            "chown",
                            "mysql:mysql",
                            f
                        ]).wait()

            if self.getAutocommit():
                cmd_opts.append("--autocommit")
            else:
                cmd_opts.append("--skip-autocommit")

            if self.getSynchronous():
                cmd_opts.append("--flush")
                cmd_opts.append("--innodb-flush-log-at-trx-commit=1")
            else:
                cmd_opts.append("--innodb-flush-log-at-trx-commit=2")

            cmd_opts.extend([
                "--big-tables",
                # TODO: warn about hugetlbfs mount and sysctl setup
                #"--large-pages",

                "--query-cache-min-res-unit=1k",
                "--query-cache-limit=4M",
                "--query-cache-size=64M",
                "--max-allowed-packet=32M",
            ])

            if is_new:

                self.getLogger().info("Setup new PgSQL system databases")

                cmd = ["mysql_install_db"]
                cmd.extend(cmd_opts)

                self.getLogger().debug("CMD: %r" % (cmd,))

                sf = open(setupfile, 'a')
                sf.write("\n---=== %s ===---\n" % datetime.now())
                retcode = subprocess.Popen(
                    cmd,
                    cwd=self.getBasePath(),
                    stdout=sf,
                    stderr=subprocess.STDOUT
                ).wait()
                sf.close()
                if retcode:
                    self.getLogger().error("Something wrong! Return code: %s" % retcode)
                    return False

            cmd = ["mysqld"]
            cmd.extend(cmd_opts)

            self.getLogger().info("Starting up PgSQLd...")

            self.getLogger().debug("CMD: %r" % (cmd,))

            of = open(outputfile, 'a')
            of.write("\n---=== %s ===---\n" % datetime.now())
            self._mysqld_proc = subprocess.Popen(
                cmd,
                cwd=self.getBasePath(),
                stdout=of,
                stderr=subprocess.STDOUT
            )

            t = 30
            self.getLogger().info("Wait up %s sec for server to start, or til ping is pong..." % t)
            while (t>0):
                sleep(0.1)

                if os.path.exists(self.getSocket()):
                    if self.pingServer():
                        break
                if self._mysqld_proc.poll() is not None:
                    break

                t -= 0.1


            if self._mysqld_proc.poll() is not None:
                self.getLogger().error("Something wrong? mysqld exited with: %s" % self._mysqld_proc.poll() )
                self._mysqld_proc = None
                of.close()
                return False

            self.getLogger().info("Done in %s seconds." % t)

            self.createDb()

        return True


    def stopPgSqld(self):
        if self._mysqld_proc is not None:

            if self._notmeStarted:
                self._notmeStarted = False
                return True

            outputfile = self.getBasePath() + "/mysqladmin.log"

            cmd = [
                "mysqladmin",
                "--user=root",
                "--socket=%s" % self.getSocket(),
                "shutdown"
            ]

            self.getLogger().info("Call MySQLd shutdown")

            of = open(outputfile, "a")
            of.write("\n---=== %s ===---\n" % datetime.now())
            ret = subprocess.Popen(
                cmd,
                cwd=self.getBasePath(),
                stdout=of, stderr=subprocess.STDOUT
            ).wait()
            of.close()

            if ret:
                self.getLogger().warning("Call MySQLadmin returned code=%r! Something wrong!" % ret)
                return False

            t = 30
            self.getLogger().info("Wait up %s sec for it to stop..." % t)
            while (t>0):
                sleep(0.1)
                if self._mysqld_proc.poll() is not None:
                    break
                t -= 0.1

            if self._mysqld_proc.poll() is None:
                self.getLogger().warning("Terminate MySQLd")
                self._mysqld_proc.terminate()

                t2 = 5
                while (t2>0):
                    sleep(0.1)
                    if self._mysqld_proc.poll() is not None:
                        break
                    t2 -= 0.1

            if self._mysqld_proc.poll() is None:
                self.getLogger().error("Can't stop mysqld!")
                return False

            self.getLogger().info("Done in %s seconds" % t)

            self._mysqld_proc = None
            self._socket = None

            return True

        return False


    def getConnection(self, nodb=False):
        """
        @rtype L{pymysqlConnection}
        """
        if not self.startPgSqld():
            raise RuntimeError("Can't start PostgreSQL server!")

        if nodb:
            conn = psycopg2.connect().connect(unix_socket=self.getSocket(), user=self.getUser(), passwd=self.getPassword())
        else:

            if self._conn:
                return self._conn

            conv = pymysql.converters.conversions.copy()
            conv[246]=float     # convert decimals to floats
            conv[10]=str        # convert dates to strings

            conn = self._conn = pymysql.connect(
                unix_socket=self.getSocket(),
                user=self.getUser(),
                passwd=self.getPassword(),
                db=self.getDbName(),
                conv=conv
            )
            self._conn.autocommit(self.getAutocommit())

        cur = conn.cursor()
        if not self.getAutocommit():
            cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        cur.close()

        return conn

    def getCursor(self, new=False):
        cur = self.getConnection().cursor(cursor_type)
        cur = self.pingDb(cur)
        return cur

    def pingServer(self):
        result = True
        try:
            conn = self.getConnection(True)
            cursor = conn.cursor()
            cursor.execute('SELECT 1')
            cursor.close()
            conn.close()
        except pymysql.err.OperationalError:
            result = False
        return result

    def pingDb(self, cursor):
        try:
            cursor.execute('SELECT 1')
        except BrokenPipeError:
            self.closeConn()
            cursor = self.getConnection().cursor(cursor_type)
            pass
        return cursor

    def hasDb(self, conn):
        cur = conn.cursor(cursor_type)

        cur.execute(
            "SELECT COUNT(1) AS `DbIsThere` "+
            "FROM `INFORMATION_SCHEMA`.`STATISTICS` "+
            "WHERE `table_schema` = %s;",
            (self.getDbName(),)
        )
        row = cur.fetchone()

        exists = (row is not None) and int(row['DbIsThere']) > 0

        return exists

    def createDb(self):

        conn = self.getConnection(True)

        if not self.hasDb(conn):

            cur = conn.cursor(cursor_type)
            cur.execute("CREATE DATABASE IF NOT EXISTS `%s` COLLATE utf8_bin;" % self.getDbName())
            cur.close()

        conn.close()
        return True

    def begin(self):
        for name, t in self._table.items():
            t.begin()
        return self

    def commit(self):
        for name, t in self._table.items():
            t.commit()
        return self

    def rollback(self):
        for name, t in self._table.items():
            t.rollback()
        return self

    def vacuum(self):
        sz = 0
        for name, t in self._table.items():
            sz += t.vacuum()
        return sz

    def closeConn(self):
        if self._conn:
            self._conn.close()
            self._conn = None
        return self

    def close(self):
        self.closeConn()
        if self._pgsqld_proc is not None:
            self.stopPgSqld()
        return self

    def getSize(self):
        s = 0
        for name, t in self._table.items():
            s += t.getSize()
        return s

    def getOperationsCount(self):
        s = 0
        for name, t in self._table.items():
            s += t.getAllOperationsCount()
        return s

    def getTimeSpent(self):
        s = 0
        for name, t in self._table.items():
            s += t.getAllTimeSpent()
        return s

    def create(self):
        return self


    # Dummy functions
    def setCompressionProg(self, prog):
        return self

    def getCompressionProg(self):
        return None


    def copy(self, oldTableName, newTableName, compress_dummy=False):
        """
        Copy tables
        @param oldTableName:    Old table name
        @param newTableName:    New table nane
        @param compress_dummy:  Dummy params to be equal api with Sqlite
        @return:
        """
        self.getTable(oldTableName)
        t2 = self.getTable(newTableName)

        # Rename files
        t2.getCursor().execute("INSERT `%s` SELECT * FROM `%s`;" % (newTableName, oldTableName,))
        return self

    pass