# -*- coding: utf8 -*-

__author__ = 'sergey'

from time import sleep
import subprocess
import pymysql

class DbManager( object ):

    _table = None
    _db_name = "dedupsqlfs"
    _base_path = "/dev/shm/db"
    _autocommit = True
    _synchronous = True

    _socket = None
    _user = "root"
    _pass = ""

    _mysqld_proc = None
    """
    @ivar _mysqld_proc:
    @type _mysqld_proc: L{subprocess.Popen}
    """

    _buffer_size = 256*1024*1024

    tables = (
        "option",
        "tree",
        "name",
        "inode",
        "link",
        "block",
        "xattr",
        "compression_type",
        "hash",
        "hash_compression_type",
        "hash_block_size",
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


    def setSynchronous(self, flag=True):
        self._synchronous = flag == True
        return self

    def getSynchronous(self):
        return self._synchronous

    def setAutocommit(self, flag=True):
        self._autocommit = flag == True
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


    def getTable(self, name):
        if name not in self._table:
            if name == "option":
                from dedupsqlfs.db.mysql.table.option import TableOption
                self._table[ name ] = TableOption(self)
            elif name == "tree":
                from dedupsqlfs.db.mysql.table.tree import TableTree
                self._table[ name ] = TableTree(self)
            elif name == "name":
                from dedupsqlfs.db.mysql.table.name import TableName
                self._table[ name ] = TableName(self)
            elif name == "inode":
                from dedupsqlfs.db.mysql.table.inode import TableInode
                self._table[ name ] = TableInode(self)
            elif name == "link":
                from dedupsqlfs.db.mysql.table.link import TableLink
                self._table[ name ] = TableLink(self)
            elif name == "block":
                from dedupsqlfs.db.mysql.table.block import TableBlock
                self._table[ name ] = TableBlock(self)
            elif name == "xattr":
                from dedupsqlfs.db.mysql.table.xattr import TableInodeXattr
                self._table[ name ] = TableInodeXattr(self)
            elif name == "compression_type":
                from dedupsqlfs.db.mysql.table.compression_type import TableCompressionType
                self._table[ name ] = TableCompressionType(self)
            elif name == "hash_compression_type":
                from dedupsqlfs.db.mysql.table.hash_compression_type import TableHashCompressionType
                self._table[ name ] = TableHashCompressionType(self)
            elif name == "hash_block_size":
                from dedupsqlfs.db.mysql.table.hash_block_size import TableHashBlockSize
                self._table[ name ] = TableHashBlockSize(self)
            elif name == "hash":
                from dedupsqlfs.db.mysql.table.hash import TableHash
                self._table[ name ] = TableHash(self)
            elif name == "inode_hash_block":
                from dedupsqlfs.db.mysql.table.inode_hash_block import TableInodeHashBlock
                self._table[ name ] = TableInodeHashBlock(self)
            elif name == "subvolume":
                from dedupsqlfs.db.mysql.table.subvolume import TableSubvolume
                self._table[ name ] = TableSubvolume(self)
            else:
                raise ValueError("Unknown database %r" % name)
        return self._table[ name ]


    def startMysqld(self):
        if self._mysqld_proc is None:

            logfile = self.getBasePath() + "/error.log"
            pidfile = self.getBasePath() + "/mysql.pid"
            self._socket = self.getBasePath() + "/mysql.sock"

            cmd = [
                "mysqld",
                "--area=OFF",
                "--basedir=/usr",
                "--datadir=%s" % self.getBasePath(),
                "--plugin-dir=/usr/lib/mysql/plugin",
                "--log-error=%s" % logfile,
                "--pid-file=%s" % pidfile,
                "--skip-grant-tables",
                "--socket=%s" % self.getSocket(),
                "--default-storage-engine=InnoDB"
            ]
            if self.getAutocommit():
                cmd.append("--autocommit")
            else:
                cmd.append("--skip-autocommit")

            if self.getSynchronous():
                cmd.append("--flush")
                cmd.append("--innodb-flush-log-at-trx-commit=1")
            else:
                cmd.append("--innodb-flush-log-at-trx-commit=2")
                cmd.append("--skip-innodb-doublewrite")

            cmd.extend([
                "--big-tables",
                "--large-pages",
                "--innodb-file-per-table",
                "--innodb-flush-method=O_DIRECT",
                "--innodb-buffer-pool-size=%dM" % (self._buffer_size/1024/1024),
                "--innodb-log-file-size=32M",
                "--innodb-log-buffer-size=1M",

                "--query-cache-min-res-unit=1k",
                "--query-cache-limit=8M",
                "--query-cache-size=32M"
            ])

            self._mysqld_proc = subprocess.Popen(cmd, cwd=self.getBasePath())

            sleep(5)

            if self._mysqld_proc.poll() is not None:
                self._mysqld_proc = None
                return False

            self.createDb()

        return True

    def stopMysqld(self):
        if self._mysqld_proc is not None:

            cmd = [
                "mysqladmin",
                "--socket=%s" % self.getSocket(),
                "shutdown"
            ]

            subprocess.Popen(cmd).wait()

            sleep(5)

            if self._mysqld_proc.poll() is None:
                self._mysqld_proc.terminate()

            sleep(5)

            if self._mysqld_proc.poll() is None:
                return False

            self._mysqld_proc = None
            self._socket = None

        return True


    def getConnection(self, nodb=False):
        """
        @rtype L{pymysqlConnection}
        """
        if not self.startMysqld():
            raise RuntimeError("Can't start mysqld server!")

        if nodb:
            conn = pymysql.connect(unix_socket=self.getSocket(), user=self.getUser(), passwd=self.getPassword())
        else:
            conn = pymysql.connect(
                unix_socket=self.getSocket(),
                user=self.getUser(),
                passwd=self.getPassword(),
                db=self.getDbName())
        return conn


    def createDb(self):

        conn = self.getConnection(True)

        cur = conn.cursor()
        cur.execute("CREATE DATABASE IF NOT EXISTS `%s` COLLATE utf8_bin;" % self.getDbName())

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
        for name, t in self._table.items():
            t.vacuum()
        return self

    def close(self):
        for name, t in self._table.items():
            t.close()
        return self

    def getSize(self):
        s = 0
        for name, t in self._table.items():
            s += t.getSize()
        return s

    def getFileSize(self):
        s = 0
        for name in self.tables:
            t = self.getTable(name)
            s += t.getFileSize()
        return s

    def getOperationsCount(self):
        s = 0
        for name in self.tables:
            t = self.getTable(name)
            s += t.getAllOperationsCount()
        return s

    def getTimeSpent(self):
        s = 0
        for name in self.tables:
            t = self.getTable(name)
            s += t.getAllTimeSpent()
        return s

    def create(self):
        for t in self.tables:
            self.getTable(t).create()
        return self

    pass
