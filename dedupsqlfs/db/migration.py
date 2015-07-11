# -*- coding: utf8 -*-

__author__ = 'sergey'

import os
import sys
import re

class DbMigration( object ):

    _manager = None
    _log = None

    _migrationsDir = None
    _migrationsList = None

    def __init__( self, manager, log ):
        self._manager = manager
        self._log = log
        pass

    def getMigrationsDir(self):
        if not self._migrationsDir:
            baseDir = os.path.dirname( os.path.dirname( os.path.realpath(sys.argv[0]) ) )
            path = os.path.join(baseDir, "dedupsqlfs", "db", "migrations")
            if os.path.isdir(path):
                self._migrationsDir = path
            else:
                raise NotADirectoryError("Directory for migrations not found: %s" % path)
        return self._migrationsDir

    def getMigrations(self):
        if not self._migrationsList:
            self._migrationsList = os.listdir(self.getMigrationsDir())
        return self._migrationsList

    def getMigrationNumber(self, migrFile):
        if not migrFile:
            return 0
        name = os.path.basename(migrFile)
        number = int(re.sub(r"[^\d]+", "", name))
        return number

    def isMigrationNeeded(self):

        tableOpts = self._manager.getTable("option")

        migration = tableOpts.get("migration")
        if not migration:
            return True
        else:
            migration = int(migration)

        lastM = self.getMigrationNumber( self.getMigrations()[-1] )
        if lastM and lastM > migration:
            return True

        return False

    def run_migration(self, migFile):

        sys.path.insert(0, self.getMigrationsDir() )

        import imp

        migMod = migFile.replace(".py", "")

        fp, pathname, description = imp.find_module( migMod )
        module = imp.load_module(migMod, fp, pathname, description)

        module.run( self._manager )

        sys.path.pop(0)

        return

    def process(self):
        tableOpts = self._manager.getTable("option")

        migration = tableOpts.get("migration")
        if not migration:
            return True
        else:
            migration = int(migration)

        migSort = {}
        for mf in self.getMigrations():
            number = self.getMigrationNumber(mf)
            migSort[ number ] = mf

        migKeys = list(migSort.keys())
        migKeys.sort()

        for mn in migKeys:
            if mn <= migration:
                continue

            self._log.info("Run migration %r from file %r" % (mn, migSort[mn],))
            self.run_migration( migSort[ mn ] )

        return

    pass
