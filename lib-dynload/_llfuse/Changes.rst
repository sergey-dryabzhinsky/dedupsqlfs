===========
 Changelog
===========

.. currentmodule:: llfuse

**WARNING**: Python-LLFUSE is no longer actively developed.

Release 1.4.4 (2023-05-21)
==========================

- CI: use the matrix for cy/py combinations, support and test on Cython 3 beta
- cy3: cdef void* f(void* d) noexcept with gil, #78
- cy3: cdef nogil -> noexcept nogil
- tests: use shutil.which() instead of which(1) executable
- tests/examples: fix tmpfs: backport fix from pyfuse3
- tests/examples: fix lltest: add statfs implementation, remove -l from umount

Release 1.4.3 (2023-05-09)
==========================

* cythonize with Cython 0.29.34 (brings python 3.12 support)
* also test on python 3.12-dev
* add a minimal pyproject.toml, #70
* fix basedir in setup.py (malfunctioned with pip install -e .)
* tests: fix integer overflow on 32-bit architectures

Release 1.4.2 (2022-05-31)
==========================

* cythonize with Cython 0.29.30 (brings python 3.11 support)
* also test on python 3.10 and 3.11-dev
* remove "nonempty" default mount option, seems unsupported now.

Release 1.4.1 (2021-01-31)
==========================

* timestamp rounding tests: avoid y2038 issue in test

Release 1.4.0 (2021-01-24)
==========================

* Remove py2 and py3<3.5 support, minimum requirement is Python 3.5 now.
  If you are stuck on Python 2.x or < 3.5, use llfuse<1.4.0.
* setup.py: return rc=2 in error cases, fixes #52.
  implements same behaviour as pyfuse3 for these cases.
* Use EACCES instead of EPERM for file permission errors, fixes #36.
* Fix long-standing rounding error in file date handling when the nanosecond
  part of file dates were > 999999500, fixes #38.
* Docs: add link to pyfuse3 porting hints ticket
* Testing:

  - Add Power support (ppc64le) to travis CI.
  - Move CI to GitHub Actions, except ppc64le.
  - Test fixes for pytest 6.

Release 1.3.8 (2020-10-10)
==========================

* fix compilation on FreeBSD, #28
* export DEVELOPER_MODE=1 can be used to opt in (default is 0), #22
* twine-based pypi-uploading, Qubes OS support for GPG signing,
  see util/sdist-sign and pypi-upload

Release 1.3.7 (2020-10-04)
==========================

* Rebuild with Cython 0.29.21 for Python 3.9 compatibility.

Release 1.3.6 (2019-02-14)
==========================

* No change upload.

* Python-LLFUSE is no longer actively maintained. Unless you are stuck
  with Python 2.x or libfuse 2.x, we recommended to use the pyfuse3
  module instead.

Release 1.3.5 (2018-08-30)
==========================

* Add ``handle_signals`` option to ``llfuse.main``
* Several fixes to ``examples/passthroughfs.py``
* Now compatible with Python 3.7

Release 1.3.4 (2018-04-29)
==========================

* No-change rebuild with Cython 0.28.2.

Release 1.3.3 (2018-03-31)
==========================

* Dropped pytest-catchlog dependency and add support for Pytest 3.3.0.
* Minor bugfixes.

Release 1.3.2 (2017-11-13)
==========================

* Minor bugfixes.

Release 1.3.1 (2017-09-22)
==========================

* The main loop now terminates properly again when terminated
  by a signal.

Release 1.3 (2017-09-15)
========================

* The `StatvfsData` class now has an `!~StatvfsData.f_namemax`
  attribute.

* `EntryAttributes` and `StatvfsData` instances can now be pickled and
  copied. Other llfuse classes now raise an exception when the do not
  support pickling/copying.

Release 1.2 (2017-01-22)
========================

* The *attr* argument of the `~Operations.setattr` handler now
  contains valid values only for the attributes that are to be set.

  Previously, the documentation assorted that other attributes would
  be set to the original (unchanged) values. However, that was
  actually never the case.

Release 1.1.1 (2016-07-27)
==========================

* Fixed the description of the `~Operations.lookup` handler (should
  return zero if there is no such entry, not a negative value).
* Fixed the description of the `EntryAttributes` structure
  (descriptions of `~EntryAttributes.attr_timeout` and
  `~EntryAttributes.entry_timeout` were switched).

Release 1.1 (2016-05-23)
========================

* Added support for nanosecond resolution time-stamps in GNU/kFreeBSD.
* Fixed another set of build issues on FreeBSD.

Release 1.0 (2016-03-08)
========================

**Note:** The 1.0 version number does not imply any special
stability or an API freeze. It's a consequence of switching to
semantic versioning, where backwards incompatible changes will
always result in increased major version.

* Fixed an overflow when accessing the ``*st_xtime_ns`` attributes of
  the `~llfuse.EntryAttributes` class from Python on 32-bit systems.

* POTENTIAL COMPATIBILITY BREAK: The `~Operations.destroy` handler is now
  called with the global lock acquired.

Release 0.43 (2016-02-23)
=========================

* Fixed build-failure under OS-X.
* Fixed some build failures under FreeBSD (hopefully all of them, but
  no testers were available before the release).
* The *workers* parameter to `llfuse.main` may now be `None`.

Release 0.42.1 (2016-02-01)
===========================

* Include HTML documentation.

Release 0.42 (2016-01-30)
=========================

* The documentation has been clarified and extended - please take a
  look even if you're already familiar with Python-LLFUSE.

* Extended coverage of unit tests.

* Fixed a compile error under OS-X.

* Added `notify_store` function.

* Added `get_sup_groups` function.

* The `~Operations.read` handler may now return arbitrary objects that
  as long as they implement the buffer protocol.

* Implemented a *forget_multi* handler (used behind the scenes).

* Many classes were rewritten in Cython for improved performance.

* Python thread-specific data is now initialized only once rather
  than every time a handler is called.

* SIGINT (Ctrl-C) now properly terminates `llfuse.main` also when
  running with multiple workers.

* The return value of `llfuse.main` now indicates if the loop was
  terminated due to an unmount request or due to a signal.

BACKWARDS INCOMPATIBLE CHANGES:

* Ducktyping the `EntryAttributes` class is no longer allowed,
  `Operations` methods must return instances of this class (rather
  than any object that provides the necessary attributes).

* The `!st_Xtime` attributes of the `EntryAttributes` have been
  dropped in favor of the `!st_Xtime_ns` attributes.

* The `setattr` method now always receives a completely filled
  `EntryAttributes` instance. To determine which attributes should
  be changed, a new *fields* argument has been introduced.

* The `~Operations.setattr` method now also receives an *fh*
  parameter.

* The `llfuse.main` function now has a *workers* parameter, and
  the *single* parameter was dropped.

* Almost all request handlers now receive a `RequestContext`
  instance in an additional parameter.

Release 0.41.1 (2015-08-22)
===========================

* Added some files in :file:`test/` that were missing in the 0.41
  tarball.  Tests now support the ``--installed`` option, produce
  prettier output, and check for error messages printed to stderr or
  stdout.

* Python-LLFUSE can now also be built on NetBSD.

* Added support for FUSE_SET_ATTR_ATIME_NOW and
  FUSE_SET_ATTR_MTIME_NOW setattr flags. Previously, these would be
  silently ignored.

* Fixed an AssertionError in :file:`examples/passthroughfs.py`

Release 0.41 (2015-08-20)
=========================

* Fixed a syntax error in contrib/tmpfs.py
* Introduced an `!llfuse.__version__` attribute.
* Added more reasonable default values for `llfuse.EntryAttributes`.
* Added new minimal example, :file:`examples/lltest.py`.
* Added unit tests.
* Added an example for a pass-through file system,
  :file:`examples/passthroughfs.py`.

Release 0.40 (2013-11-26)
=========================

* Re-raising an exception from a request handler now works
  correctly under Python 3. Problems were caused by a Cython bug,
  but llfuse now works around the issue.

* File atime, ctime and mtime can now also be represented as
  nanosecond integer values for increased resolution. Thanks to
  nagy.attila@gmail.com for the initial patch.

* Python-llfuse no longer includes the setuptools/distribute
  bootstrap script. This module thus has to be installed manually
  if not already present on the system.

* Duck-typing of the Operations instance is now supported.

* Python-llfuse no longer requires a re-compilation of the
  Cython code (setup.py build_cython step) when compiling for MacOS
  or FreeBSD.

* Extended attributes are now properly supported under FreeBSD.

Release 0.39 (2013-05-11)
=========================

* When running under Python 3.x, several functions now work with
  string objects rather than byte objects:

  - llfuse.init(): the *mountpoint* argument, and the elements of
    the *fuse_opts* argument are expected to be of type str.

  - llfuse.listdir(): the *path* argument is expected to be of
    type str, and the values returned by the iterator will be of
    type str as well.

  - llfuse.getxattr(), llfuse.setxattr(): the *path* and *name*
    arguments are expected to be of type str.

  When necessary, values will be converted to the file system
  encoding using the surrogatescape handler as described in PEP 383
  (http://www.python.org/dev/peps/pep-0383/)

* Renamed get_off_t_bytes() and get_ino_t_bytes() to
  get_off_t_bits() and get_ino_t_bits() and documented them.

Release 0.38 (2013-01-05)
=========================

* Various small bugfixes.

Release 0.37.1 (2011-12-10)
===========================

* Fixed a segfault-causing race condition in Lock.acquire() and
  Lock.yield().

Release 0.37 (2011-12-05)
=========================

* Explicitly call fuse_chan_destroy() in llfuse.close(), so
  that the mountpoint becomes inaccessible right away, even
  while the process is still running.

* Added get_ino_t_bytes() and get_off_t_bytes() that return
  the number of bytes used for representing inode numbers
  and file offsets.

* The yield_() method of the global lock now takes an additional
  *count* argument that can be used to yield the lock more than
  once.

* Changed implementation of global lock. The global lock is
  no longer a mutex, but a boolean variable protected by a mutex,
  and changes are tracked with a condition object. This allows
  lock.yield() to work properly: if there are other threads waiting
  for the lock, they are guaranteed to run. If there are no other
  threads waiting for the lock, execution of the active thread
  continues immediately.

  The previous implementation using sched_yield() was mostly
  broken: threads trying to acquire the global lock were calling
  pthread_mutex_lock, so they got removed from the kernels
  runqueue. However, calls to sched_yield() would just put the
  active thread into the expired runqueue, and calls to
  pthread_mutex_unlock apparently do not synchronously move the
  threads waiting for the lock back to a runqueue. Therefore, most
  of the time the active thread would be the only thread in any
  runqueue and thus continue to run.

* The Operations.forget() method now receives a list of
  (inode, nlookup) tuples rather than just one such tuple.

* invalidate_entry() and invalidate_inode() no longer work
  synchronously. Instead, the message is put in a queue and send by
  a background thread.

* The acquire() method of the global lock now has an optional
  *timeout* parameter.

* The create() request handler now receives the open flags
  as an additional parameter.

Release 0.36 (2011-09-20)
=========================

* Don't send SIGHUP if exception is encountered in destroy()
  handler (since at that point, main loop has already terminated
  and signal handling been reset).

* Fix a problem with request handler exceptions being re-raised
  not only in llfuse.main(), but also in llfuse.close() when
  running single threaded.

Release 0.35 (2011-09-14)
=========================

* Explicitly initialize Python thread support. Previously, calling
  llfuse.main() resulted in a crash if no Python threads were used
  before the call.

* Removed handle_exc() method. If request handle raise an exception,
  the main loop now terminates and the exception is re-raised and
  passed to the caller of llfuse.main().

* llfuse.close() can now leave the mountpoint in an inaccessible
  state to signal a shutdown due to an internal file system
  error.

* The destroy() request handler is now called without the
  global lock acquired. This makes sense, because it's not called
  as part of the main loop but by llfuse.close().

Release 0.34 (2011-08-10)
=========================

* Explicitly cast S_* constants to mode_t to prevent compiler
  warnings on FreeBSD.

* Fixed initialization error under Python 3.

Release 0.33 (2011-07-03)
=========================

* Various small bugfixes.

Release 0.32 (2011-06-04)
=========================

* Fixed unlink() bug in contrib/example.py

* Include :file:`src/*.pxi` files in release tarball. Were
  accidentally omitted in previous version.

* Moved debian/ directory into separate repository.

Release 0.31 (2011-05-12)
=========================

* Use long for storing nanoseconds in file [amc]times, not int.

Release 0.30 (2011-03-08)
=========================

* Fixed compile errors with Python 3.0 and 3.1.
* Fixed error handling, errno is now read correctly.
* Documentation is now shipped in tarball rather than generated
  during installation.

Release 0.29 (2010-12-30)
=========================

* Initial release
