# -*- coding: utf8 -*-

__author__ = 'sergey'

def entry_attributes_to_dict(attr):
    """
    @param attr: Attributes object
    @type  attr: llfuse.EntryAttributes

    @return: dict
    @rtype: dict
    """
    return {
        'attr_timeout': attr.attr_timeout,
        'entry_timeout': attr.entry_timeout,
        'generation': attr.generation,
        'st_atime_ns': attr.st_atime_ns,
        'st_birthtime_ns': attr.st_birthtime_ns,
        'st_blksize': attr.st_blksize,
        'st_blocks': attr.st_blocks,
        'st_ctime_ns': attr.st_ctime_ns,
        'st_gid': attr.st_gid,
        'st_ino': attr.st_ino,
        'st_mode': attr.st_mode,
        'st_mtime_ns': attr.st_mtime_ns,
        'st_nlink': attr.st_nlink,
        'st_rdev': attr.st_rdev,
        'st_size': attr.st_size,
        'st_uid': attr.st_uid
    }

def setattr_fields_to_dict(fields):
    """
    @param fields: SetattrFields object
    @type  fields: llfuse.SetattrFields

    @return: dict
    @rtype: dict
    """
    return {
        'update_atime': fields.update_atime,
        'update_gid': fields.update_gid,
        'update_mode': fields.update_mode,
        'update_mtime': fields.update_mtime,
        'update_size': fields.update_size,
        'update_uid': fields.update_uid,
    }
