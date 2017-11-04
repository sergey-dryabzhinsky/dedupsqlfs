# -*- coding: utf8 -*-

from math import floor

def format_timespan(seconds): # {{{1
  """
  Format a timespan in seconds as a human-readable string.
  """
  result = []
  units = [('day', 60 * 60 * 24), ('hour', 60 * 60), ('minute', 60), ('second', 1)]
  for name, size in units:
    if seconds >= size:
      count = seconds / size
      seconds %= size
      result.append('%i %s%s' % (count, name, floor(count) != 1 and 's' or ''))
  if result == []:
    return 'less than a second'
  if len(result) == 1:
    return result[0]
  else:
    return ', '.join(result[:-1]) + ' and ' + result[-1]

def format_size(nbytes, si=False):
  """
  Format a byte count as a human-readable file size.

  @param si: In SI format (/1000) or Binary (/1024)
  """
  if si:
    return nbytes < 1000 and '%i bytes' % nbytes \
           or nbytes < (1000 ** 2) and __round(nbytes, 1000, 'kB') \
           or nbytes < (1000 ** 3) and __round(nbytes, 1000 ** 2, 'MB') \
           or nbytes < (1000 ** 4) and __round(nbytes, 1000 ** 3, 'GB') \
           or __round(nbytes, 1000 ** 4, 'TB')

  return nbytes < 1024 and '%i bytes' % nbytes \
      or nbytes < (1024 ** 2) and __round(nbytes, 1024, 'KiB') \
      or nbytes < (1024 ** 3) and __round(nbytes, 1024 ** 2, 'MiB') \
      or nbytes < (1024 ** 4) and __round(nbytes, 1024 ** 3, 'GiB') \
      or __round(nbytes, 1024 ** 4, 'TiB')

def __round(nbytes, divisor, suffix):
  _nbytes = float(nbytes) / divisor
  if floor(_nbytes) == _nbytes:
    return str(int(_nbytes)) + ' ' + suffix
  else:
    return '%.2f %s' % (_nbytes, suffix)

# vim: sw=2 sw=2 et
