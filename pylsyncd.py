# -*- coding: utf-8 -*-

# Copyright (C) 2009 Ioannis Aslanidis <deathwing00@deathwing00.org>
# Copyright (C) 2009 John Feuerstein <john@feurix.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.

import Queue
import logging
import os
import random
import subprocess
import sys
import threading
import time

import pyinotify

def __check_pyinotify_version():
  from pkg_resources import parse_version
  current = pyinotify.__version__
  require = '0.8.8'
  if parse_version(current) < parse_version(require):
    raise ImportWarning(
        'pyinotify version %s is not supported, need at least version %s'
        % (current, require))
__check_pyinotify_version()

##### BEGIN: Global constants #####

# Timer threshold (seconds) that triggers synchronization. All changes within
# this timeframe are aggregated, then optimized and finally synchronized.
TIMER_LIMIT = 60

# Max number of changes in a destination queue forcing queue optimization.
MAX_CHANGES = 1000

# Max number of remaining changes in a destination queue (after optimization)
# forcing synchronization.
MAX_CHANGES_SYNC = 100

# The max length of each worker's item queue (before optimization!). This
# should never be reached in your workload and will block the pyinotify
# event monitor. Avoid hitting this limit at all costs and adjust accordingly,
# taking the maximal memory footprint into consideration.
MAX_QUEUE_LEN = 100000

# The amount of time (in seconds) to delay synchronization of a destination
# which failed to synchronize earlier. Note that the actual delay will be
# failcount * TIME_SLEEP_FAILURE seconds until MAX_SYNC_FAILURES is reached.
TIME_SLEEP_FAILURE = 60

# The max amount of sequential synchronization failures of a destination that
# will cause the destination to be dropped.
MAX_SYNC_FAILURES = 5

# Note: The maximum sleep time with the default settings is:
# sum(map(lambda x: x*TIME_SLEEP_FAILURE, xrange(1,MAX_SYNC_FAILURES+1)))
#  == 900 seconds (15 minutes)

# Events to monitor
MONITOR_EV = pyinotify.EventsCodes.ALL_FLAGS['IN_CREATE']      | \
             pyinotify.EventsCodes.ALL_FLAGS['IN_DELETE']      | \
             pyinotify.EventsCodes.ALL_FLAGS['IN_ATTRIB']      | \
             pyinotify.EventsCodes.ALL_FLAGS['IN_MOVE_SELF']   | \
             pyinotify.EventsCodes.ALL_FLAGS['IN_MOVED_FROM']  | \
             pyinotify.EventsCodes.ALL_FLAGS['IN_MOVED_TO']    | \
             pyinotify.EventsCodes.ALL_FLAGS['IN_CLOSE_WRITE'] | \
             pyinotify.EventsCodes.ALL_FLAGS['IN_ONLYDIR']     | \
             pyinotify.EventsCodes.ALL_FLAGS['IN_DONT_FOLLOW']

# Rsync settings
RSYNC_PATH              = '/usr/bin/rsync'
RSYNC_OPTIONS           = '-Rd --files-from=- --from0 ' + '-HpltogD --delete'
RSYNC_OPTIONS_RECURSIVE = '-Rr --files-from=- --from0 ' + '-HpltogD --delete'
# Note: The first string of options is mandatory for correct behaviour!

# We use null ('\0') delimited paths in rsync's --files-from
# list to support paths containing newlines.
RSYNC_PATH_DELIMITER = '\0'

# Assemble remote paths relative to the local virtual root specified by
# this marker within in the source path definition. Note that this marker
# should always start and end with a path separator!
VIRTUAL_ROOT_MARKER     = '%s.%s' % (os.path.sep, os.path.sep)

##### END:   Global constants #####

##### BEGIN: Global variables #####

source = None
destinations = []

_watchmanager = None
_notifier = None
_nworkers = 0
_queues = []
_monitoring = None
_dry_run = False

##### END:   Global variables #####

##### BEGIN: Logging #####

class _LogHandler(logging.Handler):
  def emit(self, record):
    pass

log = logging.getLogger('pylsyncd')
log.addHandler(_LogHandler())

##### END:   Logging #####

##### BEGIN: Class definitions #####

class Source(object):
  def __init__(self, s):
    self.path = os.path.abspath(s)

    if os.path.normpath(s) == os.path.curdir:
      self.vroot = os.path.abspath(s)
    elif VIRTUAL_ROOT_MARKER in s:
      self.vroot = os.path.abspath(s.split(VIRTUAL_ROOT_MARKER, 1)[0])
    else:
      self.vroot = None

    log.info('Registered source: %s' % self)

  def __repr__(self):
    return '<Source path=%s vroot=%s>' % (self.path, self.vroot)

class Destination(object):
  def __init__(self, source, path, initial_sync=False):
    self.initial_sync = initial_sync
    self.source = source
    self.queue = ItemQueue()
    self.remote = False
    self.name = None
    self.path = path
    log.info('Registered destination: %s' % self)

  def __repr__(self):
    return '<Destination path=%s remote=%s name=%s source=%s queue=%s>' \
      % (self.path, self.remote, self.name, self.source, self.queue)

  @property
  def path(self):
    return self._path

  @path.setter
  def path(self, s):
    # Prepare path specification, set the remote flag and
    # the destination shortname

    # Rsync destination path modifications:
    #                DEST              ->   DEST/
    # rsync://[USER@]HOST[:PORT]/DEST  ->   rsync://[USER@]HOST[:PORT]/DEST/
    #         [USER@]HOST::DEST        ->   [USER@]HOST::DEST/
    #         [USER@]HOST:             ->   [USER@]HOST:
    #         [USER@]HOST:DEST         ->   [USER@]HOST:DEST/
    suffix = None
    if s.startswith('rsync://'):
      self.remote = True
      server = s.split('rsync://', 1)[-1]   # strip protocol
      server = server.split('/', 1)[0]      # strip path
      server = server.split('@', 1)[-1]     # strip username
      server = server.split(':', 1)[0]      # strip port/module
      self.name = server
      if not s.endswith('/'):
        suffix = '/'
    elif '::' in s:
      self.remote = True
      server = s.split('::', 1)[0]          # strip path
      server = server.split('@', 1)[-1]     # strip username
      server = server.split(':', 1)[0]      # strip port
      self.name = server
      if not s.endswith('/'):
        suffix = '/'
    elif ':' in s:
      self.remote = True
      server = s.split(':', 1)[0]           # strip path
      server = server.split('@', 1)[-1]     # strip username
      server = server.split(':', 1)[0]      # strip port
      self.name = server
      if not s.endswith(':') and not s.endswith('/'):
        suffix = '/'
    else:
      self.remote = False
      # The local shortname in the logs is the last dirname:
      #     /foo/bar/baz/ --> baz
      self.name = os.path.normpath(s).rsplit('/', 1)[-1]
      suffix = '/'

    self._path = s if self.remote else os.path.abspath(s)
    self._path = self._path if suffix is None else self._path + suffix

  def synchronize(self):
    if not len(self.queue):
      return True

    log.debug('[%s] Processing %d items' % (self.name, len(self.queue)))

    vroot = os.path.sep if self.source.vroot is None else self.source.vroot

    if self.__synchronize(self.queue.trees, vroot, recursive=True):
      self.queue.empty_trees()

    if self.__synchronize(self.queue.dirs, vroot, recursive=False):
      self.queue.empty_dirs()

    if len(self.queue):
      log.error('[%s] Error synchronizing %d items.'
          % (self.name, len(self.queue)))
      return False
    return True

  def __synchronize(self, items, vroot, recursive=False):
    if not len(items):
      return True
    if _rsync(vroot, self.path, _generate_relative_path_list(items, vroot),
            recursive=recursive):
      log.info('Synchronization of %d %s items finished successfully'
          % (len(items), 'recursive' if recursive else 'non-recursive'))
      return True
    return False

class Item(object):
  def __init__(self, path, recursive=False):
    self.path = path
    self.recursive = recursive

  def __repr__(self):
    return '<Item path=%s recursive=%s>' % (self.path, self.recursive)

class ItemQueue(object):
  def __init__(self):
    self.dirs = []   # non-recursive
    self.trees = []  # recursive

  def __repr__(self):
    return '<ItemQueue numitems=%d dirs=%d trees=%d>' \
        % (len(self), len(self.dirs), len(self.trees))

  def __len__(self):
    return len(self.dirs) + len(self.trees)

  def add(self, item):
    # Drop obvious duplicates as soon as possible and
    # prefer the recursive item over the non-recursive.
    if item.recursive:
      if not item.path in self.trees:
        self.trees.append(item.path)
    else:
      if not item.path in self.dirs and not item.path in self.trees:
        self.dirs.append(item.path)

  def optimize(self):
    numitems = len(self)
    if not numitems:
      return
    log.debug('Optimizing ItemQueue with %d items...' % numitems)
    log.debug('Before: %s' % self)

    if len(self.trees):
      # Sort the least specific tree path first to get rid of as many
      # subtrees and subdirs as soon as possible. This way we shorten
      # subsequent filter iterations over self.trees.
      self.trees.sort(lambda x,y: len(x) - len(y))

      # Get rid of subtrees
      for tree in self.trees:
        self.trees = filter(lambda x: not _is_subdir(tree, x), self.trees)

      if len(self.dirs):
        # Get rid of all subdirs
        for tree in self.trees:
          self.dirs = filter(lambda x: not _is_subdir(tree, x), self.dirs)

    # Get rid of deleted items
    self.dirs = filter(lambda x: os.path.exists(x), self.dirs)
    self.trees = filter(lambda x: os.path.exists(x), self.trees)

    log.debug('After: %s' % self)
    log.debug('Optimizing %d items is complete. Remaining items: %d'
      % (numitems, len(self)))

  def empty_dirs(self):
    self.dirs = []

  def empty_trees(self):
    self.trees = []

# Simple timer implementation
class Timer:
  def __init__(self):
    self.running = False
  def start(self, limit):
    assert not self.running
    self.start_time = time.time()
    self.time_limit = limit
    self.running = True
  def stop(self):
    assert self.running
    self.running = False
  def is_running(self):
    return self.running
  def reset(self):
    assert self.running
    self.start_time = time.time()
  def remaining(self):
    assert self.running
    now = time.time()
    diff = self.start_time + self.time_limit - now
    return max(0, diff)

# This class inherits from ProcessEvents and overloads the interesting
# methods
class ProcessEvent(pyinotify.ProcessEvent):

  def process_IN_CREATE(self, event):
    # Queue the new directory itself, recursively
    if event.dir:
      queue_item(Item(os.path.normpath(os.path.join(event.path, event.name)),
        recursive=True))
    else:
      self.process_default(event)

  def process_IN_MOVED_TO(self, event):
    # Queue the renamed directory itself, recursively
    if event.dir:
      queue_item(Item(os.path.normpath(os.path.join(event.path, event.name)),
        recursive=True))
    else:
      self.process_default(event)

  def process_default(self, event):
    # Queue the path containing the event, non-recursively
    queue_item(Item(event.path))

##### END:   Class definitions #####

##### BEGIN: Helper Functions #####

def _rsync(source, destination, iterable, recursive=False):
  global _dry_run
  assert source is not None
  assert destination is not None

  opts = RSYNC_OPTIONS_RECURSIVE if recursive else RSYNC_OPTIONS
  cmd  = [RSYNC_PATH] + opts.split() + [source, destination]

  log.info('Executing: %s' % cmd)
  if _dry_run:
    return True

  rsync = subprocess.Popen(cmd, stdin=subprocess.PIPE)
  with rsync.stdin as list:
    for i in iterable:
      log.debug('rsync src: %s' % i)
      list.write(i + os.path.sep + RSYNC_PATH_DELIMITER)
    list.close()
  return rsync.wait() == 0

def _is_subdir(parent, dir):
  path_parent = os.path.abspath(parent) + os.path.sep
  path_dir = os.path.abspath(dir)
  if len(path_dir) > len(path_parent) and path_dir.startswith(path_parent):
    return True
  return False

def _generate_relative_path_list(paths, vroot):
  if vroot == os.path.sep:
    for path in paths:
      yield path
  else:
    for path in paths:
      yield os.path.relpath(path, start=vroot)

##### END:   Helper Functions #####

##### BEGIN: Functions #####

def init(source_path, destination_paths, dry_run=False, initial_sync=False):
  global _dry_run
  global _monitoring
  global _notifier
  global _nworkers
  global _queues
  global _watchmanager
  global source
  global destinations

  if dry_run:
    _dry_run = True
    log.warning('DRY-RUN requested, not executing any external commands!')
  else:
    check_dependencies()

  source = Source(source_path)
  destinations = [Destination(source, path, initial_sync)
      for path in destination_paths]

  log.info('Aggregating changes within: %ds' % TIMER_LIMIT)
  log.info('Number of changes forcing queue optimization: %d' % MAX_CHANGES)
  log.info('Number of remaining changes forcing synchronization: %d' % MAX_CHANGES_SYNC)

  _nworkers = len(destinations)
  log.warning('Total number of additional threads: %s' % _nworkers)

  _monitoring = threading.Event()
  _watchmanager = pyinotify.WatchManager()
  _notifier = pyinotify.Notifier(_watchmanager, ProcessEvent())

  if initial_sync:
    log.warning('Initial sync requested, this might take a while')

  for destination in destinations:
    q = Queue.Queue(MAX_QUEUE_LEN)
    _queues.append(q)
    t = threading.Thread(target=worker, args=(q, source, destination))
    t.setDaemon(True)
    t.start()

  monitor(source)

def check_dependencies():
  if not os.access(RSYNC_PATH, os.X_OK):
    log.fatal('Rsync not found or not executable: %s' % RSYNC_PATH)
    raise Exception

def monitor(source):
  global _watchmanager
  global _monitoring

  assert not _monitoring.is_set()

  log.warning('Initializing monitor for path: %s' % source.path)
  _watchmanager.add_watch(source.path, MONITOR_EV, rec=True, auto_add=True)
  log.warning('Initialization complete, monitoring changes...')
  _monitoring.set()

def loop(*args, **kwargs):
  global _notifier
  return _notifier.loop(*args, **kwargs)

def worker(q, source, destination):
  global _monitoring

  # Wait until all paths are watched by inotify
  _monitoring.wait()

  if destination.initial_sync:
    destination.queue.add(Item(source.path, recursive=True))
    log.warning('[%s] Starting initial sync...' % destination.name)
    if destination.synchronize():
      log.warning('[%s] Initial sync complete.' % destination.name)
    else:
      log.error('[%s] Initial sync failed, dropping destination!'
          % destination.name)
      return # FIXME

  timer = Timer()
  timer.start(TIMER_LIMIT)

  failcount = 0 # count sequential synchronization failures

  while True:
    if failcount >= MAX_SYNC_FAILURES:
      log.error('[%s] MAX_SYNC_FAILURES=%d reached, dropping destination!'
          % (destination.name, MAX_SYNC_FAILURES))
      return # FIXME
    elif failcount:
      sleeptime = failcount * TIME_SLEEP_FAILURE
      log.warning('[%s] Destination failed to synchronize (failcount=%d).'
          % (destination.name, failcount)
          + ' Sleeping %d seconds before the next synchronization'
          % sleeptime
          + ' (%d/%d allowed failures).'
          % (failcount, MAX_SYNC_FAILURES))
      time.sleep(sleeptime)
      failcount = 0 if destination.synchronize() else failcount + 1
      continue

    try:
      log.debug('[%s] Remaining %f (%d items)' %
        (destination.name, timer.remaining(), len(destination.queue)))
      item = q.get(block=True, timeout=timer.remaining())
      destination.queue.add(item)
    except Queue.Empty:
      if len(destination.queue):
        destination.queue.optimize()
        failcount = 0 if destination.synchronize() else 1
      timer.reset()
      continue

    if len(destination.queue) >= MAX_CHANGES:
      log.debug('[%s] MAX_CHANGES=%d reached, optimizing queue...'
          % (destination.name, MAX_CHANGES))
      destination.queue.optimize()
      if len(destination.queue) >= MAX_CHANGES_SYNC:
        log.warning('[%s] MAX_CHANGES_SYNC=%d reached, processing items now!'
            % (destination.name, MAX_CHANGES_SYNC))
        destination.queue.optimize()
        failcount = 0 if destination.synchronize() else 1
        timer.reset()

def queue_full_sync():
  global _queues
  global source
  assert source is not None
  log.warning('Adding full recursive synchronization item to queue...')
  queue_item(Item(source.path, recursive=True))

def queue_item(item):
  global _queues
  for q in _queues:
    q.put(item)

##### END:   Functions #####

