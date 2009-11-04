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

##### BEGIN: Global constants #####

# Threshold that triggers directory synchronization when surpassed
MAX_CHANGES = 1000

# Timer threshold (seconds) that triggers directory synchronization when the
# countdown expires
TIMER_LIMIT = 60

# Events to monitor
MONITOR_EV = pyinotify.EventsCodes.ALL_FLAGS['IN_CREATE']      | \
             pyinotify.EventsCodes.ALL_FLAGS['IN_DELETE']      | \
             pyinotify.EventsCodes.ALL_FLAGS['IN_ATTRIB']      | \
             pyinotify.EventsCodes.ALL_FLAGS['IN_MOVE_SELF']   | \
             pyinotify.EventsCodes.ALL_FLAGS['IN_MOVED_FROM']  | \
             pyinotify.EventsCodes.ALL_FLAGS['IN_MOVED_TO']    | \
             pyinotify.EventsCodes.ALL_FLAGS['IN_CLOSE_WRITE']

# Rsync settings
RSYNC_PATH = "/usr/bin/rsync"
RSYNC_OPTIONS           = '-Rd -HpltogD --delete'
RSYNC_OPTIONS_RECURSIVE = '-Rr -HpltogD --delete'
# Warning: The first two options are mandatory for correct behaviour!

# Assemble remote paths relative to the local virtual root specified by
# this marker within in the source path definition. Note that this marker
# should always start and end with a path separator!
VIRTUAL_ROOT_MARKER     = '%s.%s' % (os.path.sep, os.path.sep)

##### END:   Global constants #####

##### BEGIN: Global variables #####

_watchmanager = None
_notifier = None
_nworkers = 0
_queues = []
_monitoring = None
_dryrun = False

##### END:   Global variables #####

##### BEGIN: Logging #####

log = logging.getLogger('pylsyncd')

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
  def __init__(self, source, s):
    self.source = source
    self.queue = ItemQueue()
    self.remote = False
    self.name = None
    self.path = s
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
      if not s.endswith('/'):
        suffix = '/'

    self._path = s if suffix is None else s + suffix

  def synchronize(self):
    if not len(self.queue):
      return True

    log.debug('%s - Processing %d items' % (self.name, len(self.queue)))

    if self.source.vroot is None:
      self.queue.filter(lambda x: not _rsync(x.path + os.path.sep,
        self.path, recursive=x.recursive))
    else:
      # Rewrite source paths on the fly to include the virtual root marker
      self.queue.filter(lambda x: not _rsync(x.vpath(self.source.vroot)
        + os.path.sep, self.path, recursive=x.recursive))

    if len(self.queue):
      log.error('%s - Error synchronizing %d items.'
          % (self.name, len(self.queue)))
      return False
    return True

class Item(object):
  def __init__(self, path, recursive=False):
    self.path = path
    self.recursive = recursive

  def __repr__(self):
    return '<Item path=%s recursive=%s>' % (self.path, self.recursive)

  def vpath(self, vroot):
    # Return our path including the virtual root marker
    return self.path.replace(vroot, vroot + VIRTUAL_ROOT_MARKER[:-1], 1)

class ItemQueue(object):
  def __init__(self):
    self.items = []

  def __repr__(self):
    return '<ItemQueue numitems=%d>' % len(self.items)

  def __len__(self):
    return len(self.items)

  def add(self, item):
    for idx, i in enumerate(self.items):
      if i.path == item.path:
        if item.recursive and not i.recursive:
          # Prefer the recursive item
          self.items[idx] = item
        return
    self.items.append(item)

  def optimize(self):
    numitems = len(self.items)
    if numitems < 2:
      return
    log.debug('Optimizing %d items' % numitems)

    # Least specific path first
    self.items.sort(lambda x,y: len(x.path) - len(y.path))

    # Find items with the recursive flag and get rid of all queued subdirs
    for parent in filter(lambda x: x.recursive, self.items):
      self.items = filter(lambda x: not _is_subdir(parent.path, x.path),
          self.items)

    # Get rid of deleted items
    self.items = filter(lambda x: os.path.exists(x.path), self.items)

    log.debug('Optimizing %d items is complete. Remaining items: %d'
      % (numitems, len(self.items)))

  def filter(self, function):
    self.items = filter(function, self.items)

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
  global _queues

  def process_IN_CREATE(self, event):
    # Queue the new directory itself, recursively
    if event.dir:
      for q in _queues:
        q.put(Item(os.path.normpath(os.path.join(event.path, event.name)),
          recursive=True))
    else:
      self.process_default(event)

  def process_IN_MOVED_TO(self, event):
    # Queue the renamed directory itself, recursively
    if event.dir:
      for q in _queues:
        q.put(Item(os.path.normpath(os.path.join(event.path, event.name)),
          recursive=True))
    else:
      self.process_default(event)

  def process_default(self, event):
    for q in _queues:
      q.put(Item(event.path))

##### END:   Class definitions #####

##### BEGIN: Helper Functions #####

def _execute(command, args):
  global _dryrun
  log.info('Executing: %s %s' % (command, ' '.join(args)))
  return True if _dryrun else subprocess.call([command] + args) == 0

def _rsync(source, destination, recursive=False):
  if source == None or destination == None:
    assert False, "Both source and destination must be provided"
  opts = RSYNC_OPTIONS_RECURSIVE if recursive else RSYNC_OPTIONS
  return _execute(RSYNC_PATH, opts.split() + [source, destination])

def _is_subdir(parent, dir):
  path_parent = os.path.abspath(parent)
  path_dir = os.path.abspath(dir)
  if len(path_dir) > len(path_parent) and path_dir.startswith(path_parent):
    return True
  return False

##### END:   Helper Functions #####

##### BEGIN: Functions #####

def init(source, destinations, dryrun=False):
  global _dryrun
  global _monitoring
  global _notifier
  global _nworkers
  global _queues
  global _watchmanager

  if dryrun:
    _dryrun = True
    log.warning('DRY-RUN requested, not executing any external commands!')
  else:
    check_dependencies()

  source = Source(source)
  destinations = [Destination(source, i) for i in destinations]

  log.info('Aggregating changes within: %ds' % TIMER_LIMIT)
  log.info('Number of changes forcing synchronization: %d' % MAX_CHANGES)

  _nworkers = len(destinations)
  log.info('Number of additional threads: %s' % _nworkers)

  _monitoring = threading.Event()
  _watchmanager = pyinotify.WatchManager()
  _notifier = pyinotify.Notifier(_watchmanager, ProcessEvent())

  for destination in destinations:
    q = Queue.Queue(0) # infinite size
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

  log.info('Initializing monitor for path: %s' % source.path)
  _watchmanager.add_watch(source.path, MONITOR_EV, rec=True, auto_add=True)
  _monitoring.set()

def loop(*args, **kwargs):
  global _notifier
  return _notifier.loop(*args, **kwargs)

def worker(q, source, destination):
  global _monitoring

  # Wait until all paths are watched by inotify
  _monitoring.wait()

  log.info('%s - Starting initial sync...' % destination.name)
  destination.queue.add(Item(source.path, recursive=True))
  if destination.synchronize():
    log.info('%s - Initial sync complete.' % destination.name)
  else:
    log.error('%s - Initial sync failed. Removing destination!'
        % destination.name)
    return

  timer = Timer()
  timer.start(TIMER_LIMIT)
  while True:
    try:
      log.debug('%s - Remaining %f (%d items)' %
        (destination.name, timer.remaining(), len(destination.queue)))
      item = q.get(block=True, timeout=timer.remaining())
      destination.queue.add(item)
    except Queue.Empty:
      destination.queue.optimize()
      destination.synchronize()
      timer.reset()
      continue
    if len(destination.queue) >= MAX_CHANGES:
      log.debug('%s - MAX_CHANGES=%d reached, optimizing queue...'
          % (destination.name, MAX_CHANGES))
      destination.queue.optimize()
      if len(destination.queue) >= MAX_CHANGES:
        log.warning('%s - MAX_CHANGES=%d reached, processing items now...'
            % (destination.name, MAX_CHANGES))
        destination.synchronize()
        timer.reset()

##### END:   Functions #####

