# -*- coding: utf-8 -*-

# (C) Copyright 2009 Ioannis Aslanidis
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
pyinotify.log.setLevel(0)

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
# rsync for directory content
RSYNC_PATH = "/usr/bin/rsync"
RSYNC_OPTIONS           = '-HpltogDd --delete'
RSYNC_OPTIONS_RECURSIVE = '-HpltogDr --delete'

##### END:   Global constants #####

##### BEGIN: Global variables #####

# Watch Monitor
wm = pyinotify.WatchManager()
# Number of workers (destination file servers)
num_worker_threads = 0
# Queues of the worker threads where the modified directories go
queues = []

##### END:   Global variables #####

##### BEGIN: Class definitions #####

# Server definition
class Server(object):
  def __init__(self, s):
    assert len(s)
    self.name = self.path = s

  def __repr__(self):
    return '<Server name=%s path=%s>' % (self.name, self.path)

  def __str__(self):
    return self.name

  def __parse_server_name(self, s):
    # [USER@]HOST::DEST --> HOST
    if '::' in s.split('/', 1)[0]:
      return s.split('@', 1)[-1].split('::', 1)[0]

    # rsync://[USER@]HOST[:PORT]/DEST --> HOST
    if s.startswith('rsync://'):
      return s.split('rsync://', 1)[1].split('@', 1)[-1].split(':', 1)[0].split('/', 1)[0]

    # [USER@]HOST:DEST --> HOST
    return s.split('@', 1)[-1].split(':', 1)[0]

  def __parse_server_path(self, s):
    # rsync://[USER@]HOST[:PORT]/DEST
    # [USER@]HOST::DEST
    hostpart = s.split('/', 1)[0]
    if s.startswith('rsync://') or '::' in hostpart or ':' in hostpart:
      return s if s.endswith('/') else s + '/'

    # Assume [USER@]HOST without :DEST
    return s + ':'

  @property
  def name(self):
    return self._name

  @name.setter
  def name(self, s):
    self._name = self.__parse_server_name(s)

  @property
  def path(self):
    return self._path

  @path.setter
  def path(self, s):
    self._path = self.__parse_server_path(s)

  def synchronize(self, src, dst=None, recursive=False):
    return rsync(src, self.path if dst is None else self.path + dst, recursive)

class Item(object):
  def __init__(self, path, recursive=False):
    self.path = path
    self.recursive = recursive

  def __repr__(self):
    return '<Item path=%s recursive=%s>' % (self.path, self.recursive)

class ItemQueue(object):
  def __init__(self, server):
    self.items = []
    self.server = server

  def __repr__(self):
    return '<ItemQueue server=%s numitems=%d>' % (self.server, len(self.items))

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
    logging.debug('%s - Optimizing %d items' % (self.server, numitems))

    # Least specific path first
    self.items.sort(lambda x,y: len(x.path) - len(y.path))

    # Find items with the recursive flag and get rid of all queued subtrees
    for parent in filter(lambda x: x.recursive, self.items):
      self.items = filter(lambda x: not is_subdir(parent.path, x.path), self.items)

    # Get rid of deleted items
    self.items = filter(lambda x: os.path.exists(x.path), self.items)

    logging.debug('%s - Optimizing %d items is complete. Remaining items: %d'
      % (self.server, numitems, len(self.items)))

  def process(self):
    if not len(self):
      return
    self.optimize()

    logging.debug('%s - Processing %d items' % (self.server, len(self)))
    self.items = filter(lambda x: not self.server.synchronize(x.path + '/',
        x.path, recursive=x.recursive), self.items)

    if len(self.items):
      logging.error('%s - Error synchronizing %d items. Trying on next run...'
          % (self.server, len(self.items)))

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
class PEvent(pyinotify.ProcessEvent):

  def process_IN_CREATE(self, event):
    # Queue the new directory itself, recursively
    if event.dir:
      for q in queues:
        q.put(Item(os.path.normpath(os.path.join(event.path, event.name)),
          recursive=True))
    else:
      self.process_default(event)

  def process_IN_MOVED_TO(self, event):
    # Queue the renamed directory itself, recursively
    if event.dir:
      for q in queues:
        q.put(Item(os.path.normpath(os.path.join(event.path, event.name)),
          recursive=True))
    else:
      self.process_default(event)

  def process_default(self, event):
    for q in queues:
      q.put(Item(event.path))

##### END:   Class definitions #####

##### BEGIN: Functions #####

# Execution wrapper
def execute(cmd, args):
  if not os.access(cmd, os.X_OK):
    logging.error('Unable to execute: %s' % cmd)
    return False
  logging.debug('Executing: %s %s' % (cmd, ' '.join(args)))
  return subprocess.call([cmd] + args) == 0

# Rsync wrapper
def rsync(src, dst, recursive=False):
  if src == None or dst == None:
    assert False, "Both src and dst must be provided"
  opts = RSYNC_OPTIONS_RECURSIVE if recursive else RSYNC_OPTIONS
  return execute(RSYNC_PATH, opts.split() + [src, dst])

# Function that checks if a given dir is a subdir of given parent dir
def is_subdir(parent, dir):
  path_parent = os.path.abspath(parent)
  path_dir = os.path.abspath(dir)
  if len(path_dir) > len(path_parent) and path_dir.startswith(path_parent):
    return True
  return False

##### END:   Functions #####

##### BEGIN: Worker Synchronization Threads #####

def worker(monitor, q, path, server):
  # Wait until all paths are watched by inotify
  monitor.wait()

  logging.info('%s - Starting initial sync' % server)
  if not server.synchronize(path, recursive=True):
    logging.error('%s - Initial sync failed. Removing server!' % server)
    return

  items, timer = ItemQueue(server), Timer()
  timer.start(TIMER_LIMIT)
  while True:
    try:
      logging.debug('%s - Remaining %f (%d items)' %
        (server, timer.remaining(), len(items)))
      item = q.get(block=True, timeout=timer.remaining())
      items.add(item)
    except Queue.Empty:
      items.process()
      timer.reset()
      continue
    if len(items) >= MAX_CHANGES:
      logging.info('%s - MAX_CHANGES=%d reached, processing items now...'
          % (server, MAX_CHANGES))
      items.process()
      timer.reset()

##### END:   Worker Synchronization Threads #####

##### BEGIN: Monitor #####

# Function that adds the inotify event handlers to the directories and defines
# the event processor
def Monitor(monitor, path):
  # Set up the inotify handler watcher
  notifier = pyinotify.Notifier(wm, PEvent())
  wm.add_watch(path, MONITOR_EV, rec=True, auto_add=True)
  logging.info('Monitor initialized!')
  monitor.set()
  notifier.loop()

##### END:   Monitor #####

