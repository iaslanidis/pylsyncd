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

##### BEGIN: Global constants #####

# Threshold that triggers directory synchronization when surpassed
MAX_CHANGES = 10
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
RSYNC_OPTIONS       = '-HpltogDd --delete --numeric-ids'
RSYNC_OPTIONS_INIT  = '-HpltogDr --delete --numeric-ids --human-readable --stats' # Initial run (recursive)

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
    self._target = s

  def __str__(self):
    return self.name

  @property
  def name(self):
    s = self._target

    # [USER@]HOST::DEST --> HOST
    if '::' in s.split('/', 1)[0]:
      return s.split('@', 1)[-1].split('::', 1)[0]

    # rsync://[USER@]HOST[:PORT]/DEST --> HOST
    if s.startswith('rsync://'):
      return s.split('rsync://', 1)[1].split('@', 1)[-1].split(':', 1)[0].split('/', 1)[0]

    # [USER@]HOST:DEST --> HOST
    return s.split('@', 1)[-1].split(':', 1)[0]

  @property
  def path(self):
    s = self._target

    # rsync://[USER@]HOST[:PORT]/DEST
    # [USER@]HOST::DEST
    hostpart = s.split('/', 1)[0]
    if s.startswith('rsync://') or '::' in hostpart or ':' in hostpart:
      if s.endswith('/'):
        return s
      else:
        return s + '/'

    # Assume [USER@]HOST without :DEST
    return s + ':'

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

  def process_default(self, event):
    for q in queues:
      q.put(event.path)
    logging.debug('pyinotify:%s' % event)

##### END:   Class definitions #####

##### BEGIN: Functions #####

# Function that synchronizes non-recursively a directory and its contents
def synchronize(src, dst, opts=None):
  if src == None or dst == None:
    assert False, "Both src and dst must be provided"
  if os.access(RSYNC_PATH, os.X_OK):
    cmd = ' '.join([RSYNC_PATH, opts, src, dst])
    logging.debug('Executing: %s' % cmd)
    return subprocess.call(cmd, shell=True) == 0
  return False

def optimize(items, server):
  numitems = len(items)
  logging.debug('%s - Optimizing %d items' % (server, numitems))
  items = sorted(items) # least specific path first
  items = filter(lambda x: os.path.exists(x), items)
  logging.debug('%s - Optimizing %d items is complete. Remaining items: %d'
      % (server, numitems, len(items)))
  return items

def process(items, server):
  items = optimize(items, server)

  logging.info('%s - Processing %d items' % (server, len(items)))
  items = filter(lambda x: not synchronize(x + '/', server.path + x,
      opts=RSYNC_OPTIONS), items)

  if len(items):
    logging.error('%s - Error synchronizing %d items. Keeping for next run...'
        % (server, len(items)))

  return items

# Function that generates a recursive list of subdirectories given the parent
def GenerateRecursiveList(path):
    dirlist = [subdir[0] for subdir in os.walk(path)]
    return dirlist

##### END:   Functions #####

##### BEGIN: Worker Synchronization Threads #####

def worker(monitor, q, path, server):
  # Wait until all paths are watched by inotify
  monitor.wait()

  logging.info('%s - Starting initial sync' % server)
  if not synchronize(path, server.path, opts=RSYNC_OPTIONS_INIT):
    logging.error('%s - Initial sync failed. Removing server!' % server)
    return

  items, timer = [], Timer()
  timer.start(TIMER_LIMIT)
  while True:
    try:
      logging.debug('%s - Remaining %f (%d items)' %
        (server, timer.remaining(), len(items)))
      item = q.get(block=True, timeout=timer.remaining())
      if item not in items:
        items.append(item)
    except Queue.Empty:
      if len(items):
        items = process(items, server)
      timer.reset()
      continue
    if len(items) >= MAX_CHANGES:
      logging.info('%s - MAX_CHANGES=%d reached, processing items now...'
          % (server, MAX_CHANGES))
      items = process(items, server)
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

