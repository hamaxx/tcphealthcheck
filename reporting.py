import logging
import time
import threading
from Queue import Queue, Empty

import statsd

import settings


class Stats(object):

	def __init__(self, local_server):
		self._stats_queue = Queue()

		self._running = False

		self._reporters = []

		if settings.WRITE_STATS_TO_FILE:
			self._reporters.append(StatsFileReporter(local_server))

		if settings.STATSD_HOST:
			self._reporters.append(StatsDReporter(local_server))

	def start(self):
		self._flush_thread = threading.Thread(target=self._flush_worker)
		self._flush_thread.daemon = True
		self._running = True
		self._flush_thread.start()

	def stop(self):
		logging.info('Stopping Stats reporter')
		self._stats_queue.join()
		self._running = False
		self._flush_thread.join()
		logging.info('Stats reporter stopped')

	def _flush_worker(self):
		while True:
			try:
				t, mtype, server, val = self._stats_queue.get(block=False)
			except Empty:
				if self._running:
					time.sleep(0.1)
					continue
				else:
					break

			logging.debug('STATS: %s %s %s', mtype, server, val)

			for reporter in self._reporters:
				reporter.write(t, mtype, server, val)

			self._stats_queue.task_done()

	def put(self, mtype, server, val, t=None):
		if not t:
			t = time.time()

		self._stats_queue.put((t, mtype, server, val))


class StatsReporter(object):

	def __init__(self, local_server):
		pass

	def write(self, t, mtype, server, val):
		pass


class StatsFileReporter(StatsReporter):

	def __init__(self, local_server):
		self._local_server = local_server
		self._stats_files = {}

	def _open_file(self, mtype, local_server_name, server_name):
		fname = '%s%s_%s_%s.log' % (settings.STATS_LOG_DIR, mtype, local_server_name, server_name)
		if fname in self._stats_files:
			return self._stats_files[fname]

		self._stats_files[fname] = open(fname, 'a', buffering=1)
		return self._stats_files[fname]

	def write(self, t, mtype, server, val):
		local_server_name = self._local_server[0]
		server_name = server[0]

		f = self._open_file(mtype, local_server_name, server_name)
		if mtype == 'timer':
			f.write('%s %s\n' % (t, val))
		elif mtype == 'error':
			f.write('%s %s\n' % (t, val))

class StatsDReporter(StatsReporter):

	def __init__(self, local_server):
		self._local_server = local_server

		statsd.config(settings.STATSD_HOST, settings.STATSD_PROJECT_NAME)
		self._statsc = statsd.get()

	def write(self, t, mtype, server, val):
		local_server_name = self._local_server[0]
		server_name = server[0]

		if mtype == 'timer':
			self._statsc.timer('tcp_beep.%s.%s' % (local_server_name, server_name), val)
		elif mtype == 'error':
			self._statsc.counter('tcp_error.%s.%s.%s' % (local_server_name, server_name, val), 1)

