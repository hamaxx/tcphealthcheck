import logging
import time
import random

import threading
import socket

import settings

class Client(object):

	def __init__(self, address, stats):
		logging.info('starting client for %s', address)

		self._running = False
		self._address = address
		self._stats = stats

		self._client_thread = threading.Thread(target=self._run_client)
		self._client_thread.daemon = True

	def start(self):
		self._running = True
		self._client_thread.start()

	def stop(self):
		self._running = False
		self._client_thread.join()

	def _sleep_for(self):
		delay = 1.0 / settings.MESSAGES_PER_SECOND
		rand = random.random() * 2
		return rand * delay

	def _run_client(self):
		while self._running:
			try:
				logging.info('connecting to % s', self._address)

				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect(self._address)
				while self._running:
					td = 0
					for x in xrange(settings.MESSAGES_PER_REPORT):
						if not self._running:
							sock.send("STOP\n")
							break

						t0 = time.time()
						sock.send("X\n")
						sock.recv(2)
						td = max(td, time.time() - t0)
						time.sleep(self._sleep_for())

					self._stats.put('timer', self._address, td * 1000, t0)
			except Exception:
				self._stats.put('error', self._address, 'connection_error')
			finally:
				sock.close()
				time.sleep(1)

class ClientPool(object):

	def __init__(self, servers, stats_queue):
		self._clients = []

		for server in servers:
			self._clients.append(Client(server, stats_queue))

	def start(self):
		delay = float(settings.MESSAGES_PER_REPORT) / float(settings.MESSAGES_PER_SECOND) / len(self._clients)
		delay *= random.random() * 2

		for client in self._clients:
			client.start()
			time.sleep(delay)

	def stop(self):
		logging.info('Stopping clients')
		for client in self._clients:
			client.stop()
		logging.info('Clients stopped')

