import logging

import SocketServer
import threading

class _MonitorTCPRequestHandler(SocketServer.BaseRequestHandler):

	def handle(self):
		data = True
		while self.server._running and data and data != 'STOP':
			data = self.request.recv(8).strip()
			self.request.send(data)

class _ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
	_running = False


class Server(object):

	def __init__(self, address):
		logging.info('starting server on %s', address)
		self._address = address

		self._server = _ThreadedTCPServer(self._address, _MonitorTCPRequestHandler)
		self._server._running = False
		self._server.timeout = 10

		self._server_thread = threading.Thread(target=self._server.serve_forever)
		self._server_thread.daemon = True

	def start(self):
		self._server._running = True
		self._server_thread.start()

	def stop(self):
		logging.info('Stopping server')
		self._server._running = False
		self._server.shutdown()
		self._server_thread.join()
		logging.info('Stopping stopped')

