import sys
import time
import logging

import socket
import SocketServer
import threading
from Queue import Queue

import statsd

import settings

local_address = (sys.argv[1], int(sys.argv[2]))
servers = [(addr, port) for addr, port in settings.SERVER_LIST if addr != local_address[0] or port != local_address[1]]

statsc = None
if settings.STATSD_HOST:
	statsd.config(settings.STATSD_HOST, settings.STATSD_PROJECT_NAME)
	statsc = statsd.get()


stats_queue = Queue()


def flush_stats():

	stats_files = {}
	def open_file(mtype, local_server_name, server_name):
		fname = '%s%s_%s_%s.log' % (settings.STATS_LOG_DIR, mtype, local_server_name, server_name)
		if fname in stats_files:
			return stats_files[fname]

		stats_files[fname] = open(fname, 'a', buffering=1)
		return stats_files[fname]

	while True:
		t, mtype, server, val = stats_queue.get(block=True)
		logging.debug('STATS: %s %s %s', mtype, server, val)

		local_server_name = local_address[0]
		server_name = server[0]

		if statsc:
			if mtype == 'timer':
				statsc.timer('tcp_beep.%s.%s' % (local_server_name, server_name), val)
			elif mtype == 'error':
				statsc.counter('tcp_error.%s.%s.%s' % (local_server_name, server_name, val), 1)

		if settings.WRITE_STATS_TO_FILE:
			f = open_file(mtype, local_server_name, server_name)
			if mtype == 'timer':
				f.write('%s %s\n' % (t, val))
			elif mtype == 'error':
				f.write('%s %s\n' % (t, val))

def single_client(server):

	while True:
		try:
			logging.info('connecting to % s', server)

			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect(server)
			while True:
				t0 = time.time()

				sock.send("X\n")
				sock.recv(2)

				td = (time.time() - t0) * 1000

				stats_queue.put((t0, 'timer', server, td))

				time.sleep(settings.MESSAGE_INTERVAL)

			sock.send("QUIT\n")
		except Exception:
			stats_queue.put((time.time(), 'error', server, 'connection_error'))
		finally:
			sock.close()
			time.sleep(1)

class ThreadedTCPRequestHandler(SocketServer.StreamRequestHandler):

	def handle(self):
		data = None
		while data != 'QUIT':
			data = self.rfile.readline().strip()
			self.wfile.write(data)

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
	pass

def server():
	logging.info('starting server on %s', local_address)

	server = ThreadedTCPServer(local_address, ThreadedTCPRequestHandler)
	server_thread = threading.Thread(target=server.serve_forever)
	server_thread.daemon = True
	return server_thread

def client(server):
	logging.info('starting client for %s', server)

	client_thread = threading.Thread(target=single_client, args=[server,])
	client_thread.daemon = True
	return client_thread


def run_monitor():
	fm = threading.Thread(target=flush_stats)
	fm.daemon = True

	fm.start()
	s = server()
	s.start()
	cs = []
	for se in servers:
		c = client(se)
		cs.append(c)
		c.start()

	for c in cs:
		c.join()
	s.join()
	stats_queue.join()
	fm.join()

if __name__ == "__main__":
	run_monitor()
