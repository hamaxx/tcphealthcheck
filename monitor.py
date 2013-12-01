import sys
import time

import settings
from server import Server
from client import ClientPool
from reporting import Stats

def run_monitor():
	local_address = (sys.argv[1], int(sys.argv[2]))
	servers = [(addr, port) for addr, port in settings.SERVER_LIST if addr != local_address[0] or port != local_address[1]]

	stats = Stats(local_address)
	stats.start()

	server = Server(local_address)
	server.start()

	client_pool = ClientPool(servers, stats)
	client_pool.start()

	try:
		while True:
			time.sleep(1)
	except KeyboardInterrupt:
		server.stop()
		client_pool.stop()
		stats.stop()

if __name__ == "__main__":
	run_monitor()
