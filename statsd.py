'''
StatsD client ( https://github.com/etsy/statsd )

Usage
~~~~~

1.a. init default client on the top of the module

	statsc = statsd.get()

1.b. init named client on the top of the module (recommended)

	statsc = statsd.get(__name__)

2.a. configure all clients at runtime

	if __name__ == "__main__":
		statsd.config(("statsd.zemanta.com", 8125), "prefs")

2.b. configure single client at runtime

	if __name__ == "__main__":
		statsd.config(("statsd.zemanta.com", 8125), "prefs" , "prefs.api.provisioning")

3.a. use client directly

	statsc("foo.bar", duration in milliseconds, statsd.TIMER)
	statsc("foo.bar", some arbitrary integer, statsd.GAUGE)
	statsc("foo.bar", counter value, statsd.COUNTER)

3.b. use client wrappers

	statsc.timer("foo.bar", duration in milliseconds)
	statsc.gauge("foo.bar", some arbitrary integer)
	statsc.counter("foo.bar", counter value)

3.c. use the timerblock api

	with statsc.timerblock('foo.bar') as stop:
		time.sleep(0.1)
		stop()
		some_other_non_timed_thing()

3.d. use the timerblock api

	with statsc.timerblock('foo.bar'):
		time.sleep(0.1)


A fully working example
~~~~~~~~~~~~~~~~~~~~~~~
import logging
import time
import random
logging.basicConfig(level=logging.DEBUG)

STATSD_HOST = ('statsd.zemanta.com', 8125)

from zlibs.monitoring import statsd2 as statsd
statsc = statsd.get(__name__)

if __name__	== '__main__':
	statsd.config(STATSD_HOST,'project_foo')

	while 1:
		with statsc.timeblock('foo.bar', debug=True) as stop:
			time.sleep(0.1)
			stop()
		statsc.gauge('foo.gauge', 1600 + random.randint(0,50), debug=True)
		statsc.counter('foo.count', 1, debug=True)
'''
import time
import socket
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)
statsd_clients_dict = {}
configured = False
socket_wrapper = None
project_name = None


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# types of stats https://github.com/etsy/statsd

# Counting
# 	gorets:1|c
# 	This is a simple counter. Add 1 to the "gorets" bucket.
# 	It stays in memory until the flush interval config.flushInterval.
COUNTER = "c"

# Timing
#	glork:320|ms
# 	The glork took 320ms to complete this time. StatsD figures out 
#	90th percentile, average (mean), lower and upper bounds for the
#	flush interval. The percentile threshold can be tweaked with 
#	config.percentThreshold.
TIMER = "ms"

#Gauges
#	StatsD now also supports gauges, arbitrary values, which can be recorded.
#	gaugor:333|g
GAUGE = "g"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class StatsDTimer(object):

	def __init__(self,statsd_client,time0, service, debug):
		self._statsd_client = statsd_client
		self._time0 = time0
		self._service = service
		self._debug = debug
		self._stopped = False

	def __call__(self):
		if self._stopped:
			return
		duration = (time.time() - self._time0) * 1000 # we need milliseconds
		self._stopped = True
		self._statsd_client(self._service,duration,TIMER,self._debug)



class StatsDClient(object):

	def config(self, socket_wrapper, project):
		self._socket_wrapper = socket_wrapper
		self._project = project

	def __call__(self, label, value, typ, debug=False):
		if not hasattr(self,'_socket_wrapper') or self._socket_wrapper is None:
			return
		# prepare data
		project = self._project
		label = label.lower()
		if isinstance(value, float):
			value = str(int(value))
		else:
			value = str(value)
		data = '%(project)s.%(label)s:%(value)s|%(typ)s' % vars()

		self._socket_wrapper.send_data(data, debug)

	def counter(self,service,value,debug=False):
		self(service,value,COUNTER,debug)

	def timer(self,service,value,debug=False):
		self(service,value,TIMER,debug)

	def gauge(self,service,value,debug=False):
		self(service,value,GAUGE,debug)

	@contextmanager
	def timerblock(self, service, debug=False):
		sd_timer = StatsDTimer(self, time.time(), service, debug)
		try:
			yield sd_timer
		finally:
			sd_timer()


class ClientSocketWrapper(object):

	def __init__(self, host_port):
		try:
			self._client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			# this is used as part of the data sent to statsd
			self.local_hostname = socket.gethostname().replace('.', '_')
			self._statsd_host_port = ( socket.gethostbyname(host_port[0]), host_port[1] )
			logger.info("Statsd client %s will be sending to %s" , repr(self), repr(self._statsd_host_port))
		except Exception, e:
			logger.exception("Statsd client configuration error: %s", repr(e))

	def send_data(self, data, debug=False):
		try:
			if debug: # emit log if in debug mode
				logger.debug("Statsd client sending %s to %s" % (repr(data),repr(self._statsd_host_port)) )
			self._client_socket.sendto(data, self._statsd_host_port)
		except Exception, e:
			logger.exception("Statsd client error %s", repr(e))


def config(host_port_tuple, project, name=None):
	global statsd_clients_dict, configured, socket_wrapper, project_name
	if configured:
		logger.warn('statsd is already configured')
		return
	configured = True

	if host_port_tuple is None:
		socket_wrapper = None
	else:
		socket_wrapper = ClientSocketWrapper(host_port_tuple)
	project_name = project

	if name:
		logger.debug('configured :%s: statsd client', name)
		statsd_clients_dict[name].config(socket_wrapper, project.lower())
		return

	for name, statsd_client in statsd_clients_dict.iteritems():
		logger.debug('configured :%s: statsd client', name)
		statsd_client.config(socket_wrapper, project.lower())


def get(name=''):
	global statsd_clients_dict, configured, socket_wrapper, project_name
	if name not in statsd_clients_dict:
		statsd_client = StatsDClient()
		if configured:		# statsd library has already been configured, so we need to configure this newly created client here
			logger.debug('configured :%s: statsd client', name)
			statsd_client.config(socket_wrapper, project_name)
		statsd_clients_dict[name] = statsd_client
		return statsd_client
	return statsd_clients_dict[name]

