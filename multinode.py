
import multiprocessing.managers
import Queue
import threading

PORT_NUMBER = 45237

class NodeServer(object):
	
	class NodeManager(multiprocessing.managers.BaseManager): pass
	def __init__(self):
		self.jobqueue = Queue.Queue()
		self.jobresult = {}
		self.jobclient = {}
		NodeManager.register('get_job', callable=lambda:getjob())
		NodeManager.register('job_count', callable=lambda:jobqueue.qsize())
		pass
		
	def getjob(self):
		#grab a job from the queue, return it and it's related result
		job = self.jobqueue.get()
		return (job, self.jobresult[job])
	
	#pool members
	def apply_async(self, func, args=(), kwargs=None):
        if not kwargs:
            kwargs = {}
		#create a job and result
		job = (func, args, kwargs)
		result = NodeResult()
		
		#map them
		self.jobresult[job] = result
		
		#add the job to the queue
		self.jobqueue.put(job)
		
		#return the result (for the caller to 'get' later)
        return result
		
    def close(self):
        pass
    def join(self):
		#possibly block here until there are no jobs in the queue?
        pass
		
	#result that is populated by the processor node
	#this is heavily based on the ApplyResult from multiprocessing.Pool module
	class NodeResult(object):
		def __init__(self):
			self._cond = threading.Condition(threading.Lock())
			self._ready = False
			self._res = None
			
		def wait(self):
			self._cond.acquire()
			try:
				if not self._ready:
					self._cond.wait(timeout)
			finally:
				self._cond.release()
				
		def ready(self):
			return self._ready

		def successful(self):
			assert self._ready
			return self._success
			
		def get(self, timeout=None):
			#block here until self.res is set
			self.wait(timeout)
			if not self._ready:
				raise TimeoutError
			if self._success:
				return self._value
			else:
				raise self._value
				
		def _set(self, val):
			self._success, self._value = val
			self._cond.acquire()
			try:
				self._ready = True
				self._cond.notify()
			finally:
				self._cond.release()
			
class NodeClient(object):
	def __init__(self, manager_address, manager_authkey=''):
		self.address = address
		self.authkey = authkey
		self.manager = NodeManager(address=self.address, authkey=self.authkey)
		self.manager.connect()
		
	def go(self):
		#grab a job
		job = self.manager.get_job()
		func = job[0][0]
		args = job[0][1]
		kwargs = job[0][2]
		result = job[1]
		
		result._set(func(*args, **kwargs))