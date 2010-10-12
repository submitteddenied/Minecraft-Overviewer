
import multiprocessing.managers
from multiprocessing import TimeoutError
import Queue
import threading

PORT_NUMBER = 45237

class NodeManager(multiprocessing.managers.BaseManager): pass

#result that is populated by the processor node
#this is heavily based on the ApplyResult from multiprocessing.Pool module
class NodeResult(object):
    def __init__(self):
        self._cond = threading.Condition(threading.Lock())
        self._ready = False
        self._res = None
        
    def wait(self, timeout=None):
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

class NodeServer(object):
    #pool members
    def apply_async(self, func, args=(), kwds=None):
        if not kwds:
            kwds = {}
        #create a job with a result
        result = NodeResult()
        self.jobresult[self.jobindex] = result
        job = (func, args, kwds, self.jobindex)
        self.jobindex += 1
        
        #add the job to the queue
        self.jobqueue.put(job)
        
        #return the result (for the caller to 'get' from later)
        return result
        
    def close(self):
        pass
        
    def join(self):
        #possibly block here until there are no jobs in the queue?
        pass
    
    def __init__(self, serve_address=('localhost', PORT_NUMBER), serve_authkey=''):
        self.jobqueue = Queue.Queue()
        self.jobresult = {}
        self.jobindex = 0
        NodeManager.register('getjob', callable=lambda:self.getjob())
        NodeManager.register('jobcount', callable=lambda:self.jobqueue.qsize())
        NodeManager.register('submitjob', callable=lambda id, result: self.jobresult[id]._set(result))
        self.manager = NodeManager(address=serve_address, authkey=serve_authkey)
        server_thread = threading.Thread(target=self.manager.get_server().serve_forever)
        server_thread.setDaemon(True)
        server_thread.start()
        
    def getjob(self):
        #grab a job from the queue, return it and it's related result
        job = self.jobqueue.get()
        return job
            
class NodeClient(object):
    def __init__(self, manager_address=('localhost', PORT_NUMBER), manager_authkey='', environment=None):
        self.address = manager_address
        self.authkey = manager_authkey
        NodeManager.register('getjob')
        NodeManager.register('jobcount')
        NodeManager.register('submitjob')
        self.manager = NodeManager(address=self.address, authkey=self.authkey)
        self.manager.connect()
        self.environment = environment
        self.debug = False
        
    def go(self):
        #grab a job (getjob() returns an autoproxy, _getvalue() grabs out the
        #tuple inside it...
        job = self.manager.getjob()._getvalue()
        #this code kind of sucks, magic numbers abound!
        #the important thing here is that the elements of job are:
        #       func - the function to call
        #       args - the args for the function
        #       kwds - more args for the function
        #       resultkey - the key to the result stored at the server
        
        if self.debug: print "Got a job!"
        
        func = job[0]
        args = job[1]
        kwds = job[2]
        #add the node specific environment information
        #(or clobber what was passed...)
        kwds["environment"] = self.environment
        resultkey = job[3]
        
        #call to func() here does the actual work
        try:
            result = (True, func(*args, **kwds))
            if self.debug: print "Job #{0} - complete!".format(resultkey)
        except Exception, e:
            result = (False, e)
            if self.debug: print "Job #{0} - failed!".format(resultkey)
        
        self.manager.submitjob(resultkey, result)
        
        
    def run(self):
        #basically run until this node is killed.
        #this is designed to be run in a background thread
        if self.debug: print "Node starting up, connected to manager..."
        while True:
            if self.debug:
                print "waiting for a job"
            
            self.go()
        
    def startbg(self):
        self.client_thread = threading.Thread(target=self.run)
        self.client_thread.setDaemon(True)
        self.client_thread.start()