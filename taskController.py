# -*- coding: utf-8 -*-
import luigi
import datetime
import json
import time
import urllib2, urllib
import urlparse

TRIGGER = "/_task/trigger"
STATUS = "/_task/status"
STOP = "/_task/stop"

# task server 端
class TaskController(luigi.Task):

    _task_url = None #url which support taskWorker
    _task_path = None #path where taskWorker here 
    _args = []
    _kwargs = {}
    _delay = 30 # check status lantance
    _input = []
    _id = None # task id

    def requires(self):
        return []
    

    def output(self):
        name = self.__class__.__name__
        return luigi.LocalTarget(name)

    def run(self):
        inputs = []
        print 'run'
        for result in self.input(): 
            result = result.open('r').read()
            inputs.append(result)
        print 'get depance'

        self._inputs = inputs
        print 'trigger'
        self.pre_trigger()
        self._id = self._trigger()

        result = self._check_status()

        f = self.output().open('w')
        f.write(json.dumps(result))
        f.close()


    def pre_trigger(self):
        pass

    
    def _trigger(self):
        trigger_url = urlparse.urljoin(self._task_url, TRIGGER)
        data = {
            "path": self._task_path,
            "args": self._args,
            "kwargs": json.dumps(self._kwargs)
        }
        query = urllib.urlencode(data)
        resp = urllib2.urlopen(trigger_url, query)

        _id = json.loads(resp.read())['id']
        print "id" + _id

        return _id


    def _check_status(self):
        status_url = urlparse.urljoin(self._task_url, STATUS)
        status_url += "?id={}".format(self._id)

        while True:
            print 'check status'
            time.sleep(self._delay)
            try:
                resp = urllib2.urlopen(status_url)
                info = json.loads(resp.read())
                # success
                if info['status'].lower() == "done":
                    print 'success'
                    return info
                # error
                elif info['status'].lower() == "failed":
                    raise Exception(info["msg"])
                else:
                    continue


            except urllib2.URLError, e:
                if e.code == 500:
                    import pdb;pdb.set_trace()
                    raise e
                else:
                    continue


    def _stop(self):
        stop_url = urllib.urljoin(self._task_url, STATUS)
        stop_url += "?id={}".format(self._id)

        urllib2.urlopen(stop_url)



        


# 不需要 reguires 的 task(default 都會去 search)
class ExternalTaskController(TaskController):
    run = NotImplemented


