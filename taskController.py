# -*- coding: utf-8 -*-
import luigi
import datetime
import json
import time
import logging
import urllib2, urllib
import urlparse
logger = logging.getLogger('luigi-interface')

TRIGGER = "/_task/trigger"
STATUS = "/_task/status"
STOP = "/_task/stop"


# task server 端
class TaskController(luigi.Task):

    _task_url = None #url which support taskWorker
    _task_path = None #path where taskWorker here 
    _args = [] # for task args
    _kwargs = {} # for task kwargs
    _delay = 30 # check status lantance
    _id = None # custom setting task id

    _retry = 0 #retry times 
    _timeout = 0 # time out for sec 0 for unlimit
    
    output_format = "{0.__class__.__name__}"

    def requires(self):
        return []

    def output(self):
        name = self.output_format.format(self)
        return luigi.LocalTarget(name)

    def run(self):
        inputs = self.__read_input()

        args = self.prepare_args(inputs)
        task_id = self.__start_task(args)
        
        for retry_count in xrange(self._retry + 1):
            try:
                result = self.__watting_task(task_id)
            except Exception as e:
                self.__retry_task(task_id)
            finally:
                logger.info('retry {}'.format(retry_count))
                if self._retry and retry_count >= self._retry:
                    logger.error('maxmun retry')
                    raise Exception('maxmun retry')

        self.__write_output(result)



    ## prepare task prams
    def prepare_args(self, inputs):
        data = {
            "args": self._args,
            "kwargs": self._kwargs,
        }

        return data
    

    ## load task 
    def __read_input(self):
        inputs = []
        print 'run'
        for result in self.input(): 
            result = result.open('r').read()
            inputs.append(result)
        return inputs

    
    ## trigger task
    def __start_task(self, args):
        trigger_api = urlparse.urljoin(self._task_url, TRIGGER)
        
        args['args'] = json.dumps(args['args'])
        args['kwargs'] = json.dumps(args['kwargs'])
        args['path'] = self._task_path

        query = urllib.urlencode(args)
        resp = urllib2.urlopen(trigger_api, query)
        
        task_id = json.loads(resp.read())['id']
        logger.info('start: {}'.format(task_id))
        return task_id

    
    ## watting for task end and get task result
    def __watting_task(self, task_id):
        check_status_api = urlparse.urljoin(self._task_url, STATUS)

        start_time = datetime.datetime.now()

        while True:
            try:
                result = urllib2.urlopen(check_status_api + "?id=" + task_id ).read()
                status = json.loads(result)['status'].lower()
                if status == 'done':
                    break
                elif status == "failed":
                    raise Exception('failed')

                logger.info('watting {}'.format(task_id))

            except urllib2.URLError, e:
                if e.code == 500:
                    logger.error('server error ')
                    raise e
                else:
                    continue
            except Exception as e:
                logger.error('error for {}'.format(e.message))
                raise e

            finally:
                time.sleep(self._delay)
                delta = datetime.datetime.now() - start_time
                assert not self._timeout or delta.total_seconds() < self._timeout, 'Timeout Exception'



    ## stop task
    def __stop_task(self, task_id):
        stop_api = urlparse.urljoin(self._task_url, STOP)
        urllib2.urlopen(stop_api + "?id="+task_id).read()


    ## retry task
    def __retry_task(self, task_id):
        logger.info('retry')
        self.__stop_task(task_id)
        self.__start_task()


    def __write_output(self, result):
        output_file = self.output().open('w')
        output_file.write(json.dumps(result))




        
        


# 不需要 reguires 的 task(default 都會去 search)
class ExternalTaskController(TaskController):
    run = NotImplemented


