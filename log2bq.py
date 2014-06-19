from taskController import ExternalTaskController, TaskController
import json

class Log2GsTask(TaskController):
    _task_path = 'share_libs.pipelines.Log2Gs'
    _task_url = 'http://client.tagtootrack.appspot.com/'
    _kwargs = {
        'name': 'test',
        'mapper': 'main.log2json',
        'start_time': 1402979734,
        'end_time':   1402980864,
        'version_ids': ['1'],
        'gsbucketname': 'tagtootest',
        'shards': 5
    }



class Gs2BqTask(TaskController):
    _task_path = 'share_libs.pipelines.Gs2Bq'
    _task_url = 'http://client.tagtootrack.appspot.com/'
    _kwargs = {
        #'files': ["/gs/tagtootest/test-1577677112291931E7B4A-output"],
        'bqproject': '103827621493',
        'bqdataset': 'tagtootest',
        'table': 'test_table',
        'fields': [{'type': 'string', 'name': 'features', 'mode': 'repeated'}, {'type': 'string', 'name': 'vars', 'mode': 'repeated'}, {'type': 'string', 'name': 'ip'}, {'type': 'string', 'name': 'session'}, {'type': 'string', 'name': 'slot'}, {'type': 'string', 'name': 'title'}, {'type': 'string', 'name': 'creative'}, {'type': 'string', 'name': 'pc'}, {'type': 'string', 'name': 'version'}, {'type': 'string', 'name': 'type'}, {'type': 'string', 'name': 'publisher'}, {'type': 'timestamp', 'name': 'start_time'}, {'type': 'string', 'name': 'host'}, {'type': 'string', 'name': 'user'}, {'type': 'string', 'name': 'qp'}, {'type': 'string', 'name': 'target'}, {'fields': [{'type': 'string', 'name': 'var'}, {'type': 'string', 'name': 'advertiser'}, {'type': 'string', 'name': 'campaign'}], 'type': 'record', 'name': 'items', 'mode': 'repeated'}, {'type': 'string', 'name': 'ext'}, {'type': 'string', 'name': 'tag'}, {'type': 'string', 'name': 'user_agent'}, {'type': 'string', 'name': 'referral'}, {'type': 'string', 'name': 'qm'}, {'type': 'string', 'name': 'page'}]
    }

    def requires(self):
        return [Log2GsTask()]

    def pre_trigger(self):
        files = json.loads(self._inputs[0])['output']['default']
        self._kwargs['files'] = files
        



    
    
    
import luigi
luigi.run(main_task_cls=Gs2BqTask) 
