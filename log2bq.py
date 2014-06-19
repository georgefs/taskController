from taskController2 import ExternalTaskController, TaskController
import json
from datetime import timedelta, datetime
import luigi

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

    output_format = "{0.__class__.__name__}_{0.hour_time:%Y-%m-%dT%H}"
    hour_time = luigi.DateHourParameter()


    def prepare_args(self, inputs):
        args = super(Log2GsTask, self).prepare_args(inputs)
        start_time = self.hour_time.strftime('%s')
        start_time = int(start_time)
        end_time = start_time + 3600

        
        args['kwargs']['start_time'] = start_time
        args['kwargs']['end_time'] = end_time
        return args




class Gs2BqTask(TaskController):


    _task_path = 'share_libs.pipelines.Gs2Bq'
    _task_url = 'http://client.tagtootrack.appspot.com/'
    _kwargs = {
        #'files': ["/gs/tagtootest/test-1577677112291931E7B4A-output"],  save to bigquery file
        'bqproject': '103827621493',
        'bqdataset': 'tagtootest',
        'table': 'test_table',
        'fields': [{'type': 'string', 'name': 'features', 'mode': 'repeated'}, {'type': 'string', 'name': 'vars', 'mode': 'repeated'}, {'type': 'string', 'name': 'ip'}, {'type': 'string', 'name': 'session'}, {'type': 'string', 'name': 'slot'}, {'type': 'string', 'name': 'title'}, {'type': 'string', 'name': 'creative'}, {'type': 'string', 'name': 'pc'}, {'type': 'string', 'name': 'version'}, {'type': 'string', 'name': 'type'}, {'type': 'string', 'name': 'publisher'}, {'type': 'timestamp', 'name': 'start_time'}, {'type': 'string', 'name': 'host'}, {'type': 'string', 'name': 'user'}, {'type': 'string', 'name': 'qp'}, {'type': 'string', 'name': 'target'}, {'fields': [{'type': 'string', 'name': 'var'}, {'type': 'string', 'name': 'advertiser'}, {'type': 'string', 'name': 'campaign'}], 'type': 'record', 'name': 'items', 'mode': 'repeated'}, {'type': 'string', 'name': 'ext'}, {'type': 'string', 'name': 'tag'}, {'type': 'string', 'name': 'user_agent'}, {'type': 'string', 'name': 'referral'}, {'type': 'string', 'name': 'qm'}, {'type': 'string', 'name': 'page'}]
    }

    hour_time = luigi.DateHourParameter()
    output_format = "{0.__class__.__name__}_{0.hour_time:%Y-%m-%dT%H}"

    def requires(self):
        return [Log2GsTask(self.hour_time)]

    def prepare_args(self, inputs):
        args = super(Gs2BqTask, self).prepare_args(inputs)

        files = json.loads(inputs[0])['output']['default']
        args['kwargs']['files'] = files
        return args
        

class GenReport(TaskController):
    date = luigi.DateParameter(default=datetime(2014,06,19))

    output_format = "{0.__class__.__name__}_{0.date:%Y-%m-%d}"

    def requires(self):
        return [Gs2BqTask(self.date + timedelta(hours=i)) for i in xrange(24)]
    
    def prepare_args(self, inputs):
        date = date.strftime('%Y-%m-%d')
        args = super(Gs2BqTask, self).prepare_args()
        args['kwargs']['date'] = date
        return args
    
    
import luigi
luigi.run(main_task_cls=GenReport()) 
