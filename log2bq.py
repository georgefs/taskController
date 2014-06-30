from taskController import ExternalTaskController, TaskController
import os
import json
from datetime import timedelta, datetime
import luigi

gs_bucket = "tagtootest"
gs_name_format = "logs-{:%Y%m%d}"

class Log2GsTask(TaskController):

    _task_path = 'share_libs.pipelines.Log2Gs'
    _task_url = 'http://2.tagtootrack.appspot.com/'
    _kwargs = {
        'mapper': 'main.log2json',
        'start_time': 0,  #total seconds
        'end_time':   0,   #total seconds
        'version_ids': ['1'],
        'gsbucketname': 'tagtootest',
        'shards': 30
    }

    
    output_format = "{0.__class__.__name__}_{0.hour_time:%Y-%m-%d_%H}"
    hour_time = luigi.DateHourParameter()

    def prepare_args(self, inputs):
        args = super(Log2GsTask, self).prepare_args(inputs)
        start_time = self.hour_time.strftime('%s')
        start_time = int(start_time) 
        end_time = start_time + 3600

        
        args['kwargs']['name'] = "log2bq-{:%Y%m%d%H%M%S}".format(self.hour_time)
        args['kwargs']['start_time'] = start_time
        args['kwargs']['end_time'] = end_time
        return args



class Log2BqTask(TaskController):

    hour_time = luigi.DateHourParameter()
    _task_path = 'share_libs.pipelines.Gs2Bq'
    _task_url = 'http://2.tagtootrack.appspot.com/'
    _kwargs = {
        #'files': ["/gs/tagtootest/test-1577677112291931E7B4A-output"],  save to bigquery file
        'bqproject': '103827621493',
        'bqdataset': 'tagtootest',
        'fields': [{'type': 'string', 'name': 'features', 'mode': 'repeated'}, {'type': 'string', 'name': 'vars', 'mode': 'repeated'}, {'type': 'string', 'name': 'ip'}, {'type': 'string', 'name': 'session'}, {'type': 'string', 'name': 'slot'}, {'type': 'string', 'name': 'title'}, {'type': 'string', 'name': 'creative'}, {'type': 'string', 'name': 'pc'}, {'type': 'string', 'name': 'version'}, {'type': 'string', 'name': 'type'}, {'type': 'string', 'name': 'publisher'}, {'type': 'timestamp', 'name': 'start_time'}, {'type': 'string', 'name': 'host'}, {'type': 'string', 'name': 'user'}, {'type': 'string', 'name': 'qp'}, {'type': 'string', 'name': 'target'}, {'fields': [{'type': 'string', 'name': 'var'}, {'type': 'string', 'name': 'advertiser'}, {'type': 'string', 'name': 'campaign'}], 'type': 'record', 'name': 'items', 'mode': 'repeated'}, {'type': 'string', 'name': 'ext'}, {'type': 'string', 'name': 'tag'}, {'type': 'string', 'name': 'user_agent'}, {'type': 'string', 'name': 'referral'}, {'type': 'string', 'name': 'qm'}, {'type': 'string', 'name': 'page'}],
        'overwrite': False,
    }

    output_format = "{0.__class__.__name__}_{0.hour_time:%Y-%m-%d_%H}"

    def requires(self):
        return [Log2GsTask(hour_time=self.hour_time)]

    def prepare_args(self, inputs):
        args = super(Log2BqTask, self).prepare_args(inputs)

        files = json.loads(inputs[0])['output']['default']
        args['kwargs']['files'] = files
        args['kwargs']['table'] = 'logs_{0:%Y%m%d}'.format(self.hour_time)
        return args
        



class Log2GenReport(TaskController):
    date = luigi.DateParameter()

    output_format = "{0.__class__.__name__}_{0.date:%Y-%m-%d}"

    def requires(self):
        result = []
        for i in xrange(24):
            time = datetime(*self.date.timetuple()[:3], hour=i)
            result.append(Log2BqTask(time))

        return result

    def prepare_args(self, inputs):
        date = self.date.strftime('%Y-%m-%d')
        args = super(Log2GenReport, self).prepare_args()
        args['kwargs']['date'] = date
        return args
    

class ReLoadBq(TaskController):
    date = luigi.DateParameter()
    _task_path = 'share_libs.pipelines.Gs2Bq'
    _task_url = 'http://2.tagtootrack.appspot.com/'
    _kwargs = {
        #'files': ["/gs/tagtootest/test-1577677112291931E7B4A-output"],  save to bigquery file
        'bqproject': '103827621493',
        'bqdataset': 'tagtootest',
        'fields': [{'type': 'string', 'name': 'features', 'mode': 'repeated'}, {'type': 'string', 'name': 'vars', 'mode': 'repeated'}, {'type': 'string', 'name': 'ip'}, {'type': 'string', 'name': 'session'}, {'type': 'string', 'name': 'slot'}, {'type': 'string', 'name': 'title'}, {'type': 'string', 'name': 'creative'}, {'type': 'string', 'name': 'pc'}, {'type': 'string', 'name': 'version'}, {'type': 'string', 'name': 'type'}, {'type': 'string', 'name': 'publisher'}, {'type': 'timestamp', 'name': 'start_time'}, {'type': 'string', 'name': 'host'}, {'type': 'string', 'name': 'user'}, {'type': 'string', 'name': 'qp'}, {'type': 'string', 'name': 'target'}, {'fields': [{'type': 'string', 'name': 'var'}, {'type': 'string', 'name': 'advertiser'}, {'type': 'string', 'name': 'campaign'}], 'type': 'record', 'name': 'items', 'mode': 'repeated'}, {'type': 'string', 'name': 'ext'}, {'type': 'string', 'name': 'tag'}, {'type': 'string', 'name': 'user_agent'}, {'type': 'string', 'name': 'referral'}, {'type': 'string', 'name': 'qm'}, {'type': 'string', 'name': 'page'}],
        'overwrite': False,
    }

    output_format = "{0.__class__.__name__}_{0.date:%Y-%m-%d_%H}"


    def prepare_args(self, inputs):
        cmdin, cmdout = os.popen2('gsutil ls "gs://tagtooad-test/log2bq-{}*"'.format(self.date.strftime('%Y%m%d')))
        files = cmdout.read().split()



        args = super(ReLoadBq, self).prepare_args(inputs)

        files = files
        args['kwargs']['files'] = files
        args['kwargs']['table'] = 'logs_{0:%Y%m%d}'.format(self.date)
        return args




    

luigi.run(main_task_cls=Log2GenReport)

