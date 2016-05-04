from airflow.plugins_manager import AirflowPlugin
from airflow.operators import BashOperator
from airflow.utils import apply_defaults
import os

class HadoopStreamingOperator(BashOperator):
    @apply_defaults
    def __init__(
            self,
            input,
            output,
            mapper,
            combiner=None,
            reducer=None,
            hadoop_site_conf='hadoop-site.xml',
            hadoop_cmd='hadoop',
            files=None,
            archives=None,
            cmd_args=None,
            base_path=None,
            *args, **kwargs):
        if not hadoop_site_conf:
            raise AirflowException('A Hadoop site configuration file should be specified.')

        # get base path
        dag = kwargs.get('dag')
        base_path = base_path or dag.dag_id
        airflow_home = os.getenv('AIRFLOW_HOME', '~/airflow') + '/dags/'
        base_path = airflow_home + base_path
        params = kwargs.get('params', {})
        params['base_path'] = base_path
        kwargs['params'] = params

        # add site configuration file
        cmd = hadoop_cmd + ' streaming'
        cmd += ' -conf {{ params.base_path }}/' + hadoop_site_conf

        # add input and output
        if isinstance(input, list):
            input = ','.join(input)
        cmd += ' -input ' + input
        cmd += ' -output ' + output

        # add mapper, combiner and reducer
        cmd += ' -mapper ' + mapper
        if combiner:
            cmd += ' -combiner ' + combiner
        if reducer:
            cmd += ' -reducer ' + reducer
        else:
            cmd_args = cmd_args + ' -recuder NONE' if cmd_args else '-reducer NONE'

        # add files
        if files:
            if isinstance(files, basestring):
                files = [files]
            for f in files:
                cmd += ' -file {{ params.base_path }}/' + f

        # add archives
        if archives:
            if isinstance(archives, basestring):
                archives = [archives]
            for a in archives:
                cmd += ' -cacheArchive ' + a

        # add command args
        if cmd_args:
            cmd += ' ' + cmd_args

        # super init
        super(HadoopStreamingOperator, self).__init__(bash_command=cmd, base_path=base_path, *args, **kwargs)


# Defining the plugin class
class HadoopPlugin(AirflowPlugin):
    name = "hadoop"
    operators = [HadoopStreamingOperator]

