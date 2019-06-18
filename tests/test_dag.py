import unittest
import airflow
from airflow.models import DagBag

class TestDag(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag(dag_folder="/Users/oscar.barlow/Projects/work/infinityworks/airflow-hacknight-coke/dags")

    def test_should_be_s3_sensor_task(self):
        dag_id = 's3_event'
        dag = self.dagbag.get_dag(dag_id)
        tasks = dag.tasks

        task_ids = list(map(lambda task: task.task_id, tasks))
        self.assertListEqual(task_ids, ['s3_sensor', 'print_key'])

        self.assertEqual(type(tasks[0]), airflow.operators.sensors.S3KeySensor)


