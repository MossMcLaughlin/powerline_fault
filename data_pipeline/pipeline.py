from common.configs import pipeline_config, ml_config
from common.logger import logger
from data_pipeline.pipeline_tasks import \
    DataPreparationTask, FeatureEngineeringTask, ModelTrainingTask, ReportGenerationTask


class DataPipeline:
    def __init__(self):
        self.pipeline_config = pipeline_config
        self.ml_config = ml_config

        possible_tasks = {
            'data_preparation': DataPreparationTask,
            'model_training': ModelTrainingTask,
            'report_generation': ReportGenerationTask
        }
        self.tasks = [
            possible_tasks[task_name]
            for task_name in pipeline_config['pipeline_tasks']
        ]

    def proceed(self, task):
        logger.info("Starting task {}".format(task.name))
        task.run()
        logger.info('Successfully completed task {}'.format(task.name))

    def run_pipeline(self):
        logger.info('Running pipeline...')
        for task in self.tasks:
            self.proceed(task)



