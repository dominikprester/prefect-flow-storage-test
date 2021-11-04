import prefect
from prefect import task, Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GitHub

@task
def return_world():
    logger = prefect.context.get('logger')
    logger.info('prvi task log')
    return "world"

@task
def hello(arg):
    logger = prefect.context.get('logger')
    logger.info(f'got argument {arg}')
    logger.warning('warning log test')
    logger.error('error log test')
    logger.info(f'result: hello {arg}')
    logger.warning('warning log test')
    logger.error('error log test')

with Flow('Hello World Flow') as flow:
    w = return_world()
    res = hello(w)

flow.run_config = KubernetesRun()
flow.storage = GitHub(
    repo='dominikprester/prefect-flow-storage-test',
    path='flows/hello_world.py'
)