import prefect
from prefect import task, Flow
from prefect.executors import DaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GitHub

@task
def get_data():
    return [*range(1000)]

@task
def map_fn(x):
    return x + 1

@task
def reduce(x):
    logger = prefect.context.get('logger')
    result = sum(x)
    logger.info(f'result: {result}')
    return result

@task
def new_task():
    logger = prefect.context.get('logger')
    logger.info('Hello, goodbye')

with Flow('Dask Map Reduce') as flow:
    data = get_data()
    mapped_result = map_fn.map(data)
    result = reduce(mapped_result)
    new_task()

flow.run_config = KubernetesRun()
flow.executor = DaskExecutor(
    cluster_kwargs={
        'image': 'dprester/dask_worker_base2'
    }
)
flow.storage = GitHub(
    repo='dominikprester/prefect-flow-storage-test',
    path='flows/dask_map_reduce.py'
)