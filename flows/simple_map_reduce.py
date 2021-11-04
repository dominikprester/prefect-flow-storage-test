import prefect
from prefect import task, Flow
from prefect.executors import LocalExecutor
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

with Flow('Simple Map Reduce') as flow:
    data = get_data()
    mapped_result = map_fn.map(data)
    result = reduce(mapped_result)

flow.run_config = KubernetesRun()
flow.executor = LocalExecutor()
flow.storage = GitHub(
    repo='dominikprester/prefect-flow-storage-test',
    path='flows/simple_map_reduce.py'
)