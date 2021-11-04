import prefect
from prefect import task, Flow
from prefect.executors import DaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GitHub

from dask_kubernetes import KubeCluster, make_pod_spec

# could/should be wrapped in THE package?
def get_kube_dask_cluster(image):
    pod_spec = make_pod_spec(image=image)
    return KubeCluster(pod_template=pod_spec)

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

with Flow('KubeDask Map Reduce') as flow:
    data = get_data()
    mapped_result = map_fn.map(data)
    result = reduce(mapped_result)
    new_task()

flow.run_config = KubernetesRun(
    image='dprester/kubedask_run_base'
)
flow.executor = DaskExecutor(
    cluster_class=get_kube_dask_cluster,
    cluster_kwargs={
        'image': 'dprester/dask_worker_base2'
    }
)
flow.storage = GitHub(
    repo='dominikprester/prefect-flow-storage-test',
    path='flows/kubedask_map_reduce.py'
)