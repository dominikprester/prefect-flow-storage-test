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
def task1():
    logger = prefect.context.get('logger')
    var = 5
    logger.info(f'value: {var}')
    return var
    

@task
def task2(arg):
    logger = prefect.context.get('logger')
    var = arg
    logger.info(f'value: {var}')

@task
def task3():
    logger = prefect.context.get('logger')
    raise ValueError('Oops, something went wrong.')
    logger.error('will this print? If so, something went wrong')

with Flow('Container Dependency Test') as flow:
    res1 = task1()
    task2(res1)
    task3()

flow.run_config = KubernetesRun(
    image='dprester/kubedask_run_base'
)
flow.run_config = DaskExecutor(
    cluster_class=get_kube_dask_cluster,
    cluster_kwargs={
        'image': 'dprester/dask_worker_base2'
    },
    adapt_kwargs={
        'maximum': 10
    }
)
flow.storage = GitHub(
    repo='dominikprester/prefect-flow-storage-test',
    path='flows/container_dependency_test.py'
)