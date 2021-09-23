from prefect import task, Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GitHub

@task
def return_world():
    return "world"

@task
def hello(arg):
    print(f'hello {arg}')
    return f'hello {arg}'

with Flow('Hello World Flow') as flow:
    w = return_world()
    res = hello(w)

flow.run_config = KubernetesRun()
flow.storage = GitHub(
    repo='https://github.com/dominikprester/prefect-flow-storage-test',
    path='flows/hello_world.py'
)


