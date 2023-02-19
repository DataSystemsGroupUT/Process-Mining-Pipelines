import time
from dask import compute
def useExecutionTime(func):
    def compute(*args, **kwargs):
        begin = time.time()

        result = func(*args, **kwargs)

        end = time.time()

        return {"result": result, "execution_time": end - begin}

    return compute


@useExecutionTime
def getComputeTime(*args, **kwargs):
    return compute(*args, **kwargs)