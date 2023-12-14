from concurrent.futures import ThreadPoolExecutor
from typing import Callable, List, Tuple

from streamlit.runtime.scriptrunner import add_script_run_ctx


# Run a list of jobs in parallel using multithreading. We want to be able to pass dedicated arguments to each job.
def run_multithreaded(jobs: List[Tuple[Callable, Tuple]], max_workers: int) -> List:
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(job[0], **job[1]) for job in jobs]
        for t in executor._threads:
            add_script_run_ctx(t)
    return [future.result() for future in futures]
