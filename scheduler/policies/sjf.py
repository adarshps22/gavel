import os, sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

import copy
import random

import job_id_pair
from policy import Policy, PolicyWithPacking

class SJFPolicy(Policy):
    def __init__(self, mode='base'):
        self._name = 'SJF'
        self._mode = mode
        self._allocation = {}

    def get_max_gpu(self, job_throughputs):
        max_val = max(job_throughputs.values())
        max_gpu = [k for k, v in job_throughputs.items() if v == max_val][0]
        return max_val, max_gpu

    def get_allocation(self, throughputs, cluster_spec):
        # Sort the jobs based on their scheduled status and the inverse of the highest GPU value (throughput)
        sorted_job_ids = sorted(throughputs, key=lambda x: 1/self.get_max_gpu(throughputs[x])[0])

        # Assign the best available GPU for each job
        assignment = {}
        available_gpus = copy.deepcopy(cluster_spec)

        for job_id in sorted_job_ids:
            job_throughputs = throughputs[job_id]
            for gpu, throughput in sorted(job_throughputs.items(), key=lambda x: x[1], reverse=True):
                if available_gpus[gpu] > 0:
                    assignment[job_id] = {gpu_type: 1.0 if gpu_type == gpu else 0.0 for gpu_type in cluster_spec}
                    available_gpus[gpu] -= 1
                    break

        return assignment