from functools import reduce
from collections import namedtuple
import heapq


def solution(queries):
    ans = []

    PromotionRequest = namedtuple("PromotionRequest", 'start_timestamp, new_compensation, new_position')
    WorkLog = namedtuple('WorkLog', 'entry exit active compensation')
    Worker = namedtuple('Worker', 'id position compensation worklogs time promotion_request')
    worker_map = {}

    def add_worker(query):
        worker_id, position, compensation = query
        compensation = int(compensation)

        if worker_id in worker_map:
            return 'false'
        worker = Worker(worker_id, position, compensation, worklogs=[], time=0, promotion_request=None)
        worker_map[worker_id] = worker
        return "true"

    def register(query):
        worker_id, timestamp = query
        timestamp = int(timestamp)
        if worker_id not in worker_map:
            return 'invalid_request'

        worker = worker_map[worker_id]
        worker = process_promotion(worker, timestamp)

        active_work_log = worker.worklogs.pop() if worker.worklogs and worker.worklogs[-1].active else None
        if active_work_log:
            active_work_log = active_work_log._replace(exit=timestamp, active=False)
        else:
            active_work_log = WorkLog(timestamp, float('inf'), True, worker.compensation)
        worker = log_work(worker, active_work_log)
        return 'registered'

    def log_work(worker, work_log):
        worker.worklogs.append(work_log)
        if work_log.exit != float('inf'):
            worker = worker._replace(time=work_log.exit - work_log.entry)
        worker_map[worker.id] = worker
        return worker

    def process_promotion(worker, timestamp):
        if worker.promotion_request and worker.promotion_request.start_timestamp <= timestamp:
            pr = worker.promotion_request
            active_work_log = worker.worklogs.pop() if worker.worklogs and worker.worklogs[-1].active else None
            if active_work_log:
                active_work_log._replace(exit=timestamp, active=False)
                worker = log_work(worker, active_work_log)
            worker = worker._replace(position=pr.new_position, compensation=pr.new_compensation, time=0,
                                     promotion_request=None)
            if active_work_log:
                worker = log_work(worker, WorkLog(timestamp, float('inf'), True, worker.compensation))
            worker_map[worker.id] = worker
        return worker

    def get(query):
        worker_id = query[0]
        if worker_id not in worker_map:
            return ''

        worker = worker_map[worker_id]
        logs = filter(lambda x: x.active == False, worker.worklogs)
        total_work = reduce(sum, map(lambda log: log.exit - log.entry, logs))
        return str(total_work)

    def top_n_workers(query):
        n, position = query
        n = int(n)
        workers = filter(lambda worker: worker.position == position, worker_map.values())
        top_workers = heapq.nlargest(n, workers, key=lambda worker: worker.time)
        return ', '.join(map(lambda x: x.id, top_workers))

    def promote(query):
        worker_id, new_position, new_compensation, start_timestamp = query
        start_timestamp = int(start_timestamp)
        new_compensation = int(new_compensation)

        if worker_id not in worker_map:
            return ''

        worker = worker_map[worker_id]

        if worker.promotion_request:
            return 'invalid_request'

        promotion_request = PromotionRequest(start_timestamp, new_compensation, new_position)
        worker = worker._replace(promotion_request=promotion_request)
        worker_map[worker_id] = worker
        return 'success'

    def calc_salary(query):
        workerId, startTimestamp, endTimestamp = query

        worker = worker_map[workerId]
        logs = filter(lambda x: x.entry <= startTimestamp and x.exit <= endTimestamp, worker.worklogs)
        salaries = map(lambda x: x.compensation * (min(x.exit, endTimestamp) - max(x.entry, startTimestamp)), logs)
        ans = reduce(sum, salaries)
        return ans

    for query in queries:
        op, *query = query
        res = locals()[op.lower()](query)
        print(op, query, res)
        ans.append(res)
    print(ans)


queries = [["ADD_WORKER", "John", "Junior Developer", "100"],
           ["ADD_WORKER", "Ashley", "Junior Developer", "100"],
           ["REGISTER", "John", "150"],
           ["REGISTER", "Ashley", "160"],
           ["REGISTER", "John", "200"],
           ["REGISTER", "Ashley", "220"],
           ["TOP_N_WORKERS", "10", "Junior Developer"],
           ["PROMOTE", "John", "Middle Developer", "200", "500"],
           ["REGISTER", "Ashley", "300"],
           ["REGISTER", "John", "310"],
           ["TOP_N_WORKERS", "10", "Junior Developer"],
           ["TOP_N_WORKERS", "10", "Middle Developer"],
           ["REGISTER", "Ashley", "500"],
           ["REGISTER", "John", "510"],
           ["TOP_N_WORKERS", "10", "Junior Developer"],
           ["TOP_N_WORKERS", "10", "Middle Developer"],
           ["REGISTER", "John", "600"],
           ["REGISTER", "John", "720"],
           ["TOP_N_WORKERS", "10", "Junior Developer"],
           ["TOP_N_WORKERS", "10", "Middle Developer"],
           ["PROMOTE", "John", "Senior Developer", "320", "800"],
           ["REGISTER", "John", "800"],
           ["TOP_N_WORKERS", "10", "Junior Developer"],
           ["TOP_N_WORKERS", "10", "Middle Developer"],
           ["TOP_N_WORKERS", "10", "Senior Developer"],
           ["REGISTER", "John", "877"],
           ["TOP_N_WORKERS", "10", "Middle Developer"],
           ["TOP_N_WORKERS", "10", "Senior Developer"]]
solution(queries)