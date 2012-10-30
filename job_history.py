import sys
import os
import re
import time
import threading
from util import *


DayPathRegex = re.compile(r'((19|20)\d\d)/?(0[1-9]|10|11|12)/?([0][1-9]|[12][0-9]|30|31)')
def parse_day_from_path(path):
    m = DayPathRegex.search(path)
    if m:
        return '%s-%s-%s' % (m.group(1), m.group(3), m.group(4))
    return None

JobIDInFileNameRegex = re.compile(r'_job_\d+_\d+_')
def parse_jobid_from_filename(f):
    if f.endswith('.xml') or f.endswith('.crc'):
        return None
    m = JobIDInFileNameRegex.search(f)
    if m:
        return m.group(0)[1:-1]
    return None

JobBriefInfoProperties = ('date', 'jobname', 'jobid', 'user', 'job_priority', 'job_status', 'total_maps', 'total_reduces', 'failed_maps', 'failed_reduces', 'submit_time', 'launch_time', 'finish_time', 'hdfs_bytes_read', 'hdfs_bytes_written', 'map_output_bytes')

class HistoryRepo(Record):
    def __init__(self, path, scan=False):
        Record.__init__(self, ["path", "days", "status", "last_scan"])
        self.path = path
        self.days = {}
        self._scan_lock = threading.RLock()
        self.status = "IDLE"
        self.last_scan = 0
        if scan:
            self.scan()
    def update(self):
        return self.scan()
    def scan(self):
        if self.status == "SCANNING" or self.last_scan + 5 > time.time():
            return False
        with self._scan_lock:
            if self.last_scan + 5 > time.time():
                return False
            self.status = "SCANNING"
            for root, dirs, files in os.walk(self.path):
                datestr = parse_day_from_path(root) or 'default'
                for f in files:
                    jobid = parse_jobid_from_filename(f)
                    if not jobid:
                        continue
                    day = self.days.get(datestr)
                    if day == None:
                        day = Day(datestr)
                        self.days[datestr] = day
                    jh = day.jobs.get(jobid)
                    if jh == None:
                        jh = JobHistory(jobid, os.path.join(root, f))
                        day.jobs[jobid] = jh
            self.status = "IDLE"
            self.last_scan = time.time()
            return True
    def get_brief_info_table(self):
        data = []
        for day in sorted(self.days.iterkeys()):
            data.extend(self.days[day].get_brief_info_table()["data"])
        return {"columns": JobBriefInfoProperties, "data": data}
    def query_date_range(self, start_date, end_date):
        data = []
        for k in sorted(self.days.iterkeys()):
            if k == 'default':
                day = self.days[k]
                jobids = sorted(day.jobs.iterkeys())
                for jobid in jobids:
                    d = day.jobs[jobid].get_date()
                    if ((not start_date) or d >= start_date) and ((not end_date) or d < end_date):
                        bi = day.jobs[jobid].get_brief_info()
                        data.append(bi)
            else:
                if k >= start_date and k < end_date:
                    data.extend(self.days[k].get_brief_info_table()['data'])
        return {"columns": JobBriefInfoProperties, "data": data}

class Day(Record):
    def __init__(self, day):
        Record.__init__(self, ["day", "jobs"])
        self.day = day
        self.jobs = {}
    def get_brief_info_table(self):
        jobids = sorted(self.jobs.iterkeys())
        data = [self.jobs[jid].get_brief_info() for jid in jobids]
        return {"columns": JobBriefInfoProperties, "data": data}

KeyValuePairRegex = re.compile(r'(\w+)="([^"]*(\\"[^"]*)*)"')
def parse_history_record(line):
    ret = {}
    first_space = line.find(' ')
    ret['_TYPE_'] = line[:first_space]
    for m in KeyValuePairRegex.finditer(line[first_space+1:]):
        ret[m.group(1)] = m.group(2)
    return ret


CounterFilter = ('HDFS_BYTES_READ', 'HDFS_BYTES_WRITTEN', 'MAP_OUTPUT_BYTES')
CounterRegex = re.compile(r'\[\(([\w_]+)\)\([^\)]+\)\((\d+)\)\]')
def parse_counters(s):
    ret = {}
    for m in CounterRegex.finditer(s):
        if (not CounterFilter) or (m.group(1) in CounterFilter):
            ret[m.group(1).lower()] = int(m.group(2))
    return ret

def update_job(job, record):
    for k,v in record.iteritems():
        if k in ('JOBID', 'USER', 'JOB_STATUS', 'JOB_PRIORITY'):
            job[k.lower()] = v
        elif k == 'JOBNAME':
            job['jobname'] = v.decode('string-escape')
        elif k in ('SUBMIT_TIME', 
                   'LAUNCH_TIME', 
                   'FINISH_TIME'):
            job[k.lower()] = int(v)/1000
        elif k in ('TOTAL_MAPS', 
                   'TOTAL_REDUCES', 
                   'FINISHED_MAPS', 
                   'FINISHED_REDUCES', 
                   'FAILED_MAPS', 
                   'FAILED_REDUCES'):
            job[k.lower()] = int(v)
        elif k == 'COUNTERS':
            for ck,cv in parse_counters(v).iteritems():
                job[ck] = cv
    if job.get('submit_time'):
        job['date'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(job['submit_time']))

def update_task(task, record):
    pass

def simplify(id):
    return '_'.join(id.split('_')[3:])

def update_attempt(attempt, record):
    for k,v in record.iteritems():
        if k == 'TASK_ATTEMPT_ID':
            attempt['id'] = v
            fds = v.split('_')
            attempt['task_index'] = int(fds[-2])
            attempt['attempt_index'] = int(fds[-1])
        elif k == 'TASK_STATUS':
            attempt['status'] = v
        elif k in ('TASK_TYPE', 'HOSTNAME'):
            attempt[k.lower()] = v
        elif k in ('START_TIME', 'FINISH_TIME', 'SHUFFLE_FINISHED', 'SORT_FINISHED'):
            attempt[k.lower()] = int(v)/1000
        elif k == 'COUNTERS':
            for ck,cv in parse_counters(v).iteritems():
                attempt[ck] = cv

def parse_history(fin, shallow=False):
    ret = {}
    ret['tasks'] = {}
    ret['attempts'] = {}
    for line in fin:
        record = parse_history_record(line)
        rtype = record['_TYPE_']
        if rtype == 'META':
            continue
        elif rtype == 'Job':
            update_job(ret, record)
            if shallow and ret.get('launch_time'):
                return ret
        elif rtype == "Task":
            taskid = simplify(record['TASKID'])
            task = ret['tasks'].get(taskid)
            if not task:
                task = {}
                ret['tasks'][taskid] = task
            update_task(task, record)
        elif rtype in ("MapAttempt", "ReduceAttempt"):
            aid = simplify(record['TASK_ATTEMPT_ID'])
            attempt = ret['attempts'].get(aid)
            if not attempt:
                attempt = {}
                ret['attempts'][aid] = attempt
            update_attempt(attempt, record)
        else:
            pass
    return ret

class JobHistory(Record):
    cache = {}
    cache_max_csize = 2048
    @staticmethod
    def update_cache():
        c = JobHistory.cache
        size = JobHistory.cache_max_csize
        if len(c) <= size:
            return
        hs = [ (v, k) for k,v in c.iteritems() ]
        hs.sort()
        # empty 1/4 cache
        for i in range(len(c)-size/4*3):
            jh = c.get(hs[i][1])
            if jh:
                with jh._load_lock:
                    jh._history = None
    def __init__(self, jobid, path):
        Record.__init__(self, ["jobid", "path", "history"])
        self.jobid = jobid
        self.path = path
        self._history = None
        self._load_lock = threading.RLock()
        self._brief_info = None
        self._date = None
        self._name = None
    def get_history(self):
        ret = self._history
        if not ret:
            with self._load_lock:
                if not self._history:
                    self.load()
                    ret = self._history
        JobHistory.cache[self.jobid] = time.time()
        return ret
    def load(self):
        JobHistory.update_cache()
        fin = open(self.path)
        self._history = parse_history(fin)
        self._brief_info = [self._history.get(k) for k in JobBriefInfoProperties]
        self._date = self._history['date']
        self._name = self._history['jobname']
        fin.close()
    def get_brief_info(self):
        if not self._brief_info:
            with self._load_lock:
                if not self._brief_info:
                    self.load()
        return self._brief_info
    def shallow_load(self):
        fin = open(self.path)
        ret = parse_history(fin, True)
        self._date = ret['date']
        self._name = ret['jobname']
        fin.close()
    def get_date(self):
        if not self._date:
            self.shallow_load()
        return self._date
    def get_name(self):
        if not self._name:
            self.shallow_load()
        return self._name


def gen_brief_report(repopath, opath, start_date=None, end_date=None):
    hr = HistoryRepo(repopath, True)
    table = hr.query_date_range(start_date, end_date)
    if opath.endswith(".json"):
        of = open(opath, 'w')
        dump_json(table, of)
        of.close()
    elif opath.endswith(".csv"):
        of = open(opath, 'w')
        table_to_csv(table, of)
        of.close()
    else:
        raise Exception("Output format not support %s" % opath)
    return table


# As a global history repo module
_default_repo = None

def init_repo(path):
    print "Start to init repo"
    global _default_repo
    _default_repo = HistoryRepo(path, True)
    print "Repo initialized"
    
def get_repo():
    return _default_repo

if __name__ == "__main__":
    # generating csv reports for a date range
    if len(sys.argv) < 5:
        print "Usage: %s <repo path> <output path *.json|*.csv> <start_date> <end_date>" % sys.argv[0]
        print "Example: python %s ./logs/history jobs-2012-08-08.csv 2012-08-08 2012-0809" % sys.argv[0]
        print "         python %s ./logs/history jobs-2012-08-08.json 2012-08-08 None" % sys.argv[0]
        sys.exit(1)
    start_date = sys.argv[3]
    if start_date == "None":
        start_date = None
    end_date = sys.argv[4]
    if end_date == "None":
        end_date = None
    table = gen_brief_report(sys.argv[1], sys.argv[2], start_date, end_date)
    print "Finished, total %d jobs" % len(table['data'])



