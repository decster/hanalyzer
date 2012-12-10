#!/usr/bin/env python

#
# Hadoop job history analyzer
#
# This script can parse hadoop job history log files, and generate csv/json 
# reports, basically you can use it to get execution times, counters, status 
# of jobs, statistics about tasks and attempts, etc.
#
# This module can be used as a module to parse job history repository, or used 
# as a utility to generate job statistics report to get brief information about 
# the whole cluster.
#

import sys
import os
import re
import time
import threading
from optparse import OptionParser
from util import *

DayPathRegex = re.compile(r'((19|20)\d\d)/?(0[1-9]|10|11|12)/?([0][1-9]|[12][0-9]|30|31)')
def parse_day_from_path(path):
    m = DayPathRegex.search(path)
    if m:
        return '%s-%s-%s' % (m.group(1), m.group(3), m.group(4))
    return None

JobIDInFileNameRegex = re.compile(r'job_(\d+)_(\d+)_')
def parse_jobid_from_filename(f):
    if f.endswith('.xml') or f.endswith('.crc'):
        return None
    m = JobIDInFileNameRegex.search(f)
    if m:
        return m.group(0)[1:-1]
    return None

def parse_jobid_from_filename2(f):
    if f.endswith('.xml') or f.endswith('.crc'):
        return None
    m = JobIDInFileNameRegex.search(f)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    return None

JobBriefInfoProperties = ('date',
                          'jobname', 
                          'jobid', 
                          'user', 
                          'job_priority', 
                          'job_status', 
                          'total_maps', 
                          'total_reduces', 
                          'failed_maps', 
                          'failed_reduces', 
                          'submit_time', 
                          'launch_time', 
                          'finish_time', 
                          'map_avg_time', 
                          'reduce_avg_time', 
                          'hdfs_bytes_read', 
                          'hdfs_bytes_written', 
                          'map_output_bytes')


class BriefReportVistor(object):
    def __init__(self, start_date, end_date, scanOnly):
        self.start_date = start_date
        self.end_date = end_date
        self.scanOnly = scanOnly
    def accept(self, dt):
        return (self.start_date and dt >= self.start_date) and (self.end_date and dt < self.end_date)
    def onColumns(self, columns):
        pass
    def onStats(self, stats):
        pass
    def onRecord(self, record):
        pass
    def onEnd(self):
        pass

def get_job_date(jobfile):
    try:
        f = open(jobfile, "r")
        job = parse_history(f, True)
        f.close()
        return time.strftime('%Y-%m-%d', time.localtime(job['submit_time']))
    except:
        print "wrong job file: %s" % jobfile
        raise

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
            counter = 0
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
                    counter += 1
            self.status = "IDLE"
            self.last_scan = time.time()
            return True
    def get_brief_info_table(self):
        data = []
        for day in sorted(self.days.iterkeys()):
            data.extend(self.days[day].get_brief_info_table()["data"])
        return {"columns": JobBriefInfoProperties, "data": data}
    def query_date_range(self, start_date, end_date, progress=None):
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
                if ((not start_date) or k >= start_date) and ((not end_date) or k < end_date):
                    data.extend(self.days[k].get_brief_info_table()['data'])
                    if callable(progress):
                        progress(data, k)
        return {"columns": JobBriefInfoProperties, "data": data}
    def get_date_range_stats(self, start_date, end_date):
        total = 0
        unknown = 0
        count = 0
        mindate = None
        maxdate = None
        for k in sorted(self.days.iterkeys()):
            if k == 'default':
                n = len(self.days[k].jobs)
                total += n
                unknown += n
            else:
                if (mindate == None) or (k < mindate):
                    mindate = k
                if (maxdate == None) or (k > maxdate):
                    maxdate = k
                n = len(self.days[k].jobs)
                total += n
                count += n
        return {'total': total, 'unknown': unknown, 'count': count, 'mindate': mindate, 'maxdate':maxdate}
    @staticmethod
    def process_brief_report_nested(repopath, visitor):
        repo = HistoryRepo(repopath, True)
        start_date = visitor.start_date
        end_date = visitor.end_date
        visitor.onStats(repo.get_date_range_stats(visitor.start_date, visitor.end_date))
        if visitor.scanOnly:
            return
        visitor.onColumns(JobBriefInfoProperties)
        for k in sorted(repo.days.iterkeys()):
            if k == 'default':
                day = repo.days[k]
                jobids = sorted(day.jobs.iterkeys())
                for jobid in jobids:
                    d = day.jobs[jobid].get_date()
                    if ((not start_date) or d >= start_date) and ((not end_date) or d < end_date):
                        bi = day.jobs[jobid].get_brief_info()
                        visitor.onRecord(bi)
            else:
                if ((not start_date) or k >= start_date) and ((not end_date) or k < end_date):
                    for r in repo.days[k].get_brief_info_table()['data']:
                        visitor.onRecord(r)
        visitor.onEnd()
    @staticmethod
    def process_brief_report_flat(repopath, visitor):
        # dealing with a monster directory with ~millions of log files
        # using binary search to locate logs in date range 
        start_date = visitor.start_date
        end_date = visitor.end_date
        files = os.listdir(repopath)
        logs = []
        if repopath[-1] != '/':
            repopath = repopath + '/'
        for f in files:
            jobid = parse_jobid_from_filename2(f)
            if not jobid:
                continue
            if os.stat(repopath+f).st_size < 100:
                continue
            logs.append((jobid, f))
        logs.sort()
        lo = 0
        hi = len(logs)
        if start_date:
            while lo < hi:
                mid = (lo+hi)//2
                if get_job_date(repopath + logs[mid][1]) < start_date: 
                    lo = mid+1
                else:
                    hi = mid
        start_idx = lo
        end_idx = len(logs)
        if end_date:
            lo = 0
            hi = len(logs)
            while lo < hi:
                mid = (lo+hi)//2
                cdate = get_job_date(repopath + logs[mid][1])
                if end_date < cdate:
                    hi = mid
                else:
                    lo = mid+1
            end_idx = lo
        mindate = start_date
        if start_idx < len(logs) and start_idx>=0:
            mindate = get_job_date(repopath + logs[start_idx][1])
        maxdate = start_date
        if (end_idx-1) < len(logs) and (end_idx-1)>=0:
            maxdate = get_job_date(repopath + logs[end_idx-1][1])
        total = end_idx - start_idx
        print "Get date range (%s:%s - %s:%s)" % ('None' if start_idx==len(logs) else logs[start_idx][0], 
                                                  'None' if start_idx==len(logs) else logs[start_idx][1],
                                                  'None' if end_idx==len(logs) else logs[end_idx][0],
                                                  'None' if end_idx==len(logs) else logs[end_idx][1])
        visitor.onStats({'total': len(logs), 'unknown': 0, 'count': total, 'mindate': mindate, 'maxdate':maxdate})
        if visitor.scanOnly:
            return
        visitor.onColumns(JobBriefInfoProperties)
        for logpair in logs[start_idx:end_idx]:
            f = open(repopath + logpair[1], "r")
            job = None
            try:
                job = parse_history(f, False)
            except Exception as e:
                print "Exception %s when parse %s" % (str(e), logpair[1])
            f.close()
            if job == None:
                continue
            bi = [job.get(k) for k in JobBriefInfoProperties]
            visitor.onRecord(bi)
        visitor.onEnd()

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
            job[k.lower()] = int(round(float(v)/1000))
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
            attempt[k.lower()] = int(round(float(v)/1000))
        elif k == 'COUNTERS':
            for ck,cv in parse_counters(v).iteritems():
                attempt[ck] = cv

def parse_history(fin, shallow=False):
    ret = {}
    ret['tasks'] = {}
    ret['attempts'] = {}
    # possible not in history if killed
    ret['failed_maps'] = 0
    ret['failed_reduces'] = 0
    for line in fin:
        record = parse_history_record(line)
        rtype = record['_TYPE_']
        if rtype == 'META':
            continue
        elif rtype == 'Job':
            update_job(ret, record)
            if shallow and ret.get('submit_time'):
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
    # get avg successful task attempt time
    mc = 0
    mt = 0
    rc = 0
    rt = 0
    for atp in ret['attempts'].itervalues():
        if atp.get('status') == 'SUCCESS':
            if atp['task_type'] == 'MAP':
                mc += 1
                mt += (atp['finish_time'] - atp['start_time'])
            elif atp['task_type'] == 'REDUCE':
                rc += 1
                rt += (atp['finish_time'] - atp['start_time'])
    ret['map_avg_time'] = 0 if mc==0 else round(float(mt)/mc, 1)
    ret['reduce_avg_time'] = 0 if rc==0 else round(float(rt)/rc, 1)
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


class JsonReportVisitor(BriefReportVistor):
    def __init__(self, path, start_date, end_date, scanOnly):
        BriefReportVistor.__init__(self, start_date, end_date, scanOnly)
        self.path = path
        self.fout = None
        self.first = True
    def onColumns(self, columns):
        if self.fout == None:
            self.fout = open(self.path, 'w')
            self.fout.write('{\n  "columns": %s,\n  "data": [' % dumps_json(columns));
            self.fout.flush()
            self.first = True
    def onRecord(self, record):
        self.fout.write('%s\n      %s' % ("" if self.first else ",", dumps_json(record)));
        self.fout.flush()
        self.first = False
    def onEnd(self):
        self.fout.write('\n  ]\n}\n')
        self.fout.close()

class CsvReportVisitor(BriefReportVistor):
    def __init__(self, path, start_date, end_date, scanOnly):
        BriefReportVistor.__init__(self, start_date, end_date, scanOnly)
        self.path = path
        self.fout = None
    def onColumns(self, columns):
        if self.fout == None:
            self.fout = open(self.path, 'w')
            self.fout.write(",".join([to_csv_value(c) for c in columns]));
            self.fout.write("\n")
            self.fout.flush()
    def onRecord(self, record):
        self.fout.write(",".join([to_csv_value(c) for c in record]));
        self.fout.write("\n")
        self.fout.flush()
    def onEnd(self):
        self.fout.close()

class ProgressReportVisitor(BriefReportVistor):
    def __init__(self, output_type, output_prefix, period, start_date, end_date, scanOnly, update):
        BriefReportVistor.__init__(self, start_date, end_date, scanOnly)
        self.current = 0
        self.count = 0
        self.sub_visitors = []
        self.start_time = None
        self.end_time = None
        self.lp = None # last progress report
        self.output_json = True
        self.output_csv = True
        self.output_prefix = output_prefix
        self.update = update
        if output_type.lower() == "json":
            self.output_csv = False
        elif output_type.lower() == "csv":
            self.output_json = False
        self.period = period
        self.columns = None
    def _addSubVisitor(self, date_mark, start_date, end_date):
        print "Add date range %s -- %s" % (start_date, end_date)
        if self.output_json:
            output_path = self.output_prefix + date_mark + ".json"
            self.sub_visitors.append(JsonReportVisitor(output_path, start_date, end_date, self.scanOnly))
        if self.output_csv:
            output_path = self.output_prefix + date_mark + ".csv"
            self.sub_visitors.append(CsvReportVisitor(output_path, start_date, end_date, self.scanOnly))
    def onStats(self, stats):
        self.count = stats['count']
        if stats['unknown'] > 0:
            print "Progress may > 100% because unknown date history files exists"
        print "Total: %d Unknown: %d Needed: %d Date Range: %s-%s Estimated Time: %.0fs " % (
            stats['count'], stats['unknown'], stats['count'], stats['mindate'], stats['maxdate'], stats['count'] / 120.0)
        if self.period == None:
            date_mark = self.start_date if self.start_date else ""
            self._addSubVisitor(date_mark, self.start_date, self.end_date)
        else:
            date_ranges = get_date_ranges(stats['mindate'], stats['maxdate'], self.period)
            for dr in date_ranges:
                self._addSubVisitor(dr[0], dr[0], dr[1])
    def onColumns(self, columns):
        self.start_time = time.time()
        self.lp = self.start_time
        self.columns = columns
        for sv in self.sub_visitors:
            sv.onColumns(columns)
    def onRecord(self, record):
        dt = record[self.columns.index('date')]
        for sv in self.sub_visitors:
            if sv.accept(dt):
                sv.onRecord(record)
        self.current += 1
        ct = time.time()
        if self.lp + self.update < ct:
            print "Progress %.0f%%(%d/%d) Remain: %.1fs. Current parsing date: %s." % (
                self.current*100/float(self.count), 
                self.current, 
                self.count, 
                (ct - self.start_time) * (self.count - self.current) / self.current, 
                dt)
            self.lp = ct
    def onEnd(self):
        self.end_time = time.time()
        for sv in self.sub_visitors:
            sv.onEnd()
        print "Finished. Time: %.1fs. Parse Speed: %.0fjobs/s" % (
            self.end_time - self.start_time, self.count / (self.end_time - self.start_time))

# As a global history repo module
_default_repo = None

def init_repo(path):
    print "Start to init repo"
    global _default_repo
    _default_repo = HistoryRepo(path, True)
    print "Repo initialized"
    
def get_repo():
    return _default_repo

# As a tool to generator brief job history report
if __name__ == "__main__":
    # generating reports for a date range
    parser = OptionParser(usage="usage: %prog [options] <repo path>")
    parser.add_option('-t', '--type', dest="type", 
                  help="output file type, json|csv|all(output all formats) default: all", default='all')
    parser.add_option('-o', '--output', dest="output", 
                      help="output file prefix, default: jobstats", default='jobstats')
    parser.add_option('-s', '--start', dest="startDate", help="start date, e.g. 2012-09-09 default: None")
    parser.add_option('-e', '--end', dest="endDate", help="end date e.g. 2012-09-09 default: None")
    parser.add_option('-n', '--dry-run', dest="dryRun", action="store_true", 
                      help="just scan repo and estimate output size and running time", 
                      default=False)
    parser.add_option('-f', '--flat', dest="flat", action="store_true", 
                      help="history repo is old Hadoop 0.20 style flat directory, one monster dir with huge number of history files", 
                      default=False)
    parser.add_option('-p', '--period', dest="period", 
                      help="output one file with each period, day|week|month|none")
    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.print_help()
        parser.error("invalid repo path: %s" % str(args))
    period = None
    if options.period:
        if options.period.lower() in ('day', 'week', 'month'):
            period = options.period.lower()
        else:
            parser.print_help()
            parser.error("invaild argument period")
    repo_path = args[0]
    # update progress every 4 sec
    prv = ProgressReportVisitor(options.type, options.output, period, options.startDate, options.endDate, options.dryRun, 4.0);
    if options.flat:
        HistoryRepo.process_brief_report_flat(repo_path, prv)
    else:
        HistoryRepo.process_brief_report_nested(repo_path, prv)


