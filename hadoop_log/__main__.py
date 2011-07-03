import urllib
import re
import argparse
import os
from BeautifulSoup import BeautifulSoup


def read_jobs(base_url):
    a = BeautifulSoup(urllib.urlopen(base_url + 'jobtracker.jsp').read())
    links = [dict(x.attrs)['href'] for x in a('a') if '?jobid=' in dict(x.attrs)['href']]
    # Each is of the form:  jobdetails.jsp?jobid=job_201102271846_7624&refresh=30
    jobs_re = re.compile('jobid=job_([0-9]+)_([0-9]+)')
    jobs = sorted([map(int, jobs_re.search(l).groups()) for l in links], reverse=True)
    return jobs


def read_recent_job(base_url):
    return read_jobs(base_url)[0]


def read_task_urls(base_url, start_time, job_num):
    # URL of the form:  http://blah:50030/
    out = []
    for task_type in ['m', 'r']:
        task_num = 0
        while 1:
            a = BeautifulSoup(urllib.urlopen(base_url + 'taskdetails.jsp?jobid=job_%d_%.4d&tipid=task_%d_%.4d_%s_%.6d' % (start_time, job_num,
                                                                                                                          start_time, job_num,
                                                                                                                          task_type, task_num)).read())
            if 'No Task Attempts found' in str(a):
                break
            out.append(dict(a.find(text='All').parent.attrs)['href'])
            task_num += 1
    return out


def read_stderr(task_urls):
    out = []
    for task_url in task_urls:
        b = BeautifulSoup(urllib.urlopen(task_url).read(), convertEntities=BeautifulSoup.HTML_ENTITIES)
        out.append('StdErr for [%s]\n%s' % (task_url, b('pre')[1].text))
    return '\n'.join(out)


def main(base_url, job_num):
    if not base_url:
        try:
            base_url = os.environ['HADOOP_JOB_TRACKER_URL']
        except KeyError:
            raise ValueError('Either specify HADOOP_JOB_TRACKER_URL or --base_url')
    if not base_url[-1] == '/':
        base_url += '/'
    start_time, new_job_num = read_recent_job(base_url)
    if job_num is None:
        job_num = new_job_num
    print(read_stderr(read_task_urls(base_url, start_time, job_num)))


def _parser():
    parser = argparse.ArgumentParser(description='Scrape the Hadoop JobTracker for StdErr')
    parser.add_argument('--job_num', type=int, default=None,
                        help='Job number, if not specified then use most recent (default)')
    parser.add_argument('--base_url', default=None,
                        help='Url to the server (e.g., http://blah.com:50030/), if not specified use the HADOOP_JOB_TRACKER_URL environmental variable')
    return parser.parse_args()

if __name__ == '__main__':
    args = _parser()
    a = main(args.base_url, args.job_num)
