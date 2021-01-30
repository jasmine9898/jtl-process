#!/bin/python
# coding=utf-8
import os
import sys
import time

import queue
import threading
import psutil
import argparse
import pandas as pd
from pathlib2 import Path
from datetime import datetime
import subprocess


class ProcessMetric:
    def __init__(self, pid):
        try:
            self.process = psutil.Process(pid)
            self.pid = pid
        except psutil.NoSuchProcess as e:
            print "The {} process is not exist".format(pid)
            exit()

    def getCPU(self):
        cpu = self.process.cpu_percent(interval=1) / psutil.cpu_count()
        return cpu

    def getMemory(self):
        memoryData = self.process.memory_full_info().uss if psutil.LINUX else self.process.memory_info().rss
        return memoryData

    def getHeapUsed(self):
        try:
            jstat = subprocess.Popen('jstat -gc %s |tail -1 | awk \'{print $3+$4+$6+$8}\'' % self.pid,
                                     stdout=subprocess.PIPE, shell=True)
            heapUsed = jstat.communicate()[0].replace('\n', '')
            return float(heapUsed)
        except Exception:
            return 0

    def getOldGenUsed(self):
        try:
            jstat = subprocess.Popen('jstat -gc %s |tail -1 | awk \'{print $8}\'' % self.pid,
                                     stdout=subprocess.PIPE, shell=True)
            oldGenUsed = jstat.communicate()[0].replace('\n', '')
            return float(oldGenUsed)
        except Exception:
            return 0


class SystemMetric:
    global system_cpu_queue

    def getLoadAverage(self):
        loadaverage = psutil.getloadavg()[0]
        return loadaverage

    def storeInQueue(f):
        def wrapper(*args):
            system_cpu_queue.put(f(*args))

        return wrapper

    @storeInQueue
    def getSystemCPU(self):
        cpu_percent = psutil.cpu_percent(interval=1)
        return cpu_percent

    def getNetworkSent(self):
        networkData_bytes_sent = psutil.net_io_counters().bytes_sent
        return networkData_bytes_sent

    def getNetworkRecv(self):
        networkData_bytes_recv = psutil.net_io_counters().bytes_recv
        return networkData_bytes_recv

    def getIp(self):
        for addr in psutil.net_if_addrs().itervalues():
            for snic in addr:
                if snic.address.startswith('10.') or snic.address.startswith('192.') or snic.address.startswith('172.'):
                    return snic.address


def saveData(data_dict, file_path):
    df = pd.DataFrame([data_dict])
    now = '{:.0f}'.format(round(time.time() * 1000))
    # now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df = df.set_index([pd.Index([now])])
    df.index.name = 'date'
    if Path(file_path).exists():
        df.to_csv(file_path, header=False, mode='a')
    else:
        df.to_csv(file_path, header=True, mode='w')


system_cpu_queue = queue.Queue()


def getDataAndSave(pid, outputFile):
    systemMetric = SystemMetric()
    processMetric = ProcessMetric(pid)

    # t = threading.Thread(target=systemMetric.getSystemCPU)
    # t.start()

    data = {'Process CPU Utilization Percentage': processMetric.getCPU(),
            'Memory Usage (MB)': processMetric.getMemory() / (1024 ** 2),
            'HeapUsed Usage (MB)': processMetric.getHeapUsed() / 1024,
            'OldGen Usage (MB)': processMetric.getOldGenUsed() / 1024,
            # 'System CPU Utilization Percentage': system_cpu_queue.get(),
            'Load Average 1Min': systemMetric.getLoadAverage(),
            'Network Sent (MB)': systemMetric.getNetworkSent(),
            'Network Received (MB)': systemMetric.getNetworkRecv()
            }
    saveData(data, outputFile)


def finalProcessData(outputFile):
    df = pd.read_csv(outputFile)
    input = df['Network Sent (MB)'].diff().fillna(0)
    output = df['Network Received (MB)'].diff().fillna(0)
    duration = df['date'].diff().fillna(1)
    input_new = pd.Series(input / duration * 1000 / (1024 ** 2)).rename('Network Sent (MB)')
    df.update(input_new)
    output_new = pd.Series(output / duration * 1000 / (1024 ** 2)).rename('Network Received (MB)')
    df.update(output_new)
    df.round(2).to_csv(outputFile, header=True, mode='w+')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pid", dest="pid", type=int)
    parser.add_argument("--duration", dest="duration", type=int, default=10)
    parser.add_argument('--output_file', dest='outputFile', help='The output file', default='./test.csv')
    args = parser.parse_args()
    pid = args.pid
    duration = args.duration
    outputFile = args.outputFile

    systemMetric = SystemMetric()
    ip = systemMetric.getIp()
    if not outputFile.__contains__(ip):
        if outputFile.endswith('.csv'):
            outputFile = outputFile.replace('.csv', '-' + ip + '.csv')
        else:
            outputFile = outputFile + '-' + ip + '.csv'
    if Path(outputFile).exists():
        os.remove(outputFile)
    start_time = datetime.now()
    while (datetime.now() - start_time).seconds < duration:
        getDataAndSave(pid, outputFile)
    finalProcessData(outputFile)
    print 'save monitor data to' + outputFile


if __name__ == '__main__':
    sys.exit(main())
