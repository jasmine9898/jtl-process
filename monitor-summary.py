#!/bin/python
# coding=utf-8
import argparse
import copy
import os
import sys
import dask.dataframe as dd
import pandas as pd
from PyPDF2 import PdfFileReader, PdfFileWriter
import matplotlib

matplotlib.use('Agg')
from matplotlib import pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


def getJmeterData(files_List):
    hostJmeterData = {}
    for filepath in files_List:
        filepathArr = filepath.split('/')
        filename = filepathArr[len(filepathArr) - 1]
        fileNameArr = filename.split('-')
        host = fileNameArr[len(fileNameArr) - 1].replace('.jtl', '')
        agentVersion = fileNameArr[1]
        dtype_dic = {'timeStamp': pd.np.uint64,
                     'responseCode': str,
                     'success': str,
                     'elapsed': pd.np.float32,
                     'Connect': pd.np.float32,
                     'URL': str}
        #                      'Latency': pd.np.float32
        jtl_df = dd.read_csv(filepath, encoding='utf-8', sep=',',
                             usecols=['timeStamp', 'responseCode', 'success',
                                      'elapsed', 'Connect', 'URL'],
                             dtype=dtype_dic
                             ).compute()
        jtl_df = jtl_df[jtl_df['URL'].notnull()]
        jtl_df['timeStamp'] = jtl_df['timeStamp'] // 1000
        df_error = jtl_df[jtl_df['success'] == 'false']
        error_tps_series = df_error.groupby(['timeStamp']).size()
        df_request = jtl_df[jtl_df['responseCode'] == '200']
        request_tps_series = df_request.groupby(['timeStamp']).size()
        df_success = jtl_df[jtl_df['success'] == 'true']
        success_tps_series = df_success.groupby(['timeStamp']).size()
        df_new = jtl_df.groupby(['timeStamp']).agg(
            {'elapsed': 'mean', 'Connect': 'mean'}) \
            .join(success_tps_series.to_frame(name='success_tps')).fillna(0) \
            .join(error_tps_series.to_frame(name='error_tps')).fillna(0) \
            .join(request_tps_series.to_frame(name='request_tps')).fillna(0)
        jmeterData = {}
        jmeterData[agentVersion] = df_new
        if hostJmeterData.has_key(host):
            hostJmeterData[host].update(jmeterData)
        else:
            hostJmeterData[host] = jmeterData
    return hostJmeterData


def getMetricData(files_List):
    hostMonitorData = {}

    for filepath in files_List:
        filepathArr = filepath.split('/')
        filename = filepathArr[len(filepathArr) - 1]
        fileNameArr = filename.split('-')
        host = fileNameArr[len(fileNameArr) - 1].replace('.csv', '')
        agentVersion = fileNameArr[1]
        monitorData = {}
        monitorData[agentVersion] = pd.read_csv(filepath)
        if hostMonitorData.__len__() != 0 and hostMonitorData.__contains__(host):
            hostMonitorData[host].update(monitorData)
        else:
            hostMonitorData[host] = monitorData
    return hostMonitorData


def metricStatic(hostData):
    static_host = {}
    for host in hostData:
        agentMetric = hostData.get(host)
        static_metric = {}
        for agent in agentMetric:
            metricData = agentMetric.get(agent)
            if isinstance(metricData, dd.DataFrame):
                metricData = metricData.compute()

            for (label, content) in metricData.items():
                if label.startswith('timeStamp') or label.startswith('Unnamed') or label.startswith('date'):
                    continue
                scope_start = content.index[0] + 300
                series = pd.Series(content.index)
                scope = series[series > int(scope_start)]
                average = content[scope].mean()
                median = pd.np.median(content[scope])
                std = content[scope].std()
                static_df = pd.DataFrame([[average, median, std]], columns=['average', 'median', 'std'],
                                         index=[agent]).round(2)
                if static_metric.has_key(label):
                    df = static_metric.get(label)
                    static_metric.update({label: df.append(static_df)})
                else:
                    static_metric.update({label: static_df})
        static_host[host] = static_metric
    return static_host


def plotAndTable(static_data, data_dict, pdfpage):
    hostIndex = 0
    for host in data_dict:
        static_host = static_data.get(host)
        fig = plt.figure(num=hostIndex)
        fig, axs = plt.subplots(len(static_host))  # 7metric
        fig.set_figheight(6 * len(static_host))
        fig.suptitle(host, fontsize=16, y=0.9)
        # 子图间距动态为0.5倍 表行数
        table_rowCount = data_dict[host].__len__() + 1

        plt.subplots_adjust(hspace=0.1 * table_rowCount + 0.4)

        agentIndex = 0
        for agentversion in data_dict[host]:
            agentIndex = agentIndex + 1
            metricData = data_dict[host][agentversion]

            metricIndex = 0
            if isinstance(metricData, dd.DataFrame):
                metricData = metricData.compute()

            for (label, content) in metricData.items():
                if label.startswith('Unnamed') or label.startswith('date') or label.startswith(
                        'timeStamp'):
                    continue
                # 取index content.index[0]
                x = range(content.__len__())
                y = content
                axs[metricIndex].plot(x, y, label=agentversion, linewidth=0.5)
                # axs[r].legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)
                axs[metricIndex].legend(loc='best')
                # 图例中线条粗细
                leg = axs[metricIndex].legend()
                for line in leg.get_lines():
                    line.set_linewidth(4.0)
                axs[metricIndex].set_title(label, y=1)
                static_agent = static_host.get(label)
                ccolors = plt.cm.BuPu(pd.np.full(len(static_agent.columns), 0.2))
                # 表的bbox 为左起始 0.1,从最底部的0.1倍行高处起，宽0.9，高0-0.1倍行高
                bbox_bottom = 0 - 0.1 * table_rowCount - 0.1
                bbox_hight = 0.1 * table_rowCount
                axs[metricIndex].table(cellText=static_agent.values,
                                       rowLabels=static_agent.index,
                                       colLabels=static_agent.columns,
                                       # cellLoc='left', rowLoc='center',
                                       colColours=ccolors,
                                       bbox=[0.1, bbox_bottom, 0.9, bbox_hight])
                # 不显示x轴
                # axs[metricIndex].set_xticklabels([])
                metricIndex = metricIndex + 1
        fig.savefig(pdfpage, format='pdf', bbox_inches="tight", pad_inches=1)
        hostIndex = hostIndex + 1


def static_plot(origin_static_data, pdfpage):
    static_data = copy.deepcopy(origin_static_data)
    hostIndex = 0

    for host in static_data:
        static_metric = static_data.get(host)
        widthList = []
        for x in static_metric.values()[0].index:
            widthList.append(len(x))

        labelWidthList = []
        for x in static_metric.keys():
            labelWidthList.append(len(x))

        fig, axs = plt.subplots()
        maxWidth = pd.np.sum(widthList, axis=0) + max(labelWidthList)
        maxWidth = 8 if maxWidth <= 80 else maxWidth / 10

        fig.set_figwidth(maxWidth)

        axs.axis("off")
        fig.suptitle(host, fontsize=16, y=0.9)
        ccolors = plt.cm.BuPu(pd.np.full(len(static_metric.values()[0].index), 0.2))
        for metric in static_metric.items():
            average_series = metric[1]['average']
            static_metric[metric[0]] = average_series

        table = axs.table(cellText=static_metric.values(),
                          rowLabels=static_metric.keys(),
                          colLabels=static_metric.values()[0].index,
                          colColours=ccolors,
                          bbox=[0, 0, 1, 0.8])
        table.auto_set_font_size(False)

        cellDict = table.get_celld()
        for i in range(0, len(widthList)):
            for j in range(0, len(static_metric.keys()) + 1):
                width = 8 if widthList[i] < 8 else widthList[i]
                cellDict[(j, i)].set_width(0.1 * width)

        # table.auto_set_column_width(col=list(range(len(static_metric.values()[0].index))))
        fig.savefig(pdfpage, format='pdf', bbox_inches="tight", pad_inches=1)

        hostIndex = hostIndex + 1


def plotAll(data_dict, pdfpage):
    staticData = metricStatic(data_dict)
    static_plot(staticData, pdfpage)
    plotAndTable(staticData, data_dict, pdfpage)


def mergeGCpdf(gc_report_pdf, report_pdf_file):
    try:
        infile = PdfFileReader(report_pdf_file, 'rb')
        infile2 = PdfFileReader(gc_report_pdf, 'rb')
        output = PdfFileWriter()

        for i in xrange(infile.getNumPages()):
            p = infile.getPage(i)
            output.addPage(p)
            if i == infile.getNumPages() - 1:
                for j in xrange(infile2.getNumPages()):
                    p2 = infile2.getPage(j)
                    output.addPage(p2)

        with open(report_pdf_file, 'wb+') as f:
            output.write(f)
    except Exception:
        print "mergeGCpdf failed"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--origin_data_path", dest="origin_data", type=str, default="/Users/peng/Desktop")
    parser.add_argument("--output_pdf", dest="output_pdf", type=str, default="./")
    args = parser.parse_args()
    path = args.origin_data
    output_pdf = args.output_pdf

    if not path.endswith("/"):
        path = path + "/"

    jmeter_file_List = []
    monitor_file_List = []
    for file in os.listdir(path):
        # filename:  jmeter-3.5.1-10.128.1.4.jtl
        if file.startswith('jmeter') and file.endswith(".jtl"):
            jmeter_file_List.append(path + file)
        # filename:  monitor-3.5.1-10.128.1.4.csv
        elif file.startswith('monitor') and file.endswith(".csv"):
            monitor_file_List.append(path + file)

    createTime = pd.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    report_pdf = output_pdf + 'Result_' + createTime + '.pdf'
    pp = PdfPages(report_pdf)

    if jmeter_file_List.__len__() > 0:
        hostJmeterData = getJmeterData(jmeter_file_List)
        if hostJmeterData.__len__() > 0:
            plotAll(hostJmeterData, pp)
    if monitor_file_List.__len__() > 0:
        hostMonitorData = getMetricData(monitor_file_List)
        if hostMonitorData.__len__() > 0:
            plotAll(hostMonitorData, pp)
    plt.cla()
    plt.close()
    pp.close()
    # 合并gc report
    for name in os.listdir(path):
        if name.__contains__('Result_gc'):
            mergeGCpdf(path + name, report_pdf)


if __name__ == '__main__':
    sys.exit(main())
