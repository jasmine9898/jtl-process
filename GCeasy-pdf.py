# coding=utf-8
import argparse
import requests
import json
import pandas as pd
import os
import sys
import matplotlib

matplotlib.use('Agg')
import threading

from matplotlib import pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.font_manager import FontProperties
import time

parser = argparse.ArgumentParser()
parser.add_argument("--gcfilepath", dest="gcfilepath", default="/home/gclogs")
parser.add_argument("--pdfpath", dest="pdfpath", default="/home/remote-perfmon")
parser.add_argument("--ip", dest="ip", default="host")

args = parser.parse_args()
gcfilepath = args.gcfilepath
pdfpath = args.pdfpath
ip = args.ip

if not gcfilepath.endswith("/"):
    gcfilepath = gcfilepath + "/"

if not pdfpath.endswith("/"):
    pdfpath = pdfpath + "/"
def getGCStatistics(gcfile):
    #print "{} start thread- {} ".format(time.time(),threading.current_thread())
    with open(gcfile, 'rb') as f:
        data = f.read()
    response = requests.post(url='https://api.gceasy.io/analyzeGC?apiKey=02f02125-ce48-4b9b-acc6-aef27d4848ec',
                             data=data,
                             headers={'Content-Type': 'text', 'Content-Encoding': 'zip','Connection': 'close'}
                             )
    if response.status_code==200:

        gcEasy_response = json.loads(response.text)
        fileNameArr = gcfile.split('/')
        agentversion = fileNameArr[len(fileNameArr) - 1].replace(".zip", "")
        if all([key in gcEasy_response for key in ['throughputPercentage', 'gcStatistics', 'webReport']]):
            throughputPercentage = gcEasy_response['throughputPercentage']
            throughputPercentage_df = pd.DataFrame([[throughputPercentage]], index=['throughputPercentage'])
            webReport = gcEasy_response['webReport']
            webReport_df = pd.DataFrame([[webReport]], index=['webReport'])

            gcStatistics = gcEasy_response['gcStatistics']
            df = pd.DataFrame.from_dict(gcStatistics, orient='index').sort_index()
            # df = pd.DataFrame.from_dict([gcStatistics],orient='columns').transpose()
            # , orient='index')
            df.index.name = "Metric"
            df = df.reindex(['avgAllocationRate', 'avgPromotionRate', 'fullGCTotalTime', 'minorGCTotalTime'])
            df = df.append(throughputPercentage_df)
            df = df.append(webReport_df)
            df.columns = [agentversion]
            return df
        else:
            print "request failed :"+str(response.status_code)
            return None


def plot(df):
    tabel_df = df.reindex(
        ['avgAllocationRate', 'avgPromotionRate', 'throughputPercentage', 'fullGCTotalTime', 'minorGCTotalTime'])
    # df=df.transpose().sort_values(by=['throughputPercentage'],ascending=False)
    fig, axs = plt.subplots(figsize=(len(tabel_df.columns) * 2, len(tabel_df.index)))

    # plt.subplots_adjust(top = 1, bottom = 0, right = 1, left = 0,
    #                     hspace = 0, wspace = 0)
    axs.axis("off")
    fig.suptitle(u'GC Statistics Result '+ip, fontsize=16, y=0.9)

    ccolors = plt.cm.BuPu(pd.np.full(len(tabel_df.columns), 0.2))
    table = axs.table(cellText=tabel_df.values, rowLabels=tabel_df.index, colLabels=tabel_df.columns, cellLoc='center',
                      colColours=ccolors, bbox=[0, 0, 1, 0.9])
    # colWidths=[0.3 for x in tabel_df.columns],
    table.auto_set_column_width(col=list(range(len(tabel_df.columns))))
    for (row, col), cell in table.get_celld().items():
        if (col == -1):
            cell.set_text_props(fontproperties=FontProperties(weight='bold', size=12))
            # cell._text.set_color('b')
            # cell.set_color('b')
        if (row == 0):
            cell.set_text_props(fontproperties=FontProperties(weight='bold', size=12))

    pp = PdfPages(pdfpath + 'Result_gc-'+ip+'.pdf')
    fig.savefig(pp, format='pdf', bbox_inches="tight", pad_inches=1)
    #print "done"

    url_df = df.reindex(['webReport'])
    url_df = url_df.transpose()
    fig2, axs2 = plt.subplots(figsize=(10, len(url_df.index)))
    axs2.axis("off")
    fig2.suptitle(u'GC Easy Report', fontsize=20)
    url_table = axs2.table(cellText=url_df.values, rowLabels=url_df.index, colLabels=url_df.columns, cellLoc='center',
                           colColours=ccolors
                           , bbox=[0, 0, 1, 0.9])
    for (row, col), cell in url_table.get_celld().items():
        if (col == -1) or row == 0:
            cell.set_text_props(fontproperties=FontProperties(weight='bold', size=12))
            # cell._text.set_color('b')
            # cell.set_color('b')
        else:
            cell.set_text_props(fontproperties=FontProperties(size=4))
    url_table.auto_set_font_size(False)

    fig2.savefig(pp, format='pdf', bbox_inches="tight", pad_inches=1)
    plt.cla()
    plt.close("all")
    pp.close()
class GcThread(threading.Thread):
    def __init__(self,func,args=()):
        super(GcThread,self).__init__()
        self.func=func
        self.args= args
        self.result = None
    def run(self):
        self.result=self.func(*self.args)
    def getResult(self):
        try:
            #Thread.join(self)
            print self.result
            return self.result
        except Exception:
            return None
def main():
    merged_df = pd.DataFrame(
        index=['avgAllocationRate', 'avgPromotionRate', 'throughputPercentage', 'fullGCTotalTime', 'minorGCTotalTime','webReport'])
    for filename in os.listdir(gcfilepath):
        if filename.__contains__('gc') and filename.endswith(".zip"):
            gc_result=getGCStatistics(gcfilepath+filename)
            merged_df = pd.concat([merged_df, gc_result], axis=1, sort=False)
        # print merged_df
    if merged_df.__len__()!=0:
        plot(merged_df)

if __name__ == '__main__':
    sys.exit(main())
