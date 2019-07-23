# coding=UTF-8
'''
Created on 2019年7月8日

@author: shkk1
'''
import logging
import os
import random
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from matplotlib import cm
from matplotlib.pyplot import draw
from script.B_value import BValue
from script.plot_func import PlotBValue
from tqdm import tqdm

def getlogger():
    basedir = os.path.dirname(os.path.realpath(__file__))
    p = os.path.join(basedir, "../../data/output/log/")
    if not os.path.exists(p):
        os.makedirs(p)
    logfile = os.path.join(basedir, "../../data/output/log/random.log")
    logger = logging.getLogger('drawrandom')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(logfile, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


logger = getlogger()


class DrawRandom():
    def __init__(self, tyname):
        self.tyname = tyname
        basedir = os.path.dirname(os.path.realpath(__file__))
        self.folderpath = os.path.join(basedir, "../../data/output/")
        if not os.path.exists(self.folderpath):
            os.makedirs(self.folderpath)
        self.outputplace = os.path.join(
            basedir, "../../data/output/figure/random/")
        if not os.path.exists(self.outputplace):
            os.makedirs(self.outputplace)

    def read_csv(self, point, index_vol=0, encoding='GBK', dtype=False):
        if 1 - dtype:
            data = pd.read_csv(point, index_col=index_vol, encoding=encoding,
                               engine='python')
        else:
            data = pd.read_csv(point, index_col=index_vol,
                               encoding=encoding, low_memory=False,
                               dtype='str')
        return data

    def save_csv(self, data, point):
        data.to_csv(point, encoding="GBK")

    def getdir(self, point):
        path_dir = os.listdir(point)
        listr = []
        for p in path_dir:
            pts = os.path.join(point, './{}'.format(p))
            if os.path.isdir(pts):  # 判断是否为文件夹，如果是判断所有文件就改成： os.path.isfile(p)
                listr.append(p)
        return listr

    def getfile(self, point):
        path_dir = os.listdir(point)
        listr = []
        for p in path_dir:
            pts = os.path.join(point, './{}'.format(p))
            if os.path.isfile(pts):  # 判断是否为文件夹，如果是判断所有文件就改成： os.path.isfile(p)
                listr.append(p)
        return listr

    def add_df(self, df):
        longnum = df.shape[0]
        comlist = random.sample(self.tmp, longnum)
        for i in comlist:
            self.rst.loc[self.count] = [self.timeid, i, self.num]
            self.count += 1
            self.tmp.remove(i)
        self.num += 1

    def create_random_com(self):
        outputfile = os.path.join(self.folderpath,
                                  './model_group/{}_group.csv'
                                  .format(self.tyname))
        saveplace = os.path.join(self.folderpath,
                                 './random_group/'
                                 .format(self.tyname))
        if not os.path.exists(saveplace):
            os.makedirs(saveplace)
        df1 = self.read_csv(outputfile)
        df2 = self.read_csv(os.path.join(self.folderpath,
                                         '../processed/pivot_data.csv'))
        self.rst = pd.DataFrame(columns=['time', 'Supplier', 'num'])
        self.num = 0
        self.count = 0
        for i in df1['time'].value_counts().index.values:
            self.timeid = i
            tmp1 = df1[df1['time'] == i]
            self.tmp = set(df2[df2['分标编号'] == i]['供应商名称'].values)
            tmp1.groupby('num').filter(self.add_df)
        self.save_csv(self.rst, os.path.join(saveplace,
                                             '{}_group.csv'.format(self.tyname)))

    def cal_B_value(self):
        input_file = '../../data/processed/pivot_data.csv'
        kind = self.tyname
        output_dir = os.path.join(self.folderpath,
                                  './random_group/csv/{}'.format(kind))
        group_file = os.path.join(self.folderpath,
                                  './random_group/{}_group.csv'.format(kind))
        if_initial = True
        cal_cate_value = BValue(kind=kind, output_dir=output_dir,
                                group_file=group_file, input_file=input_file,
                                if_initial=if_initial)
        cal_cate_value()

    def plot_line(self, plot_df, plot_random_df ,batch_code, output_dir, kind, linewidth=5, min_y=-3, max_y=3):
        fig_size = (30, 20)
        label_size = 50
        _, ax = plt.subplots(figsize=fig_size)
        ax.tick_params(labelsize=label_size)
        ax.plot(np.arange(len(plot_df)),
                100 * (plot_df['B_dropGroup_value'] - plot_df['B_real_value']) /
                plot_df['B_real_value'], label='基准价格B变化率', linewidth=linewidth)
        ax.plot(np.arange(len(plot_random_df)),
                100 * (plot_random_df['B_dropGroup_value'] - plot_random_df['B_real_value']) /
                plot_random_df['B_real_value'], label='随机群体基准价格B变化率', linewidth=linewidth)
        ax.legend(fontsize=label_size)
        ax.set_xlabel('标-包', size=label_size)
        ax.set_ylabel('relative-B-value/%', size=label_size)
        ax.set_ylim(bottom=min_y, top=max_y)
        ax.set_title('relative-B-{}-{}'.format(kind,
                                               batch_code), size=label_size)
        ax.hlines(0, 0, len(plot_df), linestyles='--', colors='k')
        ax.text(x=len(plot_df), y=0.5, s='0.5%',
                color='r', size=label_size)
        ax.text(x=len(plot_df), y=-0.5, s='-0.5%',
                color='r', size=label_size)
        ax.hlines(-0.5, 0, len(plot_df), linestyles='--', colors='r')
        ax.hlines(0.5, 0, len(plot_df), linestyles='--', colors='r')
        plt.savefig('{}/{}_{}_B_value_line.png'.format(output_dir,
                                                       kind, batch_code), bbox_inches='tight')
        plt.close()

    def plotbvalue(self, b_value_df, b_value_random_df, output_dir, kind):
        for batch_code in tqdm(b_value_df['批次编码'].unique()):
                plot_df = b_value_df[b_value_df['批次编码']
                                     == batch_code]
                plot_random_df = b_value_random_df[b_value_random_df['批次编码']
                                                   == batch_code]
                self.plot_line(plot_df=plot_df,
                               plot_random_df=plot_random_df,
                               batch_code=batch_code,
                               output_dir=output_dir, kind=kind)

    def draw_B_value(self):
        kind = self.tyname
        output_dir = os.path.join(self.outputplace, kind)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        b_value_file = os.path.join(self.folderpath,
                                    './bvalue/csv/{}/{}_all_b.csv'
                                    .format(kind, kind))
        b_random_file = os.path.join(self.folderpath,
                                     './random_group/csv/{}/{}_all_b.csv'
                                     .format(kind, kind))
        b_value_df = self.read_csv(b_value_file)
        b_value_random_df = self.read_csv(b_random_file)
        self.plotbvalue(b_value_df=b_value_df,
                        b_value_random_df=b_value_random_df,
                        output_dir=output_dir, kind=kind)

    def __call__(self):
        self.create_random_com()
        logger.info('random group create success')
        self.cal_B_value()
        logger.info('random')
        self.draw_B_value()


if __name__ == '__main__':
    """默认绘制所有批次
    """
    tyname = '水泥杆'
    drawt = DrawRandom(tyname=tyname)
    drawt()
