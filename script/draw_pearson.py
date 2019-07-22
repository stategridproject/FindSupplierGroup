# coding=UTF-8
'''
Created on 2019年7月2日

@author: shkk1
'''
import logging
import os
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from matplotlib import cm


def getlogger():
    basedir = os.path.dirname(os.path.realpath(__file__))
    p = os.path.join(basedir, "../../data/output/log/")
    if not os.path.exists(p):
        os.makedirs(p)
    logfile = os.path.join(basedir, "../../data/output/log/pearson.log")
    logger = logging.getLogger('drawpear')
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


class Draw_pearson_heat():
    def __init__(self, tyname, batname, drawtype):
        self.tyname = tyname
        self.batname = batname
        self.drawtype = drawtype
        basedir = os.path.dirname(os.path.realpath(__file__))
        self.folderpath = os.path.join(basedir, "../../data/output/")
        if not os.path.exists(self.folderpath):
            os.makedirs(self.folderpath)
        self.outputplace = os.path.join(
            basedir, "../../data/output/figure/heatmap/")
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

    def create_fileplace_cate(self, comtype):
        self.file_comtype = os.path.join(self.outputplace,
                                         './{}/category'.format(comtype))
        if not os.path.exists(self.file_comtype):
            os.makedirs(self.file_comtype)

    def create_fileplace_mate(self, comtype):
        self.file_comtype = os.path.join(self.outputplace,
                                         './{}/material'.format(comtype))
        if not os.path.exists(self.file_comtype):
            os.makedirs(self.file_comtype)

    def draw_heatmap(self, rst, finlist, draw_name):
        lenfin = len(finlist)
        df = np.zeros((lenfin, lenfin))
        for index1, i in enumerate(finlist):
            for index2, j in enumerate(finlist):
                df[index1][index2] = rst[i].loc[j]

        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.set_yticks(range(len(finlist)))
#         ax.set_yticklabels(finlist)
        ax.set_xticks(range(len(finlist)))
        plt.rcParams['font.sans-serif'] = ['SimHei']
        plt.rcParams['axes.unicode_minus'] = False
        plt.xticks([])
        plt.yticks([])
        im = ax.imshow(df, cmap=cm.get_cmap('rainbow', 1000))
        # 增加右侧的颜色刻度条
        plt.colorbar(im)
        # 增加标题
        plt.title(draw_name)
        # show
        plt.savefig('{}/{}'.format(self.file_comtype,
                                   '{}.png'.format(draw_name)))
        plt.close()

    def get_finlist(self, rst, comname, y_pred, drawtype):
        if drawtype == 'beKmeans':  # 未聚类
            finlist = comname
        elif drawtype == 'afKmeans':  # 聚类后
            finlist = []
            for i in set(y_pred):
                for j, index in zip(comname, y_pred):
                    if index == i:
                        finlist.append(str(j))
        elif drawtype == 'afselect':  # 选择群体
            '''去除孤立类，类内类别小于0.9
            '''
            finlist = []  # 供应商列表
            for i in set(y_pred):
                rrlist = []
                numlist = []
                for j, index in zip(comname, y_pred):
                    if index == i:
                        rrlist.append(j)
                        numlist.append(i)
                rlen = len(rrlist)
                numsum = 0
                if rlen <= 1:
                    continue
                else:
                    count = 0
                    for i, elem1 in enumerate(rrlist):
                        for j, elem2 in enumerate(rrlist):
                            num = rst[elem1].loc[elem2]
                            numsum = numsum + num
                            count += 1
                    if numsum / count > 0.95:
                        finlist = finlist + rrlist
        return finlist

    def draw_heat_cate(self, catename, drawtype):
        filefolder = os.path.join(self.catepalce, catename)
        listfile = self.getfile(filefolder)
        for i in listfile:
            battime = i.split('_')[-1].split('.')[0]
            if battime not in self.batname:
                continue
            readfile = os.path.join(filefolder, i)
            df = self.read_csv(readfile)
            comlist = df['comname']
            y_pred = df['y_pred']
            rst = df.drop(['comname', 'y_pred'], axis=1)
            self.create_fileplace_cate(catename)
            draw_name = "HeatMap_{}_{}_{}".format(catename, battime, drawtype)
            finlist = self.get_finlist(rst, comlist, y_pred, drawtype)
            self.draw_heatmap(rst, finlist, draw_name)

    def draw_heat_mate(self, matename, drawtype):
        filefolder = os.path.join(self.matepalce, matename)
        listfile = self.getfile(filefolder)
        for i in listfile:
            battime = i.split('_')[-1].split('.')[0]
            if battime not in self.batname:
                continue
            pac = i.split('_')[2]
            readfile = os.path.join(filefolder, i)
            df = self.read_csv(readfile)
            comlist = df['comname']
            y_pred = df['y_pred']
            rst = df.drop(['comname', 'y_pred'], axis=1)
            self.create_fileplace_mate(matename)
            draw_name = "HeatMap_{}_{}_{}_{}".format(matename, battime, drawtype, pac)
            finlist = self.get_finlist(rst, comlist, y_pred, drawtype)
            self.draw_heatmap(rst, finlist, draw_name)

    def __call__(self):
        self.catepalce = os.path.join(self.folderpath, './pearson_category/')
        catelist = self.getdir(self.catepalce)
        for i in catelist:
            if i in self.tyname:
                self.draw_heat_cate(i, self.drawtype)
        logger.info('draw category success')
        self.matepalce = os.path.join(self.folderpath, './pearson_material/')
        matelist = self.getdir(self.matepalce)
        for i in matelist:
            if i in self.tyname:
                self.draw_heat_mate(i, self.drawtype)
        logger.info('draw material success')


if __name__ == '__main__':
    tyname = ['水泥杆']  # []
    batname = ['1601']
    drawtype = 'afKmeans'  # 未聚类:beKmeans ，聚类后:afKmeans，选择群体:afselect
    test = Draw_pearson_heat(tyname=tyname, batname=batname, drawtype=drawtype)
    test()
