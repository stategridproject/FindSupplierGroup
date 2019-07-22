# coding=UTF-8
"""
Created on 2019年7月2日

@author: shkk1
"""
import logging
import os
from sklearn.cluster import KMeans
import pandas as pd


def getlogger():
    basedir = os.path.dirname(os.path.realpath(__file__))
    p = os.path.join(basedir, "../../data/output/log/")
    if not os.path.exists(p):
        os.makedirs(p)
    logfile = os.path.join(basedir, "../../data/output/log/mate.log")
    logger = logging.getLogger('calmate')
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


class Model_mate_group():
    """基于包计算群体
    """

    def __init__(self, tyname, number=0, batname=[]):
        self.name = tyname
        self.number = number
        self.batname = batname

        basedir = os.path.dirname(os.path.realpath(__file__))
        self.folderpath = os.path.join(basedir, "../../data/processed/")
        if not os.path.exists(self.folderpath):
            os.makedirs(self.folderpath)
        self.outputplace = os.path.join(
            basedir, "../../data/output/")
        if not os.path.exists(self.outputplace):
            os.makedirs(self.outputplace)

    def read_csv(self, point, index_vol=0, encoding='GBK', dtype=False):
        if 1 - dtype:
            data = pd.read_csv(point, index_col=index_vol, encoding=encoding,
                               low_memory=False)
        else:
            data = pd.read_csv(point, index_col=index_vol,
                               encoding=encoding, low_memory=False,
                               dtype='str')
        return data

    def save_csv(self, data, point):
        data.to_csv(point, encoding="GBK")

    def readtxt(self, filename):
        with open(filename, "r", encoding='GBK') as f:
            sd = f.read()
        return sd

    def get_dir(self, point):
        path_dir = os.listdir(point)
        listr = []
        for p in path_dir:
            pts = os.path.join(point, './{}'.format(p))
            if os.path.isdir(pts):  # 判断是否为文件夹，如果是判断所有文件就改成： os.path.isfile(p)
                listr.append(p)
        return listr

    def get_file(self, point):
        path_dir = os.listdir(point)
        listr = []
        for p in path_dir:
            pts = os.path.join(point, './{}'.format(p))
            if os.path.isfile(pts):  # 判断是否为文件夹，如果是判断所有文件就改成： os.path.isfile(p)
                listr.append(p)
        return listr

    def maxminnormalization(self, n):
        x = n
        Min = x.min()

        Max = x.max()
        x = (x.T - Min) / (Max - Min)
        return x.T

    def samplepearson(self, data):
        """pearson系数计算
        """
        data = data - data.mean(axis=0)
        corr = data.T.corr('pearson')
        return pd.DataFrame(corr)

    def getfinlist(self, y_ied, name, df1):
        """去除孤立类，类内类别小于0.9
        """
        finlist = []  # 供应商列表
        tlist = []  # 新类别顺序名
        for i in set(y_ied):
            rrlist = []
            numlist = []
            for j, index in zip(name, y_ied):
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
                        num = df1[elem1].loc[elem2]
                        numsum = numsum + num
                        count += 1
                if numsum / count > 0.9:
                    finlist = finlist + rrlist
                    tlist = tlist + numlist
        return finlist, tlist

    def create_fileplace(self, comtype):
        self.file_comtype = os.path.join(self.folderpath,
                                         './{}/{}/'.format(comtype,self.ComID))
        if not os.path.exists(self.file_comtype):
            os.makedirs(self.file_comtype)

    def cal_kmeans_para(self, shape):
        if self.number == 0:
            if shape > 80:
                number = 30
            elif shape < 10:
                number = 5
            elif shape > 130:
                number = 45
            else:
                number = 20
            """K-means
            """
            if shape == 1:
                return -1
            if shape < number:
                number = int(shape / 2) - 1
            if number <= 0:
                number = 1
            return number
        else:
            return self.number

    def save_pearson(self, rst, comname, y_pred):
        """保存pearson矩阵
        """
        save_place = os.path.join(self.outputplace,
                                  './pearson_material/{}/'.format(self.tname))
        if not os.path.exists(save_place):
            os.makedirs(save_place)
        save_file = os.path.join(save_place,
                                 'pearson_{}_{}_{}.csv'
                                 .format(self.ComID,
                                         self.pac, self.Battime))
        rst['comname'] = comname.values
        rst.loc[:, 'y_pred'] = y_pred
        self.save_csv(rst, save_file)

    def get_person_data(self, df):
        """利用K-means对供应商群体进行划分，并进行筛选
        1.产生pearson相关系数表
        2.进行Kmeans聚类并筛选群体
        3.将结果保存
        """
        df = df.copy()
        self.pac = df.iloc[0]['包号']
        tmp = pd.DataFrame({'投标总价': df.groupby('供应商名称')['含税总价'].sum()})
        tmp.reset_index(inplace=True, drop=False)
        df = pd.merge(df, tmp, on=['供应商名称'], how='left', suffixes=['', '_x'])
        df.loc[:, 'radio'] = df['含税总价'] / df['投标总价']
        df.loc[:, '含税总单价'] = df['含税总单价'] * 10000
        df.set_index(['供应商名称', 'mat_id'], inplace=True)
        tt = df[['含税总单价']].unstack('mat_id')
        df = tt.reset_index()
        comdf = df.dropna()
        comname = comdf['供应商名称']
        comlist = comdf['含税总单价']
        if comlist.shape[1] > 2:
            rst = self.samplepearson(comlist)
        else:
            return
        rst.columns = comname
        rst.index = comname
        number = self.cal_kmeans_para(rst.shape[0])
        if number < 0:
            return
        y_pred = KMeans(n_clusters=number, random_state=9,
                        max_iter=600, init='k-means++').fit_predict(rst)
        # 存储数据
        self.save_pearson(rst, comname, y_pred)
        finlist, tlist = self.getfinlist(y_pred, comname, rst)
        tmp_save = pd.DataFrame({'name': finlist, 'num': tlist})
        if tmp_save.empty:
            logger.info('{} {} is group empty'.format(
                self.ComID, self.pac))
            return
        tmp_save.loc[:, '包号'] = self.pac
        self.rst = self.rst.append(tmp_save)
        logger.info('select {} over {}'.format(self.ComID, self.pac))

    def get_type(self, df):
        self.rst = pd.DataFrame()
        ComID = df.iloc[0]['分标编号']
        self.Battime = df.iloc[0]['bat']
        if len(self.batname) == 0:
            self.ComID = ComID
        elif self.Battime in self.batname:
            self.ComID = ComID
        else:
            return
        self.create_fileplace(self.tname)
        logger.info('start {}'.format(self.ComID))
        df.groupby('包号').filter(self.get_person_data)
        self.rst.reset_index(inplace=True, drop=False)
        self.save_csv(self.rst, os.path.join(self.file_comtype,
                                             './material.csv'))
        logger.info('save file success')

    def all_operate(self, df1):
        """
        """
        type_name = df1.iloc[0]['分标名称']
        if len(self.name) == 0:
            self.tname = type_name
        elif type_name in self.name:
            self.tname = type_name
        else:
            return
        logger.info('start {}'.format(type_name))
        df1.groupby(['分标编号']).filter(self.get_type)  # 物料群体

    def change_name(self, df):
        """供应商名称脱敏化
        """
        df2 = self.read_csv(os.path.join(
            self.folderpath, 'base_table.csv'), dtype=True)
        rr = pd.merge(df, df2, on=['供应商名称'])
        rr.drop(['供应商名称'], axis=1, inplace=True)
        rr.rename(columns={'num': '供应商名称'}, inplace=True)
        return rr

    def __call__(self):
        """输入的品类，批次编号
        --产生群体csv表，中间数据pearson相关数据csv表
        """
        df1 = self.read_csv(os.path.join(self.folderpath, 'matr_all.csv'),
                            encoding='GBK')
#         df1 = self.change_name(df1)
        df1.groupby(['分标名称']).filter(self.all_operate)


if __name__ == '__main__':
    tyname = ['水泥杆']  # []遍历所有
    batname = ['1601']  # 可选值, '1601', '1602', '16XZ', '1701', '1702', '1703', '1801', '1802', '1804'
    number = 0  # 0,自动计算
    run = Model_mate_group(tyname=tyname, number=number, batname=batname)
    run()
