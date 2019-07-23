# coding=UTF-8
"""
Created on 2019年7月2日

@author: shkk1
"""
import os
import logging
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans


def getlogger():
    basedir = os.path.dirname(os.path.realpath(__file__))
    p = os.path.join(basedir, "../../data/output/log/")
    if not os.path.exists(p):
        os.makedirs(p)
    logfile = os.path.join(basedir, "../../data/output/log/pac.log")
    logger = logging.getLogger('calpace')
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


class Model_cate_group():
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
        """读取csv格式数据
        """
        if 1 - dtype:
            data = pd.read_csv(point, index_col=index_vol, encoding=encoding,
                               low_memory=False)
        else:
            data = pd.read_csv(point, index_col=index_vol,
                               encoding=encoding, low_memory=False,
                               dtype='str')
        return data

    def save_csv(self, data, point):
        """存储csv格式数据
        """
        data.to_csv(point, encoding="GBK")

    def maxminnormalization(self, n):
        """最大最小值标准化
        """
        x = n
        Min = x.min()

        Max = x.max()
        x = (x.T - Min) / (Max - Min)
        return x.T

    def samplepearson(self, data):
        """
        计算相关系数
        """
        data = data - data.mean(axis=0)
        corr = data.T.corr('pearson')
        return pd.DataFrame(corr)

    def getfinlist(self, y_ied, name, df1):
        """
        对聚类结果进行筛选，选择合适群体
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

    def cal_kmeans_para(self, shape):
        """
        设置K值
        """
        if self.number == 0:
            if shape > 100:
                number = 40
            elif shape < 10:
                number = 5
            else:
                number = 15
            """
            K-means
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

    def create_fileplace(self, comtype):
        self.file_comtype = os.path.join(self.folderpath,
                                         './{}/{}/'.format(comtype,self.ComID))
        if not os.path.exists(self.file_comtype):
            os.makedirs(self.file_comtype)

    def save_pearson(self, rst, comname, y_pred):
        """
        保存pearson相关系数方法
        """
        save_place = os.path.join(self.outputplace,
                                  './pearson_category/{}/'.format(self.tname))
        if not os.path.exists(save_place):
            os.makedirs(save_place)
        save_file = os.path.join(save_place,
                                 'pearson_{}_{}.csv'.
                                 format(self.ComID, self.Battime))
        rst['comname'] = comname.values
        rst.loc[:, 'y_pred'] = y_pred
        self.save_csv(rst, save_file)

    def get_person_data(self, df):
        """
        利用K-means对供应商群体进行划分，并进行筛选
        1.产生pearson相关系数表
        2.进行Kmeans聚类并筛选群体
        3.将结果保存
        """
        ComID = df.iloc[0]['分标编号']
        self.Battime = df.iloc[0]['bat']
        if len(self.batname) == 0:
            self.ComID = ComID
        elif self.Battime in self.batname:
            self.ComID = ComID
        else:
            return
        self.create_fileplace(self.tname)
        file = open(os.path.join(self.file_comtype,
                                 '{}.txt'.format(df['包号'].
                                                 value_counts().
                                                  shape[0])), 'w')
        file.close()
        df.set_index(['供应商名称', '包号'], inplace=True)
        tt = df[['投标报价']].unstack('包号')
        df = tt.reset_index()
        comdf = df.dropna()
        comname = comdf['供应商名称']
        comlist = comdf['投标报价']
        if comlist.shape[1] > 2:
            rst = self.samplepearson(comlist)
        else:
            return
        rst.columns = comname
        rst.index = comname
        number = self.cal_kmeans_para(rst.shape[0])
        if number < 0:
            return
        y_pred = KMeans(n_clusters=number, random_state=20,
                        max_iter=600, init='random').fit_predict(rst)
        # 存储数据
        self.save_pearson(rst, comname, y_pred)
        finlist, tlist = self.getfinlist(y_pred, comname, rst)
        tmp_save = pd.DataFrame({'Supplier': finlist, 'num': tlist})
        if 1 - tmp_save.empty:
            tmp_save.loc[:, 'time'] = self.ComID
        self.save_csv(tmp_save.rename(columns={'Supplier': 'name'}),
                      os.path.join(self.file_comtype,
                                   './category.csv'))
        self.rst = self.rst.append(tmp_save)
        logger.info('select {} over'.format(self.ComID))

    def get_type(self, df):
        """
        对表中同一个品类所有数据进行处理
        """
        self.rst = pd.DataFrame()
        type_name = df.iloc[0]['分标名称']
        if len(self.name) == 0:
            self.tname = type_name
        elif type_name in self.name:
            self.tname = type_name
        else:
            return
        logger.info('start {}'.format(type_name))
        df.groupby('分标编号').filter(self.get_person_data)
        self.rst.reset_index(inplace=True, drop=True)
#         self.save_csv(self.rst, saveplace)
        logger.info('save file success')

    def change_name(self, df):
        """
        脱敏函数
        """
        df2 = self.read_csv(os.path.join(
            self.folderpath, 'base_table.csv'), dtype=True)
        rr = pd.merge(df, df2, on=['供应商名称'])
        rr.drop(['供应商名称'], axis=1, inplace=True)
        rr.rename(columns={'num': '供应商名称'}, inplace=True)
        return rr

    def __call__(self):
        """
        输入的品类，批次编号
        --产生群体csv表，中间数据pearson相关数据csv表
        """
        df1 = self.read_csv(os.path.join(self.folderpath, 'pivot_data.csv'))
#         df1 = self.change_name(df1)  # 对供应商名称进行脱敏
        df1.groupby(['分标名称']).filter(self.get_type)


if __name__ == '__main__':
    tyname = ['水泥杆']  # [] 可选值，各种品类名，可多选
    batname = []  # 可选值, 1601, 1602, 16XZ, 1701, 1702, 1703, 1801, 1802, 1804
    number = 0  # 0,自动计算
    run = Model_cate_group(tyname=tyname, number=number, batname=batname)
    run()
