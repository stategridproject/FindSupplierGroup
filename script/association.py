# coding=UTF-8
import os
import logging
import pandas as pd
from tqdm import tqdm
from glob import glob

def getlogger():
    basedir = os.path.dirname(os.path.realpath(__file__))
    p = os.path.join(basedir, "../../data/output/log/")
    if not os.path.exists(p):
        os.makedirs(p)
    logfile = os.path.join(basedir, "../../data/output/log/other.log")
    logger = logging.getLogger('calother')
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


class Association:
    def __init__(self, tyname, batname=[], geo_rank='province', geo_file='../../data/raw/supplier_geo.csv', txt_dir='../../data/raw/company_relation', relation_file='../../data/processed/relations.csv'):
        self.name = tyname
        self.geo_rank = geo_rank
        self.batname = batname
        self.geo_file = geo_file
        self.txt_dir = txt_dir
        self.txt_files = glob('{}/*.txt'.format(self.txt_dir))
        self.relation_file = relation_file
        basedir = os.path.dirname(os.path.realpath(__file__))
        self.folderpath = os.path.join(basedir, "../../data/processed/")
        if not os.path.exists(self.folderpath):
            os.makedirs(self.folderpath)

        if geo_rank == 'province':
            self.geo_groupby_list = ['供应商名称', 'province']
        elif geo_rank == 'city':
            self.geo_groupby_list = ['供应商名称', 'province', 'city']
        elif geo_rank == 'district':
            self.geo_groupby_list = ['供应商名称', 'province', 'city', 'district']
        else:
            self.geo_groupby_list = ['供应商名称', 'province']
        pass

    def preprocess(self):
        for file in tqdm(self.txt_files):
            count = 0
            supplier1, supplier2 = os.path.basename(file).split('.')[0].split('-')
            min_relation = 100
            try:
                with open(file, 'r') as f:
                    for line in f.readlines():
                        if '国务院国有资产监督管理委员会' in line.strip():
                            continue
                        count += 1
                        if len(line.split('-')) < min_relation:
                            min_relation = len(line.split('-'))
                    if count > 0:
                        self.relation.loc[','.join(sorted([supplier1, supplier2])), 'relation_count'] = count
                        self.relation.loc[','.join(sorted([supplier1, supplier2])), 'min_relation'] = min_relation
            except:
                print('Error:' + file)

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

    def cal_group(self, supplier_list):
        df = self.geo[self.geo['供应商名称'].isin(supplier_list)]
        pair_list = [','.join(sorted([supplier1, supplier2]))
                     for i, supplier1 in enumerate(supplier_list)
                     for supplier2 in supplier_list[i + 1:]
                     if ','.join(sorted([supplier1, supplier2])) in self.relation.index]
        group_1 = self.get_community(pair_list=pair_list)
        for i, g in enumerate(self.geo_groupby_list[1:]):
            if i == 0:
                tmp = df[g].values
                continue
            tmp += df[g].values
        group_2 = pd.DataFrame({'name': df['供应商名称'].values, 'num': tmp})
        return group_1, group_2

    def create_fileplace(self, comtype):
        self.file_comtype = os.path.join(self.folderpath,
                                         './{}/{}/'.format(comtype, self.ComID))
        if not os.path.exists(self.file_comtype):
            os.makedirs(self.file_comtype)

    def get_community(self, pair_list):
        """
        根据图数据关系，获取群体间关联
        """
        group = {}
        group_num = 0
        for pair in pair_list:
            supplier1, supplier2 = pair.split(',')
            if (supplier1 not in group) & (supplier2 not in group):
                group_num += 1
                group[supplier1] = group_num
                group[supplier2] = group_num
            elif (supplier1 not in group) & (supplier2 in group):
                group[supplier1] = group[supplier2]
            elif (supplier1 in group) & (supplier2 not in group):
                group[supplier2] = group[supplier1]
        # print(list(group.values()))
        return pd.DataFrame({'num': list(group.values()),
                             'name': list(group.keys())})

    def get_person_data(self, df):
        """
        使用图数据库进行包内供应商关联计算
        """
        df = df.copy()
        self.pac = df.iloc[0]['包号']
        comlist = list(set(df['供应商名称'].values))
        group1, group2 = self.cal_group(comlist)

        if 1 - group1.empty:
            group1.loc[:, '包号'] = self.pac
        if 1 - group2.empty:
            group2.loc[:, '包号'] = self.pac
        self.rst1 = self.rst1.append(group1)
        self.rst2 = self.rst2.append(group2)
        logger.info('select {} over {}'.format(self.ComID, self.pac))

    def get_type(self, df):
        """
        产生地理相关供应商与利益相关供应商
        """
        self.rst1 = pd.DataFrame()  # relation
        self.rst2 = pd.DataFrame()  # geo
        ComID = df.iloc[0]['分标编号']
        Battime = df.iloc[0]['bat']
        if len(self.batname) == 0:
            self.ComID = ComID
        elif Battime in self.batname:
            self.ComID = ComID
        else:
            return
        self.create_fileplace(self.tname)
        logger.info('start {}'.format(self.ComID))
        df.groupby('包号').filter(self.get_person_data)
        self.rst1.reset_index(inplace=True, drop=True)
        self.rst2.reset_index(inplace=True, drop=True)

        self.save_csv(self.rst1, os.path.join(self.file_comtype,
                                              './relation.csv'))
        self.save_csv(self.rst2, os.path.join(self.file_comtype,
                                              './geographical.csv'))
        logger.info('save file success')

    def all_operate(self, df1):
        """
        对每个物料各个批次进行操作
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
        """
        供应商名称脱敏化
        """
        df2 = self.read_csv(os.path.join(
            self.folderpath, 'base_table.csv'), dtype=True)
        rr = pd.merge(df, df2, on=['供应商名称'])
        rr.drop(['供应商名称'], axis=1, inplace=True)
        rr.rename(columns={'num': '供应商名称'}, inplace=True)
        return rr

    def __call__(self):
        df1 = self.read_csv(os.path.join(self.folderpath, 'pivot_data.csv'),
                            encoding='GBK')
        # self.get_type(df1)
        if not os.path.exists(self.relation_file):
            self.relation = pd.DataFrame()
            self.preprocess()
            self.relation.to_csv(self.relation_file, encoding='gbk')
        else:
            self.relation = pd.read_csv(self.relation_file, encoding='gbk', index_col=0)
        # self.relation = pd.read_csv(os.path.join(
        #     self.folderpath, 'relations.csv'), encoding='gbk', index_col=0)
        self.geo = pd.read_csv(self.geo_file,
                               encoding='utf-8', index_col=0)[['供应商名称',
                                                               'province',
                                                               'city',
                                                               'district']]
        self.geo.dropna(inplace=True)
#         df1 = self.change_name(df1)
        df1.groupby(['分标名称']).filter(self.all_operate)


if __name__ == '__main__':
    tyname = ['水泥杆']  # []遍历所有
    batname = []  # 可选值, 1601, 1602, 16XZ, 1701, 1702, 1703, 1801, 1802, 1804
    geo_rank = 'province'
    run = Association(tyname=tyname, batname=batname, geo_rank=geo_rank)
    run()
