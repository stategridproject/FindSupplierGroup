# coding=UTF-8
"""
Created on 2019年7月2日

@author: shkk1
"""
import os
import random
import logging
import pandas as pd
import numpy as np


def getlogger():
    basedir = os.path.dirname(os.path.realpath(__file__))
    p = os.path.join(basedir, "../../data/output/log/")
    if not os.path.exists(p):
        os.makedirs(p)

    logfile = os.path.join(basedir, "../../data/output/log/pretreate.log")
    logger = logging.getLogger('predeal')
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    fh = logging.FileHandler(logfile, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)

    logger.addHandler(fh)

    logger.addHandler(ch)

    return logger


logger = getlogger()


class preprocess():
    """数据预处理部分
    """
    def __init__(self):
        basedir = os.path.dirname(os.path.realpath(__file__))
        self.folderpath = os.path.join(basedir, "../../data/raw/")
        if not os.path.exists(self.folderpath):
            os.makedirs(self.folderpath)

        self.processplace = os.path.join(basedir, "../../data/processed/")
        if not os.path.exists(self.processplace):
            os.makedirs(self.processplace)

    def read_csv(self, point, index_vol=0, encoding='GBK', dtype=False):
        """
        读取csv格式文件
        """
        if 1 - dtype:
            data = pd.read_csv(point, index_col=index_vol,
                               encoding=encoding, low_memory=False)
        else:
            data = pd.read_csv(point, index_col=index_vol,
                               encoding=encoding, low_memory=False,
                               dtype='str')
        return data

    def save_csv(self, data, point):
        """
        存储csv格式文件
        """
        data.to_csv(point, encoding="GBK")

    def read_excel(self, point, sheet=0, header=0, index_col=None):
        """
        读取excel格式文件
        """
        data = pd.read_excel(point, sheet_name=sheet,
                             header=header, index_col=index_col)
        return data

    def create_type_csv(self):
        """
        制作品类合并表并统一品类名称
        """
        df1 = self.read_excel(os.path.join(
            self.folderpath, '开标一览表.xlsx'), sheet='Sheet1')
        df2 = pd.read_csv(os.path.join(self.folderpath, '类别名称变换表.csv'),
                          encoding='GBK', engine='python', index_col=0)
        df3 = self.read_excel(os.path.join(
            self.folderpath, '中标信息.xlsx'), sheet='中标信息', index_col=0)
        df3 = df3[['分标编码', '分标名称', '包号', '供应商名称', '中标价格']].copy()
        df3.rename(columns={'分标编码': '分标编号'}, inplace=True)
        df1 = df1[['分标编号', '分标名称', '包号', '供应商名称', '投标报价', '招标批次名称']].copy()
        df1.sort_values(['分标编号', '分标名称', '供应商名称', '包号'], inplace=True)

        df1.loc[:, 'sub'] = df1['分标编号'].str.split('-')
        df1.loc[:, 'year'] = np.where(
            df1['sub'].str[0] == 'GWXY', df1['sub'].str[2].str[:4],
            df1['sub'].str[0].str[-2:])
        tmp = df1[df1['sub'].str[0] != 'GWXY']
        tmp.loc[tmp['招标批次名称'].str.contains('第一'), 'num'] = '01'
        tmp.loc[tmp['招标批次名称'].str.contains('第二'), 'num'] = '02'
        tmp.loc[tmp['招标批次名称'].str.contains('第三'), 'num'] = '03'
        tmp.loc[tmp['招标批次名称'].str.contains('第四'), 'num'] = '04'
        tmp.loc[tmp['招标批次名称'].str.contains('新增'), 'num'] = 'XZ'
        df1.loc[tmp.index, 'num'] = tmp['num']
        df1.fillna('', inplace=True)
        df1.loc[:, 'bat'] = df1['year'] + df1['num']

        tt = pd.merge(df1, df3[['分标编号', '分标名称', '包号', '供应商名称', '中标价格']], on=[
                      '分标编号', '分标名称', '包号', '供应商名称'],
                      how='left', suffixes=('', '_x'))

        tt.fillna(0, inplace=True)
        tt.loc[:, '是否投标'] = np.where(tt['中标价格'] == 0, 0, 1)

        rr = pd.merge(tt, df3, on=['分标编号', '分标名称', '包号'],
                      how='left', suffixes=('', '_x'))
        ss = rr[['分标名称', '分标编号', '供应商名称', '包号', '投标报价',
                 '中标价格_x', '是否投标', 'bat']]
        ss.rename(columns={'中标价格_x': '中标价格'}, inplace=True)

        rr = pd.merge(ss, df2, how='left', on=['分标名称'], suffixes=['', '_x'])
        rr.drop(['分标名称'], axis=1, inplace=True)
        rr.rename(columns={'name': '分标名称'}, inplace=True)
        self.save_csv(rr, os.path.join(self.processplace, 'pivot_data.csv'))

    def create_masplist_csv(self):
        """
        拆分行报价汇总表
        """
        df_dict = self.read_excel(os.path.join(
            self.folderpath, '行报价汇总.xlsx'), None, 1)
        count = 1
        for i in df_dict.keys():
            self.save_csv(df_dict[i], os.path.join(
                self.processplace, 'hbj_{}.csv').format(count))
            count += 1

    def create_ma_comb(self):
        """
        制作物料合并表并替换品类名
        """
        tmp_df = self.read_excel(os.path.join(
            self.folderpath, '需求一览表.xlsx'), sheet='Sheet1')
        tmp2_df = self.read_excel(os.path.join(
            self.folderpath, '中标信息.xlsx'), sheet='中标信息')
        df2 = pd.read_csv(os.path.join(self.folderpath, '类别名称变换表.csv'),
                          encoding='GBK', engine='python', index_col=0)

        tmp2_df.rename(columns={'分标编码': '分标编号'}, inplace=True)
        tmp_df.rename(columns={'采购申请编码': '采购申请编号',
                               '行项目编码': '行项目编号'}, inplace=True)
        tmp_df['物料编码'] = tmp_df['物料编码'].astype('str')
        tmp_df['mat_id'] = tmp_df['物料编码'] + '-' + tmp_df['技术规范书ID']
        tmp_df = tmp_df[['采购申请编号', '行项目编号', 'mat_id', '概算单价', '分标编号', '招标批次名称']]
        tmp_df.loc[:, '概算单价'] = tmp_df['概算单价'] / 10000
        tmp_df.loc[:, 'sub'] = tmp_df['分标编号'].str.split('-')
        tmp_df.loc[:, 'year'] = np.where(
            tmp_df['sub'].str[0] == 'GWXY', tmp_df['sub'].str[2].str[:4],
            tmp_df['sub'].str[0].str[-2:])
        tmp = tmp_df[tmp_df['sub'].str[0] != 'GWXY']
        tmp.loc[tmp['招标批次名称'].str.contains('第一'), 'num'] = '01'
        tmp.loc[tmp['招标批次名称'].str.contains('第二'), 'num'] = '02'
        tmp.loc[tmp['招标批次名称'].str.contains('第三'), 'num'] = '03'
        tmp.loc[tmp['招标批次名称'].str.contains('第四'), 'num'] = '04'
        tmp.loc[tmp['招标批次名称'].str.contains('新增'), 'num'] = 'XZ'
        tmp_df.loc[tmp.index, 'num'] = tmp['num']
        tmp_df.fillna('', inplace=True)
        tmp_df.loc[:, 'bat'] = tmp_df['year'] + tmp_df['num']
        tmp_df = tmp_df[['采购申请编号', '行项目编号', 'mat_id', '概算单价', 'bat']]
        tmp_df = tmp_df.drop_duplicates()
        dtmp = pd.DataFrame()
        for i in range(3):
            df = pd.read_csv(os.path.join(self.processplace,
                                          'hbj_{}.csv'.format(i + 1)),
                             index_col=0, encoding='GBK')
            df.rename(columns={'含税单价(万元)/折扣率(%)': '含税总单价',
                               '含税总价(万元)/折扣率(%)': '含税总价'},
                      inplace=True)
            dtmp = dtmp.append(df)
        dtmp.reset_index(inplace=True, drop=True)
        rr = pd.merge(dtmp, tmp_df, on=['采购申请编号', '行项目编号'], how='left')
        rr = pd.merge(rr, tmp2_df[['分标编号', '包号', '供应商名称', '中标价格']], on=[
                      '分标编号', '包号', '供应商名称'], how='left')
        rr.loc[:, '是否中标'] = np.where(rr['中标价格'].isna(), '否', '是')
        rr.drop(['中标价格'], axis=1, inplace=True)

        rr = pd.merge(rr, df2, how='left', on=['分标名称'], suffixes=['_x', '_y'])
        rr.drop(['分标名称'], axis=1, inplace=True)
        rr.rename(columns={'name': '分标名称'}, inplace=True)
        rr = rr.drop_duplicates()
        rr = rr[['分标编号', '分标名称', '包号', '供应商名称', '采购申请编号', '行项目编号',
                 '数量', '含税总单价', '含税总价', 'mat_id', '是否中标', '概算单价', 'bat']]
        self.save_csv(rr,
                      os.path.join(self.processplace, 'matr_all.csv'))

    def create_basetable_csv(self):
        """
        制作供应商脱敏表
        """
        def func(df):
            number1 = random.sample(self.tlist, 1)[0]
            self.tlist.remove(number1)
            return number1
        dfl = self.read_csv(os.path.join(self.processplace, 'pivot_data.csv'))
        comlist = dfl['供应商名称'].value_counts().index.values
        rst = pd.DataFrame({'com': comlist})
        self.tlist = ["%05d" % i for i in range(99999 + 1)]
        rst.loc[:, 'num'] = rst['com'].apply(func)
        self.save_csv(rst, os.path.join(self.processplace, 'base_table.csv'))

    def deal_ma_csv(self):
        """
        去物料异常值
        """
        df1 = self.read_csv(os.path.join(self.processplace, 'matr_all.csv'))
        df1 = df1.drop_duplicates()
        tmp = df1[df1['是否中标'] == '是'][['分标编号', '包号',
                                       '采购申请编号', '行项目编号', 'mat_id', '含税总单价']]
        tmp = tmp.drop_duplicates()
        rr = pd.merge(df1, tmp[['分标编号', '包号', 'mat_id', '行项目编号', '含税总单价']],
                      on=['分标编号', '包号', '行项目编号', 'mat_id'], how='left',
                      suffixes=('', '_x'))
        rr.rename(columns={'含税总单价_x': '中标价格'}, inplace=True)

        rr.loc[:, '行项目编号'] = rr['行项目编号'].apply(lambda x: '%05d' % x)
        rr.loc[:, 'mat_id'] = rr['行项目编号'] + '-' + rr['mat_id']
        rr = rr[(rr['含税总单价'] < rr['中标价格'] * 100) &
                (rr['含税总单价'] > rr['中标价格'] * 0.1)]
        self.save_csv(rr, os.path.join(self.processplace, 'matr_all.csv'))

    def deal_type_csv(self):
        """
        去品类异常值
        """
        df1 = self.read_csv(os.path.join(self.processplace, 'pivot_data.csv'))
        df1 = df1.drop_duplicates()
        rr = df1[(df1['投标报价'] < df1['中标价格'] * 4) &
                 (df1['投标报价'] > df1['中标价格'] * 0.1)]
        self.save_csv(rr, os.path.join(self.processplace, 'pivot_data.csv'))

    def __call__(self):
        """
        对初始数据进行处理，产生两张通用表
        1.制作品类合并表
        2.制作物料合并表
        3.去除两表中的数据异常值
        4.产生供应商名称脱敏表
        """
        self.create_type_csv()
        logger.info('create category success')

        self.create_masplist_csv()
        logger.info('split hbjtabel')
        self.create_ma_comb()
        logger.info('create material success')

        self.deal_ma_csv()
        self.deal_type_csv()
        logger.info('deal over')

        self.create_basetable_csv()
        logger.info('create combase_table success')


if __name__ == '__main__':
    t = preprocess()
    t()
