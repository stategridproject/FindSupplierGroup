import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
plt.rcParams['axes.unicode_minus']=False #用来正常显示负号
import random
import os
from glob import glob
random.seed(0)


class BValue:
    def __init__(self, input_file, group_file, kind, output_dir,
                 encoding='gbk', if_initial=True):
        self.input_file = input_file
        self.group_file = group_file
        self.kind = kind
        self.output_dir = output_dir
        self.encoding = encoding
        if if_initial:
            if os.path.exists(self.output_dir):
                for file in os.listdir(self.output_dir):
                    os.remove('{}/{}'.format(self.output_dir, file))
        if not os.path.exists(output_dir):
            os.makedirs(self.output_dir)

    def generate_kind_df(self):
        """
        从原始表中生成某个品类的投标信息
        :return:
        """
        if os.path.exists('{}/{}_投标信息.csv'.format(self.output_dir, self.kind)):
            df1 = pd.read_csv('{}/{}_投标信息.csv'.format(self.output_dir, self.kind), encoding=self.encoding, index_col=0)
            # df2 = pd.read_csv('{}/{}_中标信息.csv'.format(self.output_dir, self.kind), encoding=self.encoding, index_col=0)
            return df1
        else:
            df = pd.read_csv(self.input_file, encoding='gbk', index_col=0)
            df1 = df[df['分标名称'] == self.kind]
            df1.rename({'分标编号': '批次编码', '投标报价':'投标报价（万元）', '是否投标': '是否中标', '供应商名称': '中标供应商'}, axis=1, inplace=True)
            df1.loc[:, '投标报价（万元）'] = df1['投标报价（万元）'] / 10000
            dict = {0: '否', 1: '是'}
            df1.loc[:, '是否中标'] = list(map(lambda x: dict[x], df1['是否中标']))
            df1.loc[:, '包号'] = df1['包号'].str[1:].astype(int)
            df1.loc[:, '中标价格（万元）'] = df1['中标价格'] / 10000
            # df2 = df1[df1['是否中标'] == '是']
            df1 = df1[(df1['投标报价（万元）'] < 10 * df1['中标价格（万元）']) & (df1['投标报价（万元）'] > 0.01 * df1['中标价格（万元）'])]
            return df1[['bat', '批次编码', '包号', '中标供应商', '投标报价（万元）', '是否中标', '中标价格（万元）']]

    def reason_b_value(self, df):
        """
        用中标供应商投标价格生成合理的B值
        :param df:
        :return:
        """
        if os.path.exists('{}/{}_reason_b.csv'.format(self.output_dir, self.kind)):
            return pd.read_csv('{}/{}_reason_b.csv'.format(self.output_dir, self.kind), encoding=self.encoding, index_col=0)
        else:
            df1 = df[df['是否中标'] == '是']
            df2 = df[df['中标供应商'].isin(df1['中标供应商'])][['bat', '批次编码', '包号', '投标报价（万元）']].groupby(
                ['bat', '批次编码', '包号']).mean().reset_index().rename({'投标报价（万元）': 'reasonable_B_value'}, axis=1)
            return df2

    def real_b_value_without_equation(self, df):
        """
        计算实际的B值（平均值代替）
        :param df:
        :return:
        """
        if os.path.exists('{}/{}_real_b.csv'.format(self.output_dir, self.kind)):
            return pd.read_csv('{}/{}_real_b.csv'.format(self.output_dir, self.kind), encoding=self.encoding, index_col=0)
        else:
            df1 = df[['bat', '批次编码', '包号', '投标报价（万元）']].groupby(['bat', '批次编码', '包号']).mean().reset_index().rename(
                {'投标报价（万元）': 'B_real_value'}, axis=1)

            return df1

    def real_b_value_drop_group_without_equation(self, df, df_group):
        """
        去掉群体后计算实际的B值
        :param df:
        :param df_group:
        :return:
        """
        def drop_group(dff):
            bid = dff.iloc[0]['批次编码']
            group_list = df_group[df_group['time'] == bid]['Supplier'].values
            if len(group_list) > 0:
                return dff[~dff['中标供应商'].isin(group_list)]
            else:
                return dff
        if os.path.exists('{}/{}_real_drop_group_b.csv'.format(self.output_dir, self.kind)):
            return pd.read_csv('{}/{}_real_drop_group_b.csv'.format(self.output_dir, self.kind), encoding=self.encoding, index_col=0)
        else:
            # df1 = pd.DataFrame()
            # for code in df['批次编码'].unique():
            #     df1 = pd.concat([df1, drop_group(df[df['批次编码'] == code])], axis=0)
            df1 = df.groupby('批次编码').apply(drop_group).reset_index(drop=True)
            df2 = df1[['bat', '批次编码', '包号', '投标报价（万元）']].groupby(['bat', '批次编码', '包号']).mean().reset_index().rename(
                {'投标报价（万元）': 'B_dropGroup_value'}, axis=1)
            return df2

    # def real_b_value_dropnogroup_without_equotion(self, df, df_group):
    #     """
    #             去掉非群体后计算实际的B值
    #             :param df:
    #             :param df_group:
    #             :return:
    #             """
    #
    #     def drop_Nogroup(dff):
    #         bid = dff.iloc[0]['批次编码']
    #         group_list = df_group[df_group['time'] == bid]['Supplier'].values
    #         if len(group_list) > 0:
    #             return dff[dff['中标供应商'].isin(group_list)]
    #         else:
    #             return dff
    #
    #     if os.path.exists('{}/{}_real_dropNoGroup_B.csv'.format(self.output_dir, self.kind)):
    #         return pd.read_csv('{}/{}_real_dropNoGroup_B.csv'.format(self.output_dir, self.kind), encoding=self.encoding,
    #                            index_col=0)
    #     else:
    #         df1 = df.groupby(['批次编码']).apply(drop_Nogroup).reset_index(drop=True)
    #         df2 = df1[['批次编码', '包号', '投标报价（万元）']].groupby(['批次编码', '包号']).mean().reset_index().rename(
    #             {'投标报价（万元）': 'B_dropNoGroup_value'}, axis=1)
    #         return df2

    def merge_b_value(self):
        """
        合并三个B值
        :return:
        """
        if os.path.exists('{}/{}_all_b.csv'.format(self.output_dir, self.kind)):
            self.all_b_value_df = pd.read_csv('{}/{}_all_b.csv'.format(self.output_dir, self.kind), index_col=0, encoding=self.encoding)
        else:

            df1 = pd.merge(self.real_b_df[['bat', '批次编码', '包号', 'B_real_value']], self.reason_b_df, on=['bat', '批次编码', '包号'], how='left')
            df2 = pd.merge(self.dropGroup_b_df[['bat', '批次编码', '包号', 'B_dropGroup_value']], df1, on=['bat', '批次编码', '包号'], how='left')
            # df2 = pd.merge(self.dropNoGroup_b_df[['批次编码', '包号', 'B_dropNoGroup_value']], df2, on=['批次编码', '包号'], how='left')
            df2.sort_values(by=['bat', '批次编码', '包号'], inplace=True)
            df2['index'] = np.arange(len(df2))
            df3 = self.df_bid[['bat', '批次编码', '包号', '中标供应商']].groupby(by=['bat', '批次编码', '包号']).count().reset_index().rename({'中标供应商': '投标供应商数量'},
                                                                                                      axis=1)
            # df7 = pd.read_csv(group_file, encoding='gb2312', index_col=0)
            df4 = self.df_group[['time', 'Supplier']].groupby(['time']).count().reset_index().rename(
                {'time': '批次编码', 'Supplier': 'group_supplier'}, axis=1)
            df5 = pd.merge(df4, df3, on='批次编码', how='left')
            df6 = pd.merge(df2, df5, on=['bat', '批次编码', '包号'], how='left')
            # df11 = pd.read_csv('../data/{}_中标信息.csv'.format(kind), encoding='gbk')
            self.all_b_value_df = pd.merge(df6, self.df_win, on=['bat', '批次编码', '包号'], how='left')
            self.all_b_value_df.dropna(inplace=True)
            # self.all_b_value_df.drop('投标报价（万元）', axis=0, inplace=True)
            self.all_b_value_df.to_csv('{}/{}_all_b.csv'.format(self.output_dir, self.kind), encoding=self.encoding)

    # def plot_b(self):
    #     """
    #     可视化3个B值相对值
    #     :return:
    #     """
    #     _, ax = plt.subplots(figsize=(30,20))
    #     ax.tick_params(labelsize=30)
    #     ax.plot(self.all_b_value_df['index'], 100 * (self.all_b_value_df['B_dropGroup_value'] - self.all_b_value_df['B_real_value'])/self.all_b_value_df['B_real_value'], label='dropGroup-real_relative_B_value')
    #     # ax.plot(self.all_b_value_df['index'],
    #     #         100 * (self.all_b_value_df['B_dropNoGroup_value'] - self.all_b_value_df['B_real_value']) /
    #     #         self.all_b_value_df['B_real_value'], label='dropNoGroup-real_relative_B_value')
    #
    #     # ax.plot(self.all_b_value_df['index'], 100 * (self.all_b_value_df['B_real_value'] - self.all_b_value_df['reasonable_B_value'])/self.all_b_value_df['reasonable_B_value'], label='real-reasonable_relative_B_value')
    #     # ax.plot(self.all_b_value_df['index'], 100 * (self.all_b_value_df['B_dropGroup_value'] - self.all_b_value_df['reasonable_B_value'])/self.all_b_value_df['reasonable_B_value'], label='dropGroup-reasonable_relative_B_value')
    #     # ax.plot(self.all_b_value_df['index'], 100 * (self.all_b_value_df['reasonable_B_value'] - self.all_b_value_df['中标价格（万元）'])/self.all_b_value_df['中标价格（万元）'], label='reasonable-中标价格_relative_B_value')
    #     ax.legend(fontsize=30)
    #     ax.set_xlabel('标-包', size=30)
    #     ax.set_ylabel('relative-B-value/%', size=30)
    #     ax.set_title('relative-B-{}'.format(self.kind), size=30)
    #     ax.hlines(0, 0, len(self.all_b_value_df), linestyles='--', colors='k')
    #     # max_value = max([max(self.all_b_value_df['B_dropGroup_value']),
    #     #                  max(self.all_b_value_df['B_real_value']),
    #     #                  max(self.all_b_value_df['reasonable_B_value'])])
    #     #
    #     # min_value = min([min(self.all_b_value_df['B_dropGroup_value']),
    #     #                  min(self.all_b_value_df['B_real_value']),
    #     #                  min(self.all_b_value_df['reasonable_B_value'])])
    #     # xlines = self.all_b_value_df[self.all_b_value_df['包号'] == 1]['index']
    #     plot_bid = []
    #     for i in self.all_b_value_df.index:
    #         if self.all_b_value_df.loc[i, '批次编码'] in plot_bid:
    #             continue
    #         else:
    #             plt.vlines(self.all_b_value_df.loc[i, 'index'], 0, 1, colors='r', linestyles='--')
    #             plt.text(self.all_b_value_df.loc[i, 'index'], 1, self.all_b_value_df.loc[i, '批次编码'], fontdict={'size': 16, 'rotation': 90})
    #             plot_bid.append(self.all_b_value_df.loc[i, '批次编码'])
    #     plt.savefig('{}/{}_B_value.png'.format(self.output_dir, self.kind))
    #     plt.close()
    #
    #     # _, ax = plt.subplots()
    #     # ax.plot(self.all_b_value_df['index'],
    #     #         (self.all_b_value_df['B_dropGroup_value'] - self.all_b_value_df['B_real_value']) / self.all_b_value_df['B_real_value'])
    #     # # for i, x in enumerate(xlines):
    #     # #     plt.vlines(x, min_value, max_value, colors='r', linestyles='--')
    #     #     # plt.text(x, max_value, self.all_b_value_df.iloc[x]['批次编码'], fontdict={'size': 8, 'rotation': 90})
    #     # plt.savefig('{}/{}_B_relative.png'.format(self.output_dir, self.kind))
    #     # plt.close()

    def __call__(self):
        """
        运行方法
        :return:
        """
        self.df_bid = self.generate_kind_df()
        self.df_win = self.df_bid[self.df_bid['是否中标'] == '是']
        self.df_bid.to_csv('{}/{}_投标信息.csv'.format(self.output_dir, self.kind), encoding=self.encoding)
        # self.df_win.to_csv('{}/{}_中标信息.csv'.format(self.output_dir, self.kind), encoding=self.encoding)

        self.reason_b_df = self.reason_b_value(self.df_bid).dropna()
        self.reason_b_df.to_csv('{}/{}_reason_b.csv'.format(self.output_dir, self.kind), encoding=self.encoding)

        self.real_b_df = self.real_b_value_without_equation(self.df_bid).dropna()
        self.real_b_df.to_csv('{}/{}_real_b.csv'.format(self.output_dir, self.kind), encoding=self.encoding)

        self.df_group = pd.read_csv(self.group_file, encoding='gbk', engine='python')
        if len(self.df_group) == 0:
            return
        self.dropGroup_b_df = self.real_b_value_drop_group_without_equation(df=self.df_bid, df_group=self.df_group).dropna()
        self.dropGroup_b_df.to_csv('{}/{}_real_drop_group_b.csv'.format(self.output_dir, self.kind), encoding=self.encoding)

        # self.dropNoGroup_b_df = self.real_b_value_dropNoGroup_without_equotion(df=self.df_bid, df_group=self.df_group).dropna()
        # self.dropNoGroup_b_df.to_csv('{}/{}_real_dropNoGroup_B.csv'.format(self.output_dir, self.kind), encoding=self.encoding)
        self.merge_b_value()

        # self.plot_b()


class BValueKind:
    def __init__(self, input_file, group_file, kind, output_dir,
                 encoding='gbk', if_initial=False):
        self.input_file = input_file
        self.group_file = group_file
        self.kind = kind
        self.output_dir = output_dir
        self.encoding = encoding
        if if_initial:
            if os.path.exists(self.output_dir):
                for file in os.listdir(self.output_dir):
                    os.remove('{}/{}'.format(self.output_dir, file))
        if not os.path.exists(output_dir):
            os.makedirs(self.output_dir)

    def generate_kind_df(self):
        """
        从原始表中生成某个品类的投标信息
        :return:
        """
        if os.path.exists('{}/{}_投标信息_kind.csv'.format(self.output_dir, self.kind)):
            df1 = pd.read_csv('{}/{}_投标信息_kind.csv'.format(self.output_dir, self.kind), encoding=self.encoding, index_col=0)
            # df2 = pd.read_csv('{}/{}_中标信息.csv'.format(self.output_dir, self.kind), encoding=self.encoding, index_col=0)
            return df1
        else:
            df = pd.read_csv(self.input_file, encoding='gbk', index_col=0)
            df1 = df[df['分标名称'] == self.kind]
            df1.rename({'分标编号': '批次编码', '含税总单价':'含税单价（万元）'}, axis=1, inplace=True)
            # df1['投标报价（万元）'] = df1['投标报价（万元）'] / 10000
            # dict = {0: '否', 1: '是'}
            # df1['是否中标'] = list(map(lambda x: dict[x], df1['是否中标']))
            df1.loc[:, '包号'] = df1['包号'].str[1:].astype(int)
            # df1['中标价格（万元）'] = df1['中标价格'] / 10000
            # df2 = df1[df1['是否中标'] == '是']
            # df1 = df1[(df1['投标报价（万元）'] < 10 * df1['中标价格（万元）']) & (df1['投标报价（万元）'] > 0.01 * df1['中标价格（万元）'])]
            return df1[['bat', '批次编码', '包号', 'mat_id', '供应商名称', '含税单价（万元）', '是否中标']]

    def reason_b_value(self, df):
        """
        用中标供应商投标价格生成合理的B值
        :param df:
        :return:
        """
        if os.path.exists('{}/{}_reason_b_kind.csv'.format(self.output_dir, self.kind)):
            return pd.read_csv('{}/{}_reason_b_kind.csv'.format(self.output_dir, self.kind), encoding=self.encoding, index_col=0)
        else:
            df1 = df[df['是否中标'] == '是']
            df2 = df[df['供应商名称'].isin(df1['供应商名称'])][['bat', '批次编码', '包号', 'mat_id', '含税单价（万元）']].groupby(
                ['bat', '批次编码', '包号', 'mat_id']).mean().reset_index().rename({'含税单价（万元）': 'reasonable_B_value'}, axis=1)
            return df2

    def real_b_value_without_equation(self, df):
        """
        计算实际的B值（平均值代替）
        :param df:
        :return:
        """
        if os.path.exists('{}/{}_real_b_kind.csv'.format(self.output_dir, self.kind)):
            return pd.read_csv('{}/{}_real_b_kind.csv'.format(self.output_dir, self.kind), encoding=self.encoding, index_col=0)
        else:
            df1 = df[['bat', '批次编码', '包号', 'mat_id', '含税单价（万元）']].groupby(['bat', '批次编码', '包号', 'mat_id']).mean().reset_index().rename(
                {'含税单价（万元）': 'B_real_value'}, axis=1)
            return df1

    def real_b_value_drop_group_without_equation(self, df, df_group):
        """
        去掉群体后计算实际的B值
        :param df:
        :param df_group:
        :return:
        """
        def drop_group(dff):
            bid = dff.iloc[0]['批次编码']
            group_list = df_group[df_group['time'] == bid]['Supplier'].values
            if len(group_list) > 0:
                return dff[~dff['供应商名称'].isin(group_list)]
            else:
                return dff

        if os.path.exists('{}/{}_real_drop_group_b_kind.csv'.format(self.output_dir, self.kind)):
            return pd.read_csv('{}/{}_real_drop_group_b_kind.csv'.format(self.output_dir, self.kind), encoding=self.encoding, index_col=0)
        else:
            df1 = df.groupby(['批次编码']).apply(drop_group).reset_index(drop=True)
            df2 = df1[['bat', '批次编码', '包号', 'mat_id', '含税单价（万元）']].groupby(['bat', '批次编码', '包号', 'mat_id']).mean().reset_index().rename(
                {'含税单价（万元）': 'B_dropGroup_value'}, axis=1)
            return df2

    def merge_b_value(self):
        """
        合并三个B值
        :return:
        """
        if os.path.exists('{}/{}_all_b_kind.csv'.format(self.output_dir, self.kind)):
            self.all_b_value_df = pd.read_csv('{}/{}_all_b_kind.csv'.format(self.output_dir, self.kind), index_col=0, encoding=self.encoding)
        else:

            df1 = pd.merge(self.real_b_df[['bat', '批次编码', '包号', 'mat_id', 'B_real_value']], self.reason_b_df, on=['bat', '批次编码', '包号', 'mat_id'], how='left')
            df2 = pd.merge(self.dropGroup_b_df[['bat', '批次编码', '包号', 'mat_id', 'B_dropGroup_value']], df1, on=['bat', '批次编码', '包号', 'mat_id'], how='left')
            df2.sort_values(by=['bat', '批次编码', '包号','mat_id'], inplace=True)
            df2['index'] = np.arange(len(df2))
            df3 = self.df_bid[['bat', '批次编码', '包号', 'mat_id', '供应商名称']].groupby(by=['bat', '批次编码', '包号', 'mat_id']).count().reset_index().rename({'供应商名称': '投标供应商数量'},
                                                                                                      axis=1)
            # df7 = pd.read_csv(group_file, encoding='gb2312', index_col=0)
            df4 = self.df_group[['time', 'Supplier']].groupby(['time']).count().reset_index().rename(
                {'time': '批次编码', 'Supplier': 'group_supplier'}, axis=1)
            df5 = pd.merge(df4, df3, on='批次编码', how='left')
            df6 = pd.merge(df2, df5, on=['bat', '批次编码', '包号', 'mat_id'], how='left')
            # df11 = pd.read_csv('../data/{}_中标信息.csv'.format(kind), encoding='gbk')
            df6['bat'] = df6['bat'].astype(object)
            self.all_b_value_df= pd.merge(df6, self.df_win, on=['bat', '批次编码', '包号','mat_id'], how='left')
            self.all_b_value_df.dropna(inplace=True)
            self.all_b_value_df.to_csv('{}/{}_all_b_kind.csv'.format(self.output_dir, self.kind), encoding=self.encoding)

    # def plot_b(self):
    #     """
    #     可视化3个B值相对值
    #     :return:
    #     """
    #     _, ax = plt.subplots(figsize=(30,20))
    #     ax.tick_params(labelsize=30)
    #     ax.plot(self.all_b_value_df['index'], 100 * (self.all_b_value_df['B_dropGroup_value'] - self.all_b_value_df['B_real_value'])/self.all_b_value_df['B_real_value'], label='dropGroup-real_relative_B_value')
    #     # ax.plot(self.all_b_value_df['index'], 100 * (self.all_b_value_df['B_real_value'] - self.all_b_value_df['reasonable_B_value'])/self.all_b_value_df['reasonable_B_value'], label='real-reasonable_relative_B_value')
    #     # ax.plot(self.all_b_value_df['index'], 100 * (self.all_b_value_df['B_dropGroup_value'] - self.all_b_value_df['reasonable_B_value'])/self.all_b_value_df['reasonable_B_value'], label='dropGroup-reasonable_relative_B_value')
    #     # ax.plot(self.all_b_value_df['index'], 100 * (self.all_b_value_df['reasonable_B_value'] - self.all_b_value_df['中标价格（万元）'])/self.all_b_value_df['中标价格（万元）'], label='reasonable-中标价格_relative_B_value')
    #     ax.legend(fontsize=30)
    #     ax.set_xlabel('标-包-型号', size=30)
    #     ax.set_ylabel('relative-B-value/%', size=30)
    #     ax.set_title('relative-B-{}'.format(self.kind), size=30)
    #     ax.hlines(0, 0, len(self.all_b_value_df), colors='k', linestyles='--')
    #     # max_value = max([max(self.all_b_value_df['B_dropGroup_value']),
    #     #                  max(self.all_b_value_df['B_real_value']),
    #     #                  max(self.all_b_value_df['reasonable_B_value'])])
    #     #
    #     # min_value = min([min(self.all_b_value_df['B_dropGroup_value']),
    #     #                  min(self.all_b_value_df['B_real_value']),
    #     #                  min(self.all_b_value_df['reasonable_B_value'])])
    #     # xlines = self.all_b_value_df[self.all_b_value_df['包号'] == 1]['index']
    #     plot_bid = []
    #     for i in self.all_b_value_df.index:
    #         if self.all_b_value_df.loc[i, '批次编码'] in plot_bid:
    #             continue
    #         else:
    #             plt.vlines(self.all_b_value_df.loc[i, 'index'], 0, 1, colors='r', linestyles='--')
    #             plt.text(self.all_b_value_df.loc[i, 'index'], 1, self.all_b_value_df.loc[i, '批次编码'], fontdict={'size': 16, 'rotation': 90})
    #             plot_bid.append(self.all_b_value_df.loc[i, '批次编码'])
    #     plt.savefig('{}/{}_B_value_kind.png'.format(self.output_dir, self.kind))
    #     plt.close()
    #
    #     # _, ax = plt.subplots()
    #     # ax.plot(self.all_b_value_df['index'],
    #     #         (self.all_b_value_df['B_dropGroup_value'] - self.all_b_value_df['B_real_value']) / self.all_b_value_df['B_real_value'])
    #     # # for i, x in enumerate(xlines):
    #     # #     plt.vlines(x, min_value, max_value, colors='r', linestyles='--')
    #     #     # plt.text(x, max_value, self.all_b_value_df.iloc[x]['批次编码'], fontdict={'size': 8, 'rotation': 90})
    #     # plt.savefig('{}/{}_B_relative.png'.format(self.output_dir, self.kind))
    #     # plt.close()

    def __call__(self):
        """
        运行方法
        :return:
        """
        self.df_bid = self.generate_kind_df()
        self.df_win = self.df_bid[self.df_bid['是否中标']=='是']
        self.df_bid.to_csv('{}/{}_投标信息_kind.csv'.format(self.output_dir, self.kind), encoding=self.encoding)
        # self.df_win.to_csv('{}/{}_中标信息.csv'.format(self.output_dir, self.kind), encoding=self.encoding)

        self.reason_b_df = self.reason_b_value(self.df_bid).dropna()
        self.reason_b_df.to_csv('{}/{}_reason_b_kind.csv'.format(self.output_dir, self.kind), encoding=self.encoding)

        self.real_b_df = self.real_b_value_without_equation(self.df_bid).dropna()
        self.real_b_df.to_csv('{}/{}_real_b_kind.csv'.format(self.output_dir, self.kind), encoding=self.encoding)

        self.df_group = pd.read_csv(self.group_file, encoding='gb2312',engine='python')
        self.dropGroup_b_df = self.real_b_value_drop_group_without_equation(df=self.df_bid, df_group=self.df_group).dropna()
        self.dropGroup_b_df.to_csv('{}/{}_real_drop_group_b_kind.csv'.format(self.output_dir, self.kind), encoding=self.encoding)

        self.merge_b_value()

        # self.plot_b()


if __name__ == '__main__':
    # kind = '10kV电力电缆'
#     # output_dir = '../output/bvalue/csv/{}'.format(kind)
#     #
#     # input_file = '../processed/pivot_data.csv'
#     # group_file = '../output/model1_group/{}_category.csv'.format(kind)
#     # if_initial = True
#     # test = BValue(kind=kind, output_dir=output_dir, group_file=group_file, input_file=input_file, if_initial=if_initial)
#     # test()
#     #
#     # input_file_kind = '../processed/matr_all.csv'
#     # group_file_kind = '../output/model2_group/{}_material.csv'.format(kind)
#     # test_kind = BValueKind(input_file=input_file_kind, kind=kind, output_dir=output_dir, group_file=group_file_kind)
#     # test_kind()
#     if_initial = True
#     input_file = '../data/pivot_data.csv'
#     kind = '水泥杆'
#     output_dir = '../output/bvalue/csv/{}'.format(kind)
#     group_file = '../data/水泥杆_group.csv'
#     test = BValue(kind=kind, output_dir=output_dir, group_file=group_file, input_file=input_file,
#                       if_initial=if_initial)
#     test()

    if_initial = True
    input_file = '../data/matr_all.csv'
    kind = '水泥杆'
    output_dir = '../output/bvalue/csv/水泥杆'
    group_file = '../data/水泥杆_group.csv'
    test = BValueKind(kind=kind, output_dir=output_dir, group_file=group_file, input_file=input_file, if_initial=if_initial)
    test()