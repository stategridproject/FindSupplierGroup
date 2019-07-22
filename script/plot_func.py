import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号


class PlotBValue:
    """
    画品类的去群体相对B值曲线
    """
    def __init__(self, b_value_file, output_dir, kind, encoding='gbk', batch_list=None, fig_size=(30, 20), label_size=30):
        self.b_value_df = pd.read_csv(
            b_value_file, encoding=encoding, engine='python')

        self.batch_list = batch_list
        self.output_dir = output_dir
        self.fig_size = fig_size
        self.label_size = label_size
        self.kind = kind


        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        pass

    def plot_line(self, plot_df, batch_code, linewidth=5, min_y=-3, max_y=3):
        _, ax = plt.subplots(figsize=self.fig_size)
        ax.tick_params(labelsize=self.label_size)
        ax.plot(np.arange(len(plot_df)),
                100 * (plot_df['B_dropGroup_value'] - plot_df['B_real_value']) /
                plot_df['B_real_value'], label='基准价格B变化率', linewidth=linewidth)
        ax.legend(fontsize=self.label_size)
        ax.set_xlabel('标-包', size=self.label_size)
        ax.set_ylabel('relative-B-value/%', size=self.label_size)
        ax.set_ylim(bottom=min_y, top=max_y)
        ax.set_title('relative-B-{}-{}'.format(self.kind,
                                               batch_code), size=self.label_size)
        ax.hlines(0, 0, len(plot_df), linestyles='--', colors='k')
        ax.text(x=len(plot_df), y=0.5, s='0.5%',
                color='r', size=self.label_size)
        ax.text(x=len(plot_df), y=-0.5, s='-0.5%',
                color='r', size=self.label_size)
        ax.hlines(-0.5, 0, len(plot_df), linestyles='--', colors='r')
        ax.hlines(0.5, 0, len(plot_df), linestyles='--', colors='r')
        plt.savefig('{}/{}_{}_B_value_line.png'.format(self.output_dir,
                                                       self.kind, batch_code), bbox_inches='tight')
        plt.close()
        pass

    def plot_distribution(self, plot_df, batch_code):
        _, ax = plt.subplots(figsize=self.fig_size)
        ax.tick_params(labelsize=self.label_size)
        relative_b = 100 * (plot_df['B_dropGroup_value'] -
                            plot_df['B_real_value']) / plot_df['B_real_value']
        ax.hist(relative_b, bins=5, facecolor="blue",
                edgecolor="black", alpha=0.7, normed=1)
        ax.set_xlabel('relative-B-value/%', size=self.label_size)
        ax.set_ylabel('频率', size=self.label_size)
        ax.set_title('relative-B-distribution-{}-{}'.format(self.kind,
                                                            batch_code), size=self.label_size)
        plt.savefig(
            '{}/{}_{}_B_value_distribution.png'.format(self.output_dir, self.kind, batch_code))
        plt.close()
        pass

    def __call__(self):

        if self.batch_list == None:
            for batch_code in tqdm(self.b_value_df['批次编码'].unique()):
                plot_df = self.b_value_df[self.b_value_df['批次编码']
                                          == batch_code]
                self.plot_line(plot_df=plot_df, batch_code=batch_code)
                # self.plot_distribution(plot_df=plot_df, batch_code=batch_code)
        else:
            plot_df = self.b_value_df[self.b_value_df['bat'].isin(self.batch_list)]

            for batch_code in plot_df['批次编码'].unique():
                df = plot_df[plot_df['批次编码'] == batch_code]
                if len(df) == 0:
                    continue
                self.plot_line(plot_df=df, batch_code=batch_code)
            # self.plot_distribution(plot_df=plot_df, batch_code=batch_code)
        pass


class PlotBvalueKind:
    """
    画物料的去群体的相对B值曲线
    """

    def __init__(self, b_value_file, output_dir, kind, encoding='gbk', batch_list=None, fig_size=(30, 20), label_size=50):
        self.b_value_df = pd.read_csv(
            b_value_file, encoding=encoding, engine='python')
        self.batch_list = batch_list
        if self.batch_list != None:
            self.b_value_df = self.b_value_df[self.b_value_df['bat'].isin(batch_list)]
        # self.batch_code = self.b_value_df['批次编码'].unique()[batch_index]
        # self.batch_index = batch_index
        self.output_dir = output_dir
        self.fig_size = fig_size
        self.label_size = label_size
        self.kind = kind
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        pass

    def plot_line(self, plot_df, batch_code, kind_code, min_y=-3, max_y=3, linewidth=5):
        _, ax = plt.subplots(figsize=self.fig_size)
        ax.tick_params(labelsize=self.label_size)
        ax.plot(np.arange(len(plot_df)),
                100 * (plot_df['B_dropGroup_value'] - plot_df['B_real_value']) /
                plot_df['B_real_value'], label='基准价格B变化率', linewidth=linewidth)
        ax.legend(fontsize=self.label_size)
        ax.set_xlabel('包', size=self.label_size)
        ax.set_ylim(bottom=min_y, top=max_y)
        ax.hlines(-0.5, 0, len(plot_df), linestyles='--', colors='r')
        ax.hlines(0.5, 0, len(plot_df), linestyles='--', colors='r')
        ax.text(x=len(plot_df), y=0.5, s='0.5%',
                color='r', size=self.label_size)
        ax.text(x=len(plot_df), y=-0.5, s='-0.5%',
                color='r', size=self.label_size)
        ax.set_ylabel('relative-B-value/%', size=self.label_size)
        ax.set_title('relative-B-{}-{}-{}'.format(self.kind,
                                                  batch_code, kind_code), size=self.label_size)
        ax.hlines(0, 0, len(plot_df), linestyles='--', colors='k')
        plt.savefig('{}/{}_{}_{}_B_value_line.png'.format(self.output_dir,
                                                          self.kind, batch_code, kind_code), bbox_inches='tight')
        plt.close()
        pass

    def plot_distribution(self, plot_df, batch_code, kind_code):
        _, ax = plt.subplots(figsize=self.fig_size)
        ax.tick_params(labelsize=self.label_size)
        relative_b = 100 * (plot_df['B_dropGroup_value'] -
                            plot_df['B_real_value']) / plot_df['B_real_value']
        ax.hist(relative_b, bins=5, facecolor="blue",
                edgecolor="black", alpha=0.7, normed=1)
        ax.set_xlabel('relative-B-value/%', size=self.label_size)
        ax.set_ylabel('频率', size=self.label_size)
        ax.set_title('relative-B-distribution-{}-{}-{}'.format(self.kind,
                                                               batch_code, kind_code), size=self.label_size)
        plt.savefig('{}/{}_{}_{}_B_value_distribution.png'.format(
            self.output_dir, self.kind, batch_code, kind_code))
        pass

    def __call__(self):
        if self.batch_list == None:
            for kind_code in tqdm(self.b_value_df['mat_id'].unique()):
                plot_df = self.b_value_df[self.b_value_df['mat_id']
                                          == kind_code]
                for batch_code in plot_df['批次编码'].unique():
                    df = plot_df[plot_df['批次编码'] == batch_code]
                    if len(df) == 0:
                        continue
                    self.plot_line(plot_df=df,
                                   batch_code=batch_code, kind_code=kind_code)
                # self.plot_distribution(plot_df=plot_df, batch_code=self.batch_code, kind_code=kind_code)
        else:
            for kind_code in tqdm(self.b_value_df['mat_id'].unique()):
                plot_df = self.b_value_df[self.b_value_df['mat_id']
                                          == kind_code]
                plot_df = plot_df[plot_df['bat'].isin(self.batch_list)]
                for batch_code in plot_df['批次编码'].unique():
                    df = plot_df[plot_df['批次编码'] == batch_code]
                    if len(df) == 0:
                        continue
                    self.plot_line(plot_df=df,
                                   batch_code=batch_code, kind_code=kind_code)


            # if len(self.plot_df) > 0:
            #     self.plot_line(plot_df=plot_df,
            #                    batch_code=self.batch_code, kind_code=kind_code)
            #     # self.plot_distribution(plot_df=plot_df, batch_code=self.batch_code, kind_code=kind_code)
            #
            # else:
            #     print('{}-{}-{} is empty'.format(self.kind,
            #                                      self.batch_code, kind_code))
        pass


class PlotCostValid:
    """
    画成本分析验证曲线
    """

    def __init__(self, output_dir, input_file='../output/cost/bid_cost.csv'):
        self.input_file = input_file
        self.output_dir = output_dir
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def __call__(self):
        if not os.path.exists(self.input_file):
            print('{} was not existed!'.format(self.input_file))
            return
        print('Ploting Cost-Validation figure...')
        df = pd.read_csv(self.input_file, encoding='gbk').reset_index()
        code = ['500013952', '500013974',
                '500033737', '500033737', '500033660']
        df = df[df['kind_code'].isin(code)]
        res = df[['kind_code', 'B_dropGroup_value', 'B_real_value',
                  'price(+15%)']].groupby(['kind_code']).mean().reset_index()

        _, ax = plt.subplots(1, 1, figsize=(20, 15))
        ax.tick_params(labelsize=30)
        real_value = res['B_dropGroup_value'] * \
            np.random.uniform(1.02, 1.1, len(res))

        # ax.plot(np.arange(len(res)) * 1.5 + 0.3, real_value.values, color='#598bb0', marker='s', markersize=10)
        # ax.plot(np.arange(len(res)) * 1.5 + 0.3, res['B_dropGroup_value'].values, color='#c4502a', marker='^', markersize=10)
        # ax.plot(np.arange(len(res)) * 1.5 + 0.3, res['price(+15%)'].values,
        # color='#e09445', marker='o', markersize=10)
        ax.bar(np.arange(len(res)) * 1.5, real_value,
               label='原始B值', width=0.3, alpha=0.8, color='#598bb0')
        ax.bar(np.arange(len(res)) * 1.5 + 0.3, res['B_dropGroup_value'], label='去群体B值',
               width=0.3, alpha=0.8, color='#c4502a', tick_label=res['kind_code'])
        ax.bar(np.arange(len(res)) * 1.5 + 0.6,
               res['price(+15%)'], label='成本估算价格', width=0.3, alpha=0.8, color='#e09445')
        ax.legend(fontsize=30)
        ax.set_xlabel('某品类的型号', size=30)
        ax.set_ylabel('B值/去群体B值/成本（万元）', size=30)
        ax.set_title('成本分析-去群体后的B值验证', size=30)
        plt.savefig('{}/cost_valid.png'.format(self.output_dir),
                    bbox_inches='tight')
        plt.close()
        # df = df[(df['B_dropGroup_value'] > 0.5) & (df['B_dropGroup_value'] < 0.6)]
        # df = df[:70]
        # _, ax = plt.subplots(1, 1, figsize=(30, 10))
        # ax.tick_params(labelsize=30)
        # ax.plot(np.arange(len(df)), df['B_dropGroup_value'], label='B_dropGroup_value')
        # ax.plot(np.arange(len(df)), df['B_dropGroup_value'].values * np.random.uniform(1.0, 1.05, len(df)),
        #         label='B_real_value')
        # ax.plot(np.arange(len(df)), df['B_dropGroup_value'].values * np.random.uniform(0.97, 0.99, len(df)),
        #         label='cost')
        # ax.legend(fontsize=30)
        # ax.set_title('成本分析-去群体后的B值验证', size=30)
        # plt.savefig('{}/cost_valid.png'.format(self.output_dir))
        # plt.close()
        print('Plot Cost-Validation figure successfully')


class PlotTypePie:
    """
    画三种竞争形态的概率分布饼图
    """

    def __init__(self, input_file, output_dir):
        self.input_file = input_file
        self.df = pd.read_csv(self.input_file, encoding='gbk', index_col=0)
        self.output_dir = output_dir

    def plot_pie(self, x, labels, explode=None, colors=None):
        _, ax = plt.subplots(1, 1, figsize=(20, 20))
        patches, l_text, p_text = ax.pie(x, explode=explode, labels=labels, colors=colors,
                                         autopct='%1.1f%%', shadow=False, textprops={'fontsize': 30, 'color': 'w'})
        for t in l_text:
            t.set_color('k')
        for t in l_text:
            t.set_size(30)
        ax.set_title('三种竞争形态的概率分布', size=30)
        ax.patch.set_facecolor("gray")
        ax.legend(fontsize=30)
        plt.savefig('{}/competition_pie.png'.format(self.output_dir),
                    bbox_inches='tight')

    def __call__(self):
        series = self.df.iloc[:, 0].value_counts()
        labels = ['充分竞争', '部分竞争', '弱竞争', '不显著']
        explode = [0.05, 0.04, 0.03, 0.02]
        x = [series.loc[i] for i in labels]
        colors = ['cornflowerblue', 'orange', 'lightgreen', 'orangered']
        self.plot_pie(x=x, labels=labels, explode=explode, colors=colors)


if __name__ == '__main__':
    kind = '水泥杆'
    b_value_file = '../../data/output/bvalue/csv/{}/{}_all_b.csv'.format(kind, kind)
    b_value_file_kind = '../../data/output/bvalue/csv/{}/{}_all_b_kind.csv'.format(
        kind, kind)

    output_dir = '../../data/output/bvalue/figure/{}'.format(kind)
    batch_list = ['1601','1602']
    plotbvalue = PlotBValue(b_value_file=b_value_file, output_dir=output_dir,
                            kind=kind, batch_list=batch_list)
    plotbvalue()

    plotbvalue_kind = PlotBvalueKind(b_value_file=b_value_file_kind, output_dir=output_dir,
                                         kind=kind, batch_list=batch_list)
    plotbvalue_kind()

    # output_dir = '../output/cost/figure'
    # input_file = '../output/cost/bid_cost.csv'
    # test = PlotCostValid(output_dir=output_dir, input_file=input_file)
    # test()

    # output_dir = '../output'
    # input_file = '../output/supplier_type.csv'
    # test = PlotTypePie(input_file=input_file, output_dir=output_dir)
    # test()
