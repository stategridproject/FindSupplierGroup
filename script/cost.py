import os
import pandas as pd
import numpy as np
from tqdm import tqdm


class CostEvaluation:
    def __init__(self, material_price_file, equation_file, bid_kind_file, need_file, output_dir):
        self.material_price_file = material_price_file
        self.equation_file = equation_file
        self.bid_kind_file = bid_kind_file
        self.output_dir = output_dir
        self.need_file = need_file
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        if not os.path.exists('{}/code_time.csv'.format(self.output_dir)):

            self.code_time = pd.read_excel(self.need_file, sheet_name='Sheet1', usecols=[3, 27]).rename({'分标编号': '批次编码'}, axis=1)
            self.code_time.to_csv('{}/code_time.csv'.format(self.output_dir), encoding='gbk')
        else:
            self.code_time = pd.read_csv('{}/code_time.csv'.format(self.output_dir), encoding='gbk', index_col=0)
        pass

    def cal_cost(self, material_price_file, equation_file):
        df1 = pd.read_csv(material_price_file)
        df2 = pd.read_excel(equation_file, sheet_name='Sheet1')
        df1['水泥'] = 400
        df1['砂'] = 150
        df1['石子'] = 80
        for col in df2.columns:
            param = df2[col]
            # print(param)
            df1[col] = df1['close'] * param[0] + df1['水泥'] * param[1] + df1['砂'] * param[2] + df1['石子'] * param[
                3] + param[7]
        return df1

    def merge_cost(self, df_bid, df_cost):
        df_cost.index = pd.to_datetime(df_cost.index)
        df_bid['创建时间'] = pd.to_datetime(df_bid['创建时间'])
        # prices = []

        for i in tqdm(df_bid.index):
            year = df_bid.loc[i, '创建时间'].year
            month = df_bid.loc[i, '创建时间'].month
            kind_code = int(df_bid.loc[i, 'kind_code'])
            # df1['price'] = 0

            if kind_code in df_cost.columns:
                df_bid.loc[i, 'cost'] = df_cost.loc[pd.to_datetime(pd.datetime(year, month-1, 1, 15)): pd.to_datetime(pd.datetime(year, month, 1, 15)), int(kind_code)].mean() / 10000 * 1.0
                # prices.append(price)
        df_bid['price(+10%)'] = df_bid['cost'] * 1.10
        df_bid['price(+15%)'] = df_bid['cost'] * 1.15
        return df_bid

    def __call__(self):
        df_cost = self.cal_cost(material_price_file=self.material_price_file, equation_file=self.equation_file).set_index('datetime')
        df_cost.to_csv('{}/成本.csv'.format(self.output_dir), encoding='gbk')
        df_bid = pd.read_csv(self.bid_kind_file, encoding='gbk', engine='python')
        df_bid = pd.merge(df_bid, self.code_time, on='批次编码', how='left')
        df_bid.drop_duplicates(['批次编码', '包号', 'mat_id'], inplace=True)
        df_bid['kind_code'] = df_bid['mat_id'].str.split('-', expand=True)[[1]]
        df_bid_cost = self.merge_cost(df_bid, df_cost)
        # df1['prices'] = np.array(prices)/10000
        df_bid_cost.dropna().to_csv('{}/bid_cost.csv'.format(self.output_dir), encoding='gbk')



if __name__ == "__main__":
    material_price_file = '..\\raw\\RBL8.csv'
    equation_file = '..\\raw\\水泥杆_成本公式.xlsx'
    bid_kind_file = '..\\output\\bvalue\\csv\\水泥杆\\水泥杆_all_b_kind.csv'
    need_file = '..\\raw\\需求一览表.xlsx'
    output_dir = '../output/cost'
    test = CostEvaluation(material_price_file=material_price_file, equation_file=equation_file, bid_kind_file=bid_kind_file, need_file=need_file, output_dir=output_dir)
    test()
