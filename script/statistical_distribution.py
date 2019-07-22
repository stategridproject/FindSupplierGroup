import pandas as pd
import numpy as np
import os


class StaticalDistribution:
    def __init__(self, output_dir, input_dir='../output/bvalue/csv'):
        self.output_dir = output_dir
        self.input_dir = input_dir
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def cal_file_type(self, file, kind):
        def cal_code_type(dff):
            p_value = 0.7
            b_value = 0.5
            res = np.array((dff['B_dropGroup_value'] - dff['B_real_value']) / dff['B_real_value'] * 100)
            if (len(np.where(res > b_value)[0]) / len(res) > p_value) | (
                    len(np.where(res < -b_value)[0]) / len(res) > p_value):
                Type = '部分竞争'
            elif (len(np.where(res >= 0.1)[0]) / len(res) > 0.1) & (len(np.where(res <= - 0.1)[0]) / len(res) > 0.1):
                Type = '充分竞争'
            elif (len(np.where(res <= b_value)[0]) / len(res) > p_value) & (
                    len(np.where(res >= -b_value)[0]) / len(res) > p_value):
                Type = '弱竞争'
            else:
                Type = '不显著'
            return Type

        df = pd.read_csv(file, index_col=0, encoding='gbk', engine='python')
        df_res = df.groupby(['批次编码']).apply(cal_code_type)
        df_res = pd.DataFrame(df_res)
        df_res['kind'] = kind
        return df_res
        pass

    def __call__(self):
        df_type = pd.DataFrame()
        for root_dir, _, base_files in os.walk(self.input_dir):
            kind = os.path.basename(root_dir)
            for base_file in base_files:
                if 'all_b' in base_file:
                    file = os.path.join(root_dir, base_file)
                    df_type = pd.concat([df_type, self.cal_file_type(file=file, kind=kind)], axis=0)

        df_type.to_csv('{}/supplier_type.csv'.format(self.output_dir), encoding='gbk')


def cal_file_type(file, kind):
    def cal_code_type(dff):
        p_value = 0.7
        b_value = 0.5
        res = np.array((dff['B_dropGroup_value'] - dff['B_real_value'])/dff['B_real_value'] * 100)
        if (len(np.where(res > b_value)[0])/len(res) > p_value) | (len(np.where(res < -b_value)[0])/len(res) > p_value):
            Type = '部分竞争'
        elif (len(np.where(res >= 0.1)[0])/len(res) > 0.1) & (len(np.where(res <= - 0.1)[0])/len(res) > 0.1):
            Type = '充分竞争'
        elif (len(np.where(res <= b_value)[0])/len(res) > p_value) & (len(np.where(res >= -b_value)[0])/len(res) > p_value):
            Type = '集体抱团'
        else:
            Type = '不显著'
        return Type

    df = pd.read_csv(file, index_col=0, encoding='gbk')
    df_res = df.groupby(['批次编码']).apply(cal_code_type)
    df_res = pd.DataFrame(df_res)
    df_res['kind'] = kind
    return df_res


if __name__ == '__main__':
    # csv_dir = '../output/bvalue/csv'
    #
    # df_type = pd.DataFrame()
    # for root_dir, _, base_files in os.walk(csv_dir):
    #     kind = os.path.basename(root_dir)
    #     for base_file in base_files:
    #         if 'all_b' in base_file:
    #             file = os.path.join(root_dir, base_file)
    #             df_type = pd.concat([df_type, cal_file_type(file=file, kind=kind)], axis=0)
    #
    #
    # df_type.to_csv('../output/supplier_type.csv', encoding='gbk')
    # pass
    input_dir = '../../data/output/bvalue/csv'
    output_dir = '../../data/output/figure/pie'
    test = StaticalDistribution(input_dir=input_dir, output_dir=output_dir)
    test()
