# coding=UTF-8
'''
Created on 2019年7月2日

@author: shkk1
'''
from script.basema import Model_mate_group
from script.basepac import Model_cate_group
from script.association import Association
from script.cal_apriori import CalAprior
from script.B_value import BValue, BValueKind
from script.plot_func import PlotBValue, PlotBvalueKind, PlotCostValid
from script.draw_pearson import Draw_pearson_heat
from script.preprocess import preprocess
from script.cost import CostEvaluation
from script.drawrandom import DrawRandom
import os
import pandas as pd
import shutil


def get_B_value(kind, batname):
    input_file = '../../data/processed/pivot_data.csv'
    input_kind_file = '../../data/processed/matr_all.csv'
    kind = kind
    output_dir = '../../data/output/bvalue/csv/{}'.format(kind)
    group_file = '../../data/output/model_group/{}_group.csv'.format(kind)
    if_initial = True
    cal_cate_value = BValue(kind=kind, output_dir=output_dir,
                            group_file=group_file, input_file=input_file,
                            if_initial=if_initial)
    cal_cate_value()
    cal_mate_value = BValueKind(input_file=input_kind_file, kind=kind,
                                output_dir=output_dir, group_file=group_file)
    cal_mate_value()
    """绘制图片
    """
    output_dir = '../../data/output/figure/line/{}'.format(kind)
    b_value_file = '../../data/output/bvalue/csv/{}/{}_all_b.csv'.format(kind,
                                                                         kind)
    if batname:
        pass
    else:
        batname = None
    b_value_file_kind = '../../data/output/bvalue/csv/{}/{}_all_b_kind.csv'.format(kind, kind)
    plotbvalue = PlotBValue(b_value_file=b_value_file, output_dir=output_dir,
                            kind=kind,
                            batch_list=batname)
    plotbvalue()
    plotbvalue_kind = PlotBvalueKind(b_value_file=b_value_file_kind,
                                     output_dir=output_dir, kind=kind,
                                     batch_list=batname)
    plotbvalue_kind()


def cal_cost():
    material_price_file = '../../data/raw\\RBL8.csv'
    equation_file = '../../data/raw\\水泥杆_成本公式.xlsx'
    bid_kind_file = '../../data/output\\bvalue\\csv\\水泥杆\\水泥杆_all_b_kind.csv'
    need_file = '../../data/raw\\需求一览表.xlsx'
    output_dir = '../../data//output/cost'
    test = CostEvaluation(material_price_file=material_price_file,
                          equation_file=equation_file,
                          bid_kind_file=bid_kind_file,
                          need_file=need_file, output_dir=output_dir)
    test()
    """绘制图片
    """
    output_dir = '../../data/output/figure/cost'
    input_file = '../../data/output/cost/bid_cost.csv'
    test = PlotCostValid(output_dir=output_dir, input_file=input_file)
    test()


def initial(tyname):
    for i in tyname:
        placefile = os.path.join('../../data/processed/', i)
        if os.path.isdir(placefile):
            shutil.rmtree(placefile)


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    """制造中间数据集
    """
    create_data = preprocess()
    create_data()
    """计算群体
    """
    tyname = ['水泥杆']  # ['水泥杆']
    batname = []  # 可选值, 1601, 1602, 16XZ, 1701, 1702, 1703, 1801, 1802, 1804
    part_1 = Model_mate_group(tyname=tyname, batname=batname)
    part_2 = Model_cate_group(tyname=tyname, batname=batname)
    part_3 = Association(tyname=tyname, batname=batname)
    initial(tyname)
    part_1()
    part_2()
    part_3()
    """基于群体识别的组合模型
    """
    cal_result = CalAprior(tyname=tyname)
    cal_result()
    """计算B-value并绘制折线图
    """
    for kind in tyname:
        get_B_value(kind=kind, batname=batname)
    """水泥杆成本分析
    """
    cal_cost()
    """绘制pearson热力图，查看聚类结果
    """
    drawbat = ['1601']  # 需选择批次
    drawtype = 'afselect'  # 未 聚类:beKmeans ，聚类后:afKmeans，选择群体:afselect
    draw_pearson = Draw_pearson_heat(tyname=tyname, batname=drawbat, drawtype=drawtype)
    draw_pearson()
    """
    绘制随机选取群体B-value图
    """
    for i in tyname:
        drawt = DrawRandom(i)
        drawt()
