# coding=UTF-8
'''
Created on 2019年7月5日

@author: shkk1
'''
import logging
import os
from tqdm import tqdm
import numpy as np
import pandas as pd


def getlogger():
    basedir = os.path.dirname(os.path.realpath(__file__))
    p = os.path.join(basedir, "../../data/output/log/")
    if not os.path.exists(p):
        os.makedirs(p)
    logfile = os.path.join(basedir, "../../data/output/log/calap.log")
    logger = logging.getLogger('calap')
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


class Apriori:
    def __init__(self, dataset, category, k, min_support, min_conf,
                 output_dir, is_sorted=True):
        self.dataset = dataset
        self.category = category
        self.k = k
        self.min_support = min_support
        self.min_conf = min_conf
        self.output_dir = output_dir
        self.is_sorted = is_sorted
        if not os.path.exists(self.output_dir):
            os.mkdir(self.output_dir)
        self.output_dir = os.path.join(self.output_dir,
                                       '{}_k-{}_min_support-{}_min_conf-{}'
                                       .format(self.category, self.k,
                                               self.min_support, min_conf))
        if not os.path.exists(self.output_dir):
            os.mkdir(self.output_dir)

    def create_C1(self):
        """
        Create frequent candidate 1-itemset C1 by scaning data set.
        Args:
            data_set: A list of transactions.
            Each transaction contains several items.
        Returns:
            C1: A set which contains all frequent candidate 1-itemsets
        """
        C1 = set()
        for t in self.dataset:
            for item in t:
                item_set = frozenset([item])
                C1.add(item_set)
        return C1

    def is_apriori(self, Ck_item, Lksub1):
        """
        Judge whether a frequent candidate k-itemset satisfy Apriori property.
        Args:
            Ck_item: a frequent candidate k-itemset in Ck which
                     contains all frequent candidate k-itemsets.
            Lksub1: Lk-1, a set which contains all frequent
                    candidate (k-1)-itemsets.
        Returns:
            True: satisfying Apriori property.
            False: Not satisfying Apriori property.
        """
        for item in Ck_item:
            sub_Ck = Ck_item - frozenset([item])
            if sub_Ck not in Lksub1:
                return False
        return True

    def create_Ck(self, Lksub1, k):
        """
        Create Ck, a set which contains all all frequent candidate k-itemsets
        by Lk-1's own connection operation.
        Args:
            Lksub1: Lk-1, a set which contains all frequent
            candidate (k-1)-itemsets.
            k: the item number of a frequent itemset.
        Return:
            Ck: a set which contains all all frequent candidate k-itemsets.
        """
        Ck = set()
        len_Lksub1 = len(Lksub1)
        list_Lksub1 = list(Lksub1)
        for i in tqdm(range(len_Lksub1)):
            for j in range(i + 1, len_Lksub1):
                l1 = list(list_Lksub1[i])
                l2 = list(list_Lksub1[j])
                l1.sort()
                l2.sort()
                if l1[0:k - 2] == l2[0:k - 2]:
                    Ck_item = list_Lksub1[i] | list_Lksub1[j]
                    # pruning
                    if self.is_apriori(Ck_item, Lksub1):
                        Ck.add(Ck_item)
        return Ck

    def generate_Lk_by_Ck(self, Ck, min_support):
        """
        Generate Lk by executing a delete policy from Ck.
        Args:
            data_set: A list of transactions. Each transaction
                      contains several items.
            Ck: A set which contains all all frequent candidate k-itemsets.
            min_support: The minimum support.
            support_data: A dictionary. The key is frequent
                          itemset and the value is support.
        Returns:
            Lk: A set which contains all all frequent k-itemsets.
        """

        print('Generating frequent {}-itemset length {}...'
              .format(len(list(Ck)[0]), len(list(Ck))))
        Lk = set()
        item_count = {}
        for t in tqdm(self.dataset):
            for item in Ck:
                if item.issubset(t):
                    if item not in item_count:
                        item_count[item] = 1
                    else:
                        item_count[item] += 1
        t_num = float(len(self.dataset))
        for item in item_count:
            if (item_count[item] / t_num) >= min_support:
                Lk.add(item)
                self.support_data[item] = item_count[item] / t_num
        return Lk

    def generate_L(self):
        """
        Generate all frequent itemsets.
        Args:
            data_set: A list of transactions. Each
                      transaction contains several items.
            k: Maximum number of items for all frequent itemsets.
            min_support: The minimum support.
        Returns:
            L: The list of Lk.
            support_data: A dictionary. The key is frequent
                          itemset and the value is support.
        """
        self.support_data = {}
        C1 = self.create_C1()
        L1 = self.generate_Lk_by_Ck(C1, self.min_support)
        Lksub1 = L1.copy()
        self.L = []
        self.L.append(Lksub1)
        for i in range(2, self.k + 1):
            Ci = self.create_Ck(Lksub1, i)
            if len(list(Ci)) == 0:
                break
            Li = self.generate_Lk_by_Ck(Ci, self.min_support)
            Lksub1 = Li.copy()
            self.L.append(Lksub1)

    def generate_big_rules(self):
        """
        Generate big rules from frequent itemsets.
        Args:
            L: The list of Lk.
            support_data: A dictionary. The key is
                          frequent itemset and the value is support.
            min_conf: Minimal confidence.
        Returns:
            big_rule_list: A list which contains all big rules.
                           Each big rule is represented
                           as a 3-tuple.
        """
        self.big_rule_list = []
        sub_set_list = []
        for i in tqdm(range(0, len(self.L))):
            for freq_set in self.L[i]:
                for sub_set in sub_set_list:
                    if sub_set.issubset(freq_set):
                        conf = self.support_data[freq_set] / \
                            self.support_data[freq_set - sub_set]
                        big_rule = (freq_set - sub_set, sub_set, conf)
                        if conf >= self.min_conf and big_rule not in self.big_rule_list:
                            # print freq_set-sub_set, " => ", sub_set, "conf:
                            # ", conf
                            self.big_rule_list.append(big_rule)
                sub_set_list.append(freq_set)

    def save_result(self):
        for LK in self.L:
            if len(list(LK)) > 0:
                flag = 'frequnet-{}-itemsets'.format(len(list(LK)[0]))
                with open('{}/{}.txt'.format(self.output_dir, flag),
                          'w', encoding='GBK') as f:
                    if self.is_sorted:
                        LK_sorted = self.sort_support_data(LK)
                        for freq_tuple in LK_sorted:
                            f.writelines(
                                ','.join(freq_tuple[0]) + '\t' +
                                str(freq_tuple[1]) + '\n')
                    else:
                        for freq_set in LK:
                            f.writelines(','.join(freq_set) + '\t' +
                                         str(self.support_data[freq_set]) +
                                         '\n')

    def sort_support_data(self, LK, reverse=True):
        LK_dict = {}
        for freq_set in LK:
            LK_dict[freq_set] = self.support_data[freq_set]
        LK_sorted = sorted(
            LK_dict.items(), key=lambda x: x[1], reverse=reverse)
        return LK_sorted

    def sort_conf_data(self, reverse=True):
        rule_dict = {}
        for item in self.big_rule_list:
            rule_dict[','.join(item[0]) + '=>' + ','.join(item[1])] = item[2]
        rule_sorted = sorted(
            rule_dict.items(), key=lambda x: x[1], reverse=reverse)
        return rule_sorted
        pass


class CalAprior():
    def __init__(self, tyname):
        self.tyname = tyname

        basedir = os.path.dirname(os.path.realpath(__file__))
        self.folderpath = os.path.join(basedir, "../../data/processed/")
        if not os.path.exists(self.folderpath):
            os.makedirs(self.folderpath)
        self.outputplace = os.path.join(
            basedir, "../../data/output/model_group/")
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

    def load_material_dataset_csv(self, filefolder, i, lenpac):
        """将读取内容转化为
        """
        def generate_event(df):
            return list(df['name'])
        df = pd.read_csv(os.path.join(filefolder, i), index_col=0,
                         encoding='gbk', engine='python')
        if (df.empty):
            return {}
        if i.split('.')[0] == 'category':
            tmp = df.groupby(['num']).apply(generate_event).values.tolist()
            for i in range(lenpac):
                self.rst = self.rst + tmp

        else:
            tmp = df.groupby(['包号', 'num']).apply(
                generate_event).values.tolist()
            self.rst = self.rst + tmp

    def run_ap(self, datasets, nameid):
        """进行apori分析
        """
        k = 5
        min_support = 0.005
        min_conf = 0.6
        output_dir = os.path.join(
            self.folderpath, './{}/aptest'.format(nameid))
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        is_sorted = True
        for categroy, dataset in datasets.items():
            test = Apriori(dataset=dataset, category=categroy, k=k,
                           min_support=min_support, min_conf=min_conf,
                           output_dir=output_dir, is_sorted=is_sorted)
            test.generate_L()
    #         test.generate_big_rules()
            test.save_result()

    def deal_ap(self, tname):
        """读取4个模型结果，联合计算apriori
        """
        filefolder = os.path.join(self.folderpath,
                                  './{}/'.format(tname))
        listdir = self.get_dir(filefolder)
        for t in listdir:  # 批次
            self.rst = []
            folderplace = os.path.join(filefolder, t)
            listfile = self.get_file(folderplace)
            lenpac = [int(i.split('.')[0])
                      for i in listfile if i.endswith('.txt')][0]
            for i in listfile:  # 四个模型
                if i.endswith('.csv'):
                    timeid = t
                    self.load_material_dataset_csv(folderplace, i, lenpac)
            dataset = {timeid: np.array(self.rst)}
            self.run_ap(dataset, tname)

    def get_ap_all(self, tyname, fileplace):
        output_dir = os.path.join(
            self.folderpath, './{}/aptest'.format(tyname))
        try:
            listdir = self.get_dir(output_dir)
        except BaseException:
            logger.info('no group {}'.format(tyname))
            return
        tmp = pd.DataFrame(columns=['time', 'Supplier', 'num'])
        count = 0
        ncount = 0
        for i in listdir:
            folderpalce = os.path.join(output_dir, i)
            listfile = self.get_file(folderpalce)
            group_list = []
            for t in listfile:
                if t.split('-')[1] == '1':
                    continue
                txt_r = self.readtxt(os.path.join(folderpalce, t))
                lentxt = len(txt_r)
                for n in txt_r.split('\n')[:int(lentxt / 10)]:
                    tlist = n.split('\t')[0].split(',')
                    group_list.append(tlist)
            while True:
                isstop = True
                for index1, t in enumerate(group_list):
                    for index2, j in enumerate(group_list):
                        if index1 < index2:
                            if len(set(t) & set(j)) >= 1:
                                group_list[index1] = list(set(t) | set(j))
                                group_list.remove(j)
                                isstop = False
                if isstop:
                    break
            timeid = i.split('_')[0]

            for n in group_list:
                for j in n:
                    if j == '':
                        continue
                    tmp.loc[ncount] = [timeid, j, count]
                    ncount += 1
                count += 1
        self.save_csv(tmp, fileplace)

    def __call__(self):
        """对列表内品类进行计算群体
        """
        for i in self.tyname:
            saveplace = os.path.join(
                self.outputplace, '{}_group.csv'.format(i))
            self.deal_ap(i)
            self.get_ap_all(i, saveplace)


if __name__ == '__main__':
    tyname = ['水泥杆']  # []遍历所有
    batname = []  # 可选值, 1601, 1602, 16XZ, 1701, 1702, 1703, 1801, 1802, 1804
    run = CalAprior(tyname=tyname)
    run()
