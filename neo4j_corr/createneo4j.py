# coding=UTF-8
"""
Created on 2019年7月2日

@author: shkk1
"""
import os
import logging
import json
import pandas as pd
import numpy as np
from py2neo import Graph, Node, Relationship
from glob import glob
from tqdm import tqdm


def getlogger():
    basedir = os.path.dirname(os.path.realpath(__file__))
    p = os.path.join(basedir, "../data/output/log/")
    if not os.path.exists(p):
        os.makedirs(p)
    logfile = os.path.join(basedir, "../data/output/log/pac.log")
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


class Create_relation():
    """通过爬取数据建立供应商关系数据
    """

    def __init__(self, file_dir, company_name, entity_label_path, output_dir='../relation'):
        self.entity_label_path = entity_label_path
        if os.path.exists(self.entity_label_path):
            with open(entity_label_path, 'r', encoding='utf-8') as f:
                self.entity_label_dict = json.load(f)
        else:
            self.entity_label_dict = {}
        self.company_info = {'company_name': company_name}
        self.entity_label_dict[company_name] = 'Supplier'
        if os.path.exists(os.path.join(file_dir, 'MAIN.json')):
            with open(os.path.join(file_dir, 'MAIN.json'),
                      'r', encoding='utf-8') as f:
                self.company_info['bussiness_info'] = json.load(f)
        if os.path.exists(os.path.join(file_dir, 'MAIN.json')):
            with open(os.path.join(file_dir, 'FRXX.json'),
                      'r', encoding='utf-8') as f:
                self.company_info['legal_representative_info'] = json.load(f)
        if os.path.exists(os.path.join(file_dir, 'MAIN.json')):
            with open(os.path.join(file_dir, 'GDXX.json'),
                      'r', encoding='utf-8') as f:
                self.company_info['shareholder_info'] = json.load(f)

        self.relation = []
        self.output_dir = output_dir
        if not os.path.exists(self.output_dir):
            os.mkdir(self.output_dir)

    def get_relation(self):
        if not os.path.exists('{}/{}_relation.txt'.
                              format(self.output_dir,
                                     self.company_info['company_name'])):
            self.ParserInvestment()

            self.ParserBeneficiary()

            self.ParserMainStaff()

            self.ParserHoldingCompany()

            self.ParserShareHolder()

            self.ParserLegalRepresentative()
        # print('The population of {} relation: {}'.format(self.company_info['company_name'], len(self.relation)))
            return self.relation
        else:
            return None

    def ParserShareHolder(self):
        for company in self.company_info['shareholder_info']:
            if 'GSXX' not in self.company_info['shareholder_info'][company]:
                shareholder = company
                if len(self.company_info['shareholder_info'][company]) == 0:
                    continue
                self.merge_relation(
                    shareholder, self.company_info['company_name'], '投资')
                self.entity_label(shareholder, True)
                # self.entity_label(self.company_info['company_name'], False)
                temp_dict = self.company_info['shareholder_info'][company]['DRFR']
                for DRFR in temp_dict:
                    self.merge_relation(shareholder, DRFR, '法定代表人')
                    # self.entity_label(shareholder, True)
                    self.entity_label(DRFR, False)
                """
                法人代表的对外投资公司
                """
                temp_dict = self.company_info['shareholder_info'][company]['DWTZ']
                for DWTZ in temp_dict:
                    self.merge_relation(shareholder, DWTZ, '投资')
                    # self.entity_label(shareholder, True)
                    self.entity_label(DWTZ, False)
                """
                法人代表的在外任职
                """
                temp_dict = self.company_info['shareholder_info'][company]['ZWRZ']
                for ZWRZ in temp_dict:
                    self.merge_relation(shareholder, ZWRZ,
                                        temp_dict[ZWRZ]['职位'])
                    self.entity_label(ZWRZ, False)
                """法人代表的控股企业"""
                temp_dict = self.company_info['shareholder_info'][company]['KGQY']
                for KGQY in temp_dict:
                    self.merge_relation(shareholder, KGQY, '投资')
                    self.entity_label(KGQY, False)

            else:
                self.merge_relation(
                    company, self.company_info['company_name'], '投资')
                self.entity_label(company, False)
                # self.entity_label(self.company_info['company_name'], False)
                """
                股东公司的对外投资企业
                """
                temp_dict = self.company_info['shareholder_info'][company]['DWTZ']
                for DWTZ in temp_dict:
                    sub_company = DWTZ.split(' ')[0]
                    self.merge_relation(company, sub_company, '投资')
                    self.entity_label(sub_company, False)

                    for sub_representative in str(temp_dict[DWTZ]['法定代表人']).split('、'):
                        # if '人民币' in sub_representative:
                        #     print(temp_dict[DWTZ])
                        sub_representative = sub_representative.split(' ')[0]
                        self.merge_relation(
                            sub_representative, sub_company, '法定代表人')
                        self.entity_label(sub_representative, True)
                        self.entity_label(sub_company, False)
                    if 'GLQY' in temp_dict[DWTZ]:
                        if temp_dict[DWTZ]['GLQY'] == None:
                            continue
                        for GLQY in temp_dict[DWTZ]['GLQY']:

                            sub_sub_company = GLQY.split('  ')[0]

                            self.merge_relation(
                                sub_representative, sub_sub_company, temp_dict[DWTZ]['GLQY'][GLQY]['职位'])
                            # if '人民币' in sub_representative:
                            #     print(temp_dict[DWTZ]['GLQY'])
                            # self.entity_label(sub_representative, True)
                            self.entity_label(sub_sub_company, False)
                            for sub_sub_representative in temp_dict[DWTZ]['GLQY'][GLQY]['法定代表人'].split('、'):
                                sub_sub_representative = sub_sub_representative.split(' ')[
                                    0]
                                self.merge_relation(
                                    sub_sub_representative, sub_sub_company, '法定代表人')
                                self.entity_label(sub_sub_representative, True)
                                # self.entity_label(sub_sub_company, False)

                """
                股东公司的控股企业
                """
                temp_dict = self.company_info['shareholder_info'][company]['KGQY']
                for KGQY in temp_dict:
                    self.merge_relation(company, KGQY, '投资')
                    self.entity_label(company, False)
                    self.entity_label(KGQY, False)
                """
                股东公司的主要人员
                """
                temp_dict = self.company_info['shareholder_info'][company]['ZYRY']
                for ZYRY in temp_dict:
                    self.merge_relation(ZYRY, company, temp_dict[ZYRY]['职务'])
                    self.entity_label(company, False)
                    self.entity_label(ZYRY, True)
                    if temp_dict[ZYRY]['GLQY'] == None:
                        continue
                    for GLQY in temp_dict[ZYRY]['GLQY']:

                        sub_company = GLQY.split(' ')[0]
                        self.merge_relation(
                            ZYRY, sub_company, temp_dict[ZYRY]['GLQY'][GLQY]['职位'])
                        self.entity_label(sub_company, False)
                        for sub_representative in temp_dict[ZYRY]["GLQY"][GLQY]['法定代表人'].split('、'):
                            self.merge_relation(
                                sub_representative, sub_company, '法定代表人')
                            self.entity_label(sub_representative, True)

    def ParserLegalRepresentative(self):
        """
        法人代表的担任法人的公司
        """
        legal_representative = self.company_info['legal_representative_info']['name']
        temp_dict = self.company_info['legal_representative_info']['DRFR']
        for DRFR in temp_dict:
            self.merge_relation(legal_representative, DRFR, '法定代表人')
            self.entity_label(legal_representative, True)
            self.entity_label(DRFR, False)
        """
        法人代表的对外投资公司
        """
        temp_dict = self.company_info['legal_representative_info']['DWTZ']
        for DWTZ in temp_dict:
            self.merge_relation(legal_representative, DWTZ, '投资')
            self.entity_label(DWTZ, False)
        """
        法人代表的在外任职
        """
        temp_dict = self.company_info['legal_representative_info']['ZWRZ']
        for ZWRZ in temp_dict:
            self.merge_relation(legal_representative, ZWRZ,
                                temp_dict[ZWRZ]['职位'])
            self.entity_label(ZWRZ, True)
        """法人代表的控股企业"""
        temp_dict = self.company_info['legal_representative_info']['KGQY']
        for KGQY in temp_dict:
            self.merge_relation(legal_representative, KGQY, '投资')
            self.entity_label(KGQY, False)
        pass

    def ParserBranch(self):
        pass

    def ParserInvestment(self):
        """
        公司的对外投资公司
        """
        temp_dict = self.company_info['bussiness_info']['DWTZ']

        for DWTZ in temp_dict:
            self.merge_relation(self.company_info['company_name'], DWTZ, '投资')
            self.entity_label(DWTZ, False)
        pass

    def ParserMainStaff(self):
        """
        公司的主要人员
        """
        company = self.company_info['company_name']
        temp_dict = self.company_info['bussiness_info']['ZYRY']
        for ZYRY in temp_dict:
            self.merge_relation(ZYRY, company, temp_dict[ZYRY]['职务'])
            self.entity_label(ZYRY, True)
            self.entity_label(company, False)
            if temp_dict[ZYRY]['GLQY'] == None:
                continue
            for GLQY in temp_dict[ZYRY]['GLQY']:
                sub_company = GLQY.split('  ')[0]
                self.merge_relation(ZYRY, sub_company,
                                    temp_dict[ZYRY]['GLQY'][GLQY]['职位'])
                self.entity_label(ZYRY, True)
                self.entity_label(sub_company, False)
                for sub_legal_representative in temp_dict[ZYRY]['GLQY'][GLQY]['法定代表人'].split('、'):
                    sub_legal_representative = sub_legal_representative.split(' ')[
                        0]
                    self.merge_relation(
                        sub_legal_representative, sub_company, '法定代表人')
                    self.entity_label(sub_legal_representative, True)

        pass

    def ParserHoldingCompany(self):
        """
        公司的控股企业
        """
        company = self.company_info['company_name']
        temp_dict = self.company_info['bussiness_info']['KGQY']
        for KGQY in temp_dict:
            self.merge_relation(company, KGQY, '投资')
            self.entity_label(company, False)
            self.entity_label(KGQY, False)
        pass

    def ParserBeneficiary(self):
        """
        公司的最终受益人
        """
        temp_dict = self.company_info['bussiness_info']['ZZSY']
        for ZZSY in temp_dict:
            beneficiary = ZZSY.split(' ')[0]
            self.merge_relation(
                self.company_info['company_name'], beneficiary, '最终受益人')
            self.entity_label(beneficiary, True)
            if temp_dict[ZZSY]['GLQY'] == None:
                continue
            for GLQY in temp_dict[ZZSY]['GLQY']:
                sub_company = GLQY.split('  ')[0]
                self.merge_relation(
                    temp_dict[ZZSY]['GLQY'][GLQY]['法定代表人'], sub_company, '法定代表人')
                self.entity_label(temp_dict[ZZSY]['GLQY'][GLQY]['法定代表人'], True)
                self.entity_label(sub_company, False)
                self.merge_relation(beneficiary, sub_company,
                                    temp_dict[ZZSY]['GLQY'][GLQY]['职位'])

    def merge_relation(self, vertex1, vertex2, relation):
        if '人民币' in vertex1 or '人民币' in vertex2:
            return
        if ',' in relation:
            for r in relation.split(','):
                row = [vertex1.split(' ')[0], r, vertex2.split(' ')[0]]
                if row not in self.relation:
                    self.relation.append(row)
        elif '兼' in relation:
            for r in relation.split('兼'):
                row = [vertex1.split(' ')[0], r, vertex2.split(' ')[0]]
                if row not in self.relation:
                    self.relation.append(row)
        else:
            row = [vertex1.split(' ')[0], relation, vertex2.split(' ')[0]]
            if row not in self.relation:
                self.relation.append(row)

    def save_relations(self):
        with open('{}/{}_relation.txt'.format(self.output_dir, self.company_info['company_name']), 'w', encoding='utf-8') as f:
            for row in self.relation:
                f.writelines('\t'.join(row) + '\n')
        with open(self.entity_label_path, 'w', encoding='utf-8') as f:
            json.dump(self.entity_label_dict, f)

    def entity_label(self, entity, is_person=True):
        if entity not in self.entity_label_dict:
            if is_person:
                self.entity_label_dict[entity] = 'Person'
            else:
                self.entity_label_dict[entity] = 'Company'


class Create_neo4j():
    """通过已建立的关系数据建立neo4j图数据库
    """
    def __init__(self, file_list, enetity_label_path, output_dir, is_directed=True, is_initial=True, auth=('neo4j', 'blue0506'), ):
        self.is_initial = is_initial
        self.is_directed = is_directed
        with open(enetity_label_path, 'r', encoding='utf-8') as f:
            self.entity_label = json.load(f)
        self.output_dir = output_dir
        if not os.path.exists(self.output_dir):
            os.mkdir(self.output_dir)

        self.g = Graph("bolt://localhost:7687", auth=auth)
        # 初始化图
        if self.initial:
            self.g.delete_all()
            f1 = open('../data/processed/relation/company_written.txt',
                      'w', encoding='utf-8')
            self.relations = []
            for file in file_list:
                company = file.split('_')[0]
                f1.writelines(company + '\n')

                with open(file, 'r', encoding='utf-8') as f:
                    for row in f.readlines():
                        self.relations.append(row.strip().split('\t'))

            f1.close()
        else:
            with open('../relation/company_written.txt', 'r', encoding='utf-8') as f:
                self.company_written = [company.strip()
                                        for company in f.readlines()]

            f1 = open('../relation/company_written.txt', 'w', encoding='utf-8')
            self.relations = []
            for file in file_list:
                company = file.split('_')[0]
                f1.writelines(company + '\n')
                if company in self.company_written:
                    continue
                with open(file, 'r', encoding='utf-8') as f:
                    for row in f.readlines():
                        self.relations.append(row.strip().split('\t'))

            f1.close()
        pass

    def judge_label(self, name):
        if '企业' in name or '公司' in name:
            return 'Company'
        else:
            return 'Person'

    def plot_graph(self):
        tx = self.g.begin()
        Node_dict = {}
        for row in tqdm(self.relations):
            if len(row) < 3:
                continue
            if row[0] == '-' or row[2] == '-':
                continue
            e1, r, e2 = row
            if e1 not in Node_dict:
                if e1 not in self.entity_label:
                    label = self.judge_label(e1)
                else:
                    label = self.entity_label[e1]
                a = Node(label, name=e1)
                Node_dict[e1] = a
                tx.create(a)
            if e2 not in Node_dict:
                if e2 not in self.entity_label:
                    label = self.judge_label(e2)
                else:
                    label = self.entity_label[e2]
                b = Node(label, name=e2)
                Node_dict[e2] = b
                tx.create(b)
            ab = Relationship(Node_dict[e1], r, Node_dict[e2])
            tx.create(ab)
        tx.commit()
        # self.g.exists(ab)
        pass


if __name__ == '__main__':
    """通过爬取数据建立供应商关系数据
    """
    file_dirs = glob('../data/processed/bs_com/*')
    entity_label_path = '../data/processed/relation/label.json'
    for file_dir in tqdm(file_dirs):
        # file_dir = '../qichacha/镇江市华东化工电力设备总厂有限公司'
        company_name = os.path.basename(file_dir)
        # print(company_name)
        output_dir = '../data/processed/relation'
        parser = Create_relation(file_dir=file_dir,
                                      company_name=company_name,
                                      output_dir=output_dir,
                                      entity_label_path=entity_label_path)
        if len(parser.company_info) == 4:
            print(parser.company_info['company_name'])
            relation = parser.get_relation()
            if relation is not None:
                parser.save_relations()

    """通过已建立的关系数据建立neo4j图数据库
    """
    file_dir = '../data/processed/relation'
    output_dir = '../data/processed/relation'
    entity_label_path = '../data/processed/relation/label.json'
    file_list = glob('{}/*_relation.txt'.format(file_dir))
    is_directed = True
    is_initial = False
    auth = ('neo4j', '123456')
    test = Create_neo4j(file_list=file_list, output_dir=output_dir, is_directed=is_directed,
                             auth=auth, enetity_label_path=entity_label_path, is_initial=is_initial)
    test.plot_graph()
