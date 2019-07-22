# coding=UTF-8
'''
Created on 2019年7月2日

@author: shkk1
'''
from py2neo import Graph, NodeMatcher
import os
import logging


def getlogger():
    basedir = os.path.dirname(os.path.realpath(__file__))
    p = os.path.join(basedir, "../data/output/log/")
    if not os.path.exists(p):
        os.makedirs(p)
    logfile = os.path.join(basedir, "../data/output/log/query.log")
    logger = logging.getLogger('search')
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


class query_supplier:
    """neo4j图数据库搜索函数
    """
    def __init__(self, auth):
        self.graph = Graph("bolt://localhost:7687", auth=auth)
        self.nodematch = NodeMatcher(graph=self.graph)

        basedir = os.path.dirname(os.path.realpath(__file__))
        self.folderpath = os.path.join(basedir, "../data/processed/")
        if not os.path.exists(self.folderpath):
            os.makedirs(self.folderpath)

        self.folderp = os.path.join(basedir, "../data/processed/Allcom/")
        if not os.path.exists(self.folderp):
            os.makedirs(self.folderp)

        self.foldert = os.path.join(basedir, "../data/processed/haveCom/")
        if not os.path.exists(self.foldert):
            os.makedirs(self.foldert)

    def query(self, supplier1, supplier2, max_order=5):
        assert self.nodematch.match('Supplier', name=supplier1).first(
        ) is not None, 'Supplier {} was not existed!'.format(supplier1)
        assert self.nodematch.match('Supplier', name=supplier2).first(
        ) is not None, 'Supplier {} was not existed!'.format(supplier2)
        query_rule = '''match relation=(na:Supplier{name:\'%s\'})-[*1..%d]-(nb:Supplier{name:\'%s\'}) return relation''' % (
            supplier1, max_order, supplier2)
        relations = self.graph.run(query_rule)
        related_relations = [r.get('relation') for r in set(relations)]
        return related_relations

    def query_relatedsupplier(self, supplier1, supplier2, max_order=5):
        """通过neo4j图数据库查找两供应商间的关系
        """
        assert self.nodematch.match('Supplier',
                                    name=supplier1).first() is not None, 'Supplier {} was not existed!'.format(
            supplier1)

        query_rule = '''match (na:Supplier{name:\'%s\'})-[*1..%d]-(nb:Supplier{name:\'%s\'}) return nb''' % (
            supplier1, max_order, supplier2)
        suppliers = self.graph.run(query_rule)
        related_suppliers = [s.get('nb').get('name') for s in set(suppliers)]
        if supplier2 in related_suppliers:
            return True
        return False


if __name__ == '__main__':
    auth = ('neo4j', '123456')
    supplier1 = '萍乡强盛电瓷制造有限公司'
    supplier2 = '江西新龙电瓷电器制造有限公司'
    query = query_supplier(auth=auth)
    query.query(supplier1, supplier2, 5)
