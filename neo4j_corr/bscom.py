# coding=UTF-8
"""
Created on 2019年5月20日

@author: shkk1
"""
import os
import logging
import bs4
import json
import time
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains


def getlogger():
    basedir = os.path.dirname(os.path.realpath(__file__))
    p = os.path.join(basedir, "../../data/output/log/")
    if not os.path.exists(p):
        os.makedirs(p)

    logfile = os.path.join(basedir, "../../data/output/log/BS4.log")
    logger = logging.getLogger('getcom')
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


class SpiderSupplier():
    def __init__(self):

        self.invip = 'https://www.qichacha.com/search?key={}'
        self.Comurl = 'https://www.qichacha.com{}#base'

        self.ComFR = 'https://www.qichacha.com{}'

        self.chromepath = 'D:\Program Files\chromedriver\chromedriver.exe'
# self.chromedata='C:\Users\shkk1\AppData\Local\Google\Chrome\User Data'
        basedir = os.path.dirname(os.path.realpath(__file__))
        self.folderpath = os.path.join(basedir, "../data/raw")
        if not os.path.exists(self.folderpath):
            os.makedirs(self.folderpath)

        self.processplace = os.path.join(basedir, "../data/processed/bs_com")
        if not os.path.exists(self.processplace):
            os.makedirs(self.processplace)

    def output(self, filename, Dict):
        with open(filename, 'w') as f:
            json.dump(Dict, f)

    def getcom_csv(self):
        """生成需爬取所有供应商清单
        """
        datafile = os.path.join(self.folderpath, '开标一览表.xlsx')
        savefile = os.path.join(self.processplace, 'Com_all.csv')
        df = pd.read_excel(datafile, sheep='Sheet1', header=0)

        com_list = set(df['供应商名称'].values)
        save_df = pd.DataFrame(columns=['com'])
        save_df['com'] = list(com_list)
        save_df.to_csv(savefile, encoding="GBK")

    def getcom_ip(self):
        """先获取所有供应商链接
        """
        datafile = os.path.join(self.processplace, 'Com_all.csv')
        savefile = os.path.join(self.processplace, 'Com_ip.json')
        option = webdriver.ChromeOptions()
        option.add_argument(r"user-data-dir={}".format(self.chormedata))

        driver = webdriver.Chrome(
            executable_path=self.chromepath, options=option)
        dict_re = {}
        df = pd.read_csv(datafile, index_col=0, encoding='GBK')
        df_list = df['com'].values.tolist()
        for i in df_list:
            url = self.invip.format(i)
            logger.info(i)
            driver.get(url)
            time.sleep(1)
            soup = BeautifulSoup(driver.page_source, "html.parser")

            for tr in soup.find('table', class_='m_srchList').children:
                if isinstance(tr, bs4.element.Tag):
                    tds = tr('td')
                    href = tds[2].find('a').attrs['href']
                    dict_re[i] = href
                    break
        self.output(savefile, dict_re)

    def add_gs(self, soup, isget=False):
        """工商信息
        """
        dict_re = {}
        nn = soup.find('section', id='Cominfo').table.children
        for tr in nn:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('td')

#                 tds[3].find('a').text
                if isget:
                    try:
                        url = tds[3].find('a').attrs['href']
                        self.FRXX = self.get_FRXX(url)
                    except:
                        logger.info('no 法人')
                        self.FRXX = {}

        nn = soup.find(
            'section', id='Cominfo').table.next_sibling.next_sibling.children
        count = 0
        for tr in nn:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('td')
                for i in tds:
                    str_main = i.text.strip().split('\n')[0]
                    count += 1
                    if count % 2 == 1:
                        numid_1 = str_main
                    else:
                        numid_2 = str_main
                    if count % 2 == 0:
                        dict_re[numid_1] = numid_2
        return dict_re

    def add_gd(self, soup):
        """股东信息
        """
        dic_ip = {}
        dict_re = {}
        lenlist = []
        T_main_id = 'partnerslist'
        count = 0
        try:
            nn = soup.find('section', id=T_main_id).table.children
        except:
            self.GDXX = {}
            logger.info('no 股东信息')
            return dict_re
        ttnum = 0
        for tr in soup.find('section', id=T_main_id).children:
            if isinstance(tr, bs4.element.Tag):
                ttnum += 1
        for tr in soup.find('section', id=T_main_id).table.tbody.tr.children:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('th')
                if len(tds) == 0:
                    if count == 0:
                        count += 1
                        continue
                    lenlist.append(tr.text)
        conid = len(lenlist)
        for tr in soup.find('section', id=T_main_id).table.tbody.children:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('td')
                if len(tds) == 0:
                    continue

                if ttnum == 4:
                    listnum = 3
                else:
                    listnum = 1
                rlist = tds[listnum].text.strip().split(' ')
                if len(rlist) > 1:
                    tt = [i for i in rlist if i != '']
                    if len(tt[:-2]) == 0:
                        rlist = tt
                    else:
                        rlist = tt[:-2]
                if len(rlist) != 1:
                    dict_re[rlist[0]] = {'other': rlist[1:]}
                else:
                    dict_re[rlist[0]] = {}
                try:
                    GD_ip = tds[1].find('a').attrs['href']
                    dic_ip[tds[1].find('a').text.strip()] = {'ipurl': GD_ip}
                except:
                    pass
                count = 0
                for n in lenlist:
                    tlist = tds[-conid + count].text.strip().split('\n')
                    tlist = [i.strip() for i in tlist if i != '']
                    tlist = [i for i in tlist if i != '']
                    dict_re[rlist[0]][n] = tlist[0] if len(
                        tlist) == 1 else tlist
                    count += 1
        self.GDXX = self.get_gdxx(dic_ip)

        return dict_re

    def add_mp(self, soup):
        """主要人员
        """
        dict_re = {}
        lenlist = []
        count = 0
        try:
            nn = soup.find('section', id='Mainmember').table
        except:
            logger.info('no 主要人员')
            return dict_re
        for tr in nn:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('th')
                for m in tds:
                    if count <= 1:
                        count += 1
                        continue
                    lenlist.append(m.text.strip().split('\n')[0])
                break
        nn = soup.find('section', id='Mainmember').table.tbody
        for tr in nn:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('td')
                if len(tds) == 0:
                    continue
                rlist = tds[1].text.strip().split(' ')
                dict_re[rlist[0]] = {}
                dict_re[rlist[0]]['GLQY'] = self.get_con(
                    tds[1].find('a').attrs['href'])
                dict_re[rlist[0]][lenlist[-1]] = tds[-1].text.strip()
        return dict_re

    def add_dw(self, soup):
        """对外投资
        """
        dict_re = {}
        lenlist = []
        count = 0
        T_main_id = 'touzilist'

        try:
            nn = soup.find('section', id=T_main_id).table.children
        except:
            logger.info('no 对外投资')
            return dict_re
        while(True):
            count = 0
            lenlist = []
            ttnum = 0
            for tr in soup.find('section', id=T_main_id).children:
                if isinstance(tr, bs4.element.Tag):
                    ttnum += 1
            for tr in soup.find('section', id=T_main_id).table.tbody.tr.children:
                if isinstance(tr, bs4.element.Tag):
                    tds = tr('th')
                    if len(tds) == 0:
                        if count == 0:
                            count += 1
                            continue
                        lenlist.append(tr.text)
            conid = len(lenlist)
            for tr in soup.find('section', id=T_main_id).table.tbody.children:
                if isinstance(tr, bs4.element.Tag):
                    tds = tr('td')
                    if len(tds) == 0:
                        continue

                    if ttnum == 5:
                        listnum = 3
                        urlnum = 8
                    else:
                        listnum = 1
                        urlnum = 4

                    rlist = tds[listnum].text.strip()
                    dict_re[rlist] = {}
                    try:
                        urlcon = tds[urlnum].find('a').attrs['href']
                        dict_re[rlist]['GLQY'] = self.get_con(urlcon)
                    except:
                        dict_re[rlist]['GLQY'] = {}
                    count = 0
                    for n in lenlist:
                        if count == 0:
                            tlist = tds[-conid + count -
                                        1].text.strip().split('\n')
                        else:
                            tlist = tds[-conid +
                                        count].text.strip().split('\n')
                        tlist = [i.strip() for i in tlist if i != '']
                        tlist = [i for i in tlist if i != '']
                        dict_re[rlist][n] = tlist[0] if len(
                            tlist) == 1 else tlist
                        count += 1
            try:
                islen = soup.find('section', id=T_main_id).find(
                    'ul', class_='pagination')('li')
            except:
                islen = []
            number = self.cal_number(islen)
            if number == 0:
                break
            else:
                if ttnum == 5:
                    catpage = 3
                else:
                    catpage = 2
                xpath_id = '//*[@id="{}"]/div[{}]/nav/ul/li[{}]/a'.format(
                    T_main_id, catpage, number + 1)
                js_ = self.drive.find_element_by_xpath(
                    xpath_id).get_attribute('href')
                self.drive.execute_script(js_)
                time.sleep(0.5)
                soup = BeautifulSoup(self.drive.page_source, 'html.parser')

        return dict_re

    def add_gd_dw(self, soup):
        """股东信息-对外投资
        """
        dict_re = {}
        lenlist = []

        T_main_id = 'touzilist'
        try:
            nn = soup.find('section', id=T_main_id).table.children
        except:
            logger.info('no 对外投资')
            return dict_re
        while True:
            count = 0
            lenlist = []
            ttnum = 0
            for tr in soup.find('section', id=T_main_id).children:
                if isinstance(tr, bs4.element.Tag):
                    ttnum += 1
            for tr in soup.find('section', id=T_main_id).table.children:
                if isinstance(tr, bs4.element.Tag):
                    tds = tr('th')
                    for m in tds:
                        if count <= 1:
                            count += 1
                            continue
                        lenlist.append(m.text.strip().split('\n')[0])
                    break
            conid = len(lenlist)
            for tr in soup.find('section', id=T_main_id).table.tbody.children:
                if isinstance(tr, bs4.element.Tag):
                    tds = tr('td')
                    if len(tds) == 0:
                        continue

                    if ttnum == 5:
                        listnum = 3
                        urlnum = 8
                    else:
                        listnum = 1
                        urlnum = 4

                    rlist = tds[listnum].text.strip()
                    dict_re[rlist] = {}
                    try:
                        url_ipt = tds[urlnum].find('a').attrs['href']
                        if url_ipt[:3] == '/pl':
                            dict_re[rlist]['GLQY'] = self.get_con_gd(
                                tds[urlnum].find('a').attrs['href'])
                    except:
                        dict_re[rlist]['GLQY'] = {}
                    self.drive.switch_to.window(self.test_handle)
                    count = 0
                    for n in lenlist:
                        if count == 0:
                            tlist = tds[-conid + count -
                                        1].text.strip().split('\n')
                        else:
                            tlist = tds[-conid +
                                        count].text.strip().split('\n')
                        tlist = [i.strip() for i in tlist if i != '']
                        tlist = [i for i in tlist if i != '']
                        dict_re[rlist][n] = tlist[0] if len(
                            tlist) == 1 else tlist
                        count += 1
            try:
                islen = soup.find('section', id=T_main_id).find(
                    'ul', class_='pagination')('li')
            except:
                islen = []
            number = self.cal_number(islen)
            if number == 0:
                break
            else:
                if ttnum == 5:
                    catpage = 3
                else:
                    catpage = 2
                xpath_id = '//*[@id="{}"]/div[{}]/nav/ul/li[{}]/a'.format(
                    T_main_id, catpage, number + 1)
                js_ = self.drive.find_element_by_xpath(
                    xpath_id).get_attribute('href')
                self.drive.execute_script(js_)
                time.sleep(0.5)
                soup = BeautifulSoup(self.drive.page_source, 'html.parser')

        return dict_re

    def add_sy(self, soup):
        """最终受益人
        """
        dict_re = {}
        lenlist = []
        count = 0
        try:
            nn = soup.find('section', id='syrlist').table.children
        except:
            logger.info('no 最终受益人')
            return dict_re
        for tr in nn:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('th')
                for m in tds:
                    if count <= 1:
                        count += 1
                        continue
                    lenlist.append(m.text.strip().split('\n')[0])
                break
        conid = len(lenlist)
        for tr in soup.find('section', id='syrlist').table.tbody.children:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('td')
                if len(tds) == 0:
                    continue
                rlist = tds[1].text.strip()
                dict_re[rlist] = {}
                dict_re[rlist]['GLQY'] = self.get_con(
                    tds[1].find('a').attrs['href'])
                count = 0
                for n in lenlist:
                    if count == 0:
                        tlist = tds[-conid + count -
                                    1].text.strip().split('\n')
                    else:
                        tlist = tds[-conid + count].text.strip().split('\n')
                    tlist = [i.strip() for i in tlist if i != '']
                    tlist = [i for i in tlist if i != '']
                    dict_re[rlist][n] = tlist[0] if len(tlist) == 1 else tlist
                    count += 1

        return dict_re

    def add_fz(self, soup):
        """分支机构
        """
        dic_ip = {}
        dict_re = {}
        count = 0
        try:
            nn = soup.find('section', id='Subcom').table.children
        except:
            logger.info('no 分支机构')
            return dict_re
        for tr in soup.find('section', id='Subcom').table.children:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('td')
                if len(tds) == 0:
                    continue
                for n in tds:
                    count += 1
                    if count % 2 == 0:
                        dic_ip[n.text.strip()] = n.find('a').attrs['href']

        tt = self.get_fzxx(dic_ip)

        return tt

    def add_bg(self, soup):
        """变更记录
        """
        dict_re = {}
        lenlist = []
        count = 0
        try:
            nn = soup.find('section', id='Changelist').table.children
        except:
            logger.info('no 变更记录')
            return dict_re
        for tr in nn:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('th')
                for m in tds:
                    if count == 0:
                        count += 1
                        continue
                    lenlist.append(m.text.strip().split('\n')[0])
                break
        conid = len(lenlist)
        for tr in soup.find('section', id='Changelist').table.tbody.children:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('td')
                if len(tds) == 0:
                    continue
                rlist = tds[0].text.strip()
                dict_re[rlist] = {}
                count = 0
                for n in lenlist:
                    tlist = tds[count + 1].text.strip().split('\n')
                    tlist = [i.strip() for i in tlist if i != '']
                    tlist = [i for i in tlist if i != '']
                    dict_re[rlist][n] = tlist[0] if len(tlist) == 1 else tlist
                    count += 1
        return dict_re

    def add_kg(self, soup):
        """控股企业
        """
        dict_re = {}
        lenlist = []
        count = 0
        T_main_id = 'holdcolist'
        try:
            nn = soup.find('section', id=T_main_id).table
        except:
            logger.info('no 控股企业')
            return dict_re
        islen = soup.find(
            'section', id=T_main_id).table.next_sibling.next_sibling('li')
        number = self.cal_number(islen)

        for tr in nn:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('th')
                for m in tds:
                    if count <= 1:
                        count += 1
                        continue
                    lenlist.append(m.text.strip().split('\n')[0])
                break
        conid = len(lenlist)
        for tr in soup.find('section', id=T_main_id).table.tbody.children:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('td')
                if len(tds) == 0:
                    continue
                rlist = tds[1].text.strip()
                dict_re[rlist] = {}
                count = 0
                for n in lenlist:
                    tlist = tds[count + 1].text.strip().split('\n')
                    tlist = [i.strip() for i in tlist if i != '']
                    tlist = [i for i in tlist if i != '']
                    dict_re[rlist][n] = tlist[0] if len(tlist) == 1 else tlist
                    count += 1

        while True:
            if number == 0:
                break
            else:
                xpath_id = '//*[@id="{}"]/div[2]/nav/ul/li[{}]/a'.format(
                    T_main_id, number + 1)
                js_ = self.drive.find_element_by_xpath(
                    xpath_id).get_attribute('href')
                self.drive.execute_script(js_)
                time.sleep(1)
                soup = BeautifulSoup(self.drive.page_source, 'html.parser')
                for tr in soup.find('section', id=T_main_id).table.tbody.children:
                    if isinstance(tr, bs4.element.Tag):
                        tds = tr('td')
                        if len(tds) == 0:
                            continue
                        rlist = tds[1].text.strip()
                        dict_re[rlist] = {}
                        count = 0
                        for n in lenlist:
                            tlist = tds[-conid +
                                        count].text.strip().split('\n')
                            tlist = [i.strip() for i in tlist if i != '']
                            tlist = [i for i in tlist if i != '']
                            dict_re[rlist][n] = tlist[0] if len(
                                tlist) == 1 else tlist
                            count += 1
                islen = soup.find(
                    'section', id=T_main_id).table.next_sibling.next_sibling('li')
                number = self.cal_number(islen)
        return dict_re

    def add_ty(self, soup):
        """同业分析
        """
        dict_re = {}
        for i in range(3):
            try:
                on_stop = self.drive.find_element_by_xpath(
                    '//*[@id="industryAnalysisTable"]/tbody/tr[8]')
                break
            except:
                if i == 2:
                    logger.info('no 同业分析')
                    return dict_re
                continue

        ActionChains(self.drive).move_to_element(on_stop).perform()
        self.drive.execute_script("window.scrollBy(0, 200)")
        time.sleep(1)
        count = 1
        soup = BeautifulSoup(self.drive.page_source, "html.parser")
        for tr in soup.find('table', id='industryAnalysisTable').tbody.children:
            if isinstance(tr, bs4.element.Tag):
                on_stop = self.drive.find_element_by_xpath(
                    '//*[@id="industryAnalysisTable"]/tbody/tr[{}]'.format(count))
                ActionChains(self.drive).move_to_element(on_stop).perform()
                time.sleep(1)
                soup = BeautifulSoup(self.drive.page_source, "html.parser")
                count += 1
                tds = tr('td')
                dict_re[tds[0].text.strip()] = soup.find(
                    'div', id='industryAnalysisTitle').text

        return dict_re

    def get_fzxx(self, dic_ip):
        dictt = {}
        for i in dic_ip.keys():
            url = self.Comurl.format(dic_ip[i])
            self.drive.switch_to.window(self.test_handle)
            self.drive.get(url)
            soup = BeautifulSoup(self.drive.page_source, "html.parser")
            dictt[i] = {}
            dictt[i]['GSXX'] = self.add_gs(soup, False)
            dictt[i]['ZZSY'] = self.add_sy(soup)
            dictt[i]['BGJL'] = self.add_bg(soup)
        self.drive.switch_to.window(self.base_handle)
        return dictt

    def get_gdXX(self, dic_ip):
        dictt = {}
        js = 'window.open("https://www.sogou.com");'
        self.drive.execute_script(js)
        handles = self.drive.window_handles
        self.other_handle = None
        for handle in handles:
            if (handle != self.base_handle) & (handle != self.test_handle):
                self.other_handle = handle

        for i in dic_ip.keys():
            dictt[i] = {}
            if dic_ip[i]['ipurl'] == '/firm_f37bbffa58c81ff76ac86aefa056e8be.html':
                continue
            if dic_ip[i]['ipurl'][:3] == '/pl':
                dictt[i] = self.get_FRXX(dic_ip[i]['ipurl'])
            elif dic_ip[i]['ipurl'][:3] == '/fi':
                url = self.Comurl.format(dic_ip[i]['ipurl'])
                self.drive.switch_to.window(self.test_handle)
                self.drive.get(url)
                soup = BeautifulSoup(self.drive.page_source, "html.parser")
                try:
                    dictt[i]['GSXX'] = self.add_gs(soup, False)
                except:
                    logger.info('香港公司')
                    continue
                if '基金' not in i:
                    dictt[i]['DWTZ'] = self.add_gd_dw(soup)
                dictt[i]['KGQY'] = self.add_kg(soup)
                dictt[i]['ZYRY'] = self.add_mp(soup)
                dictt[i]['BGJL'] = self.add_bg(soup)

        self.drive.switch_to.window(self.other_handle)
        self.drive.close()
        self.drive.switch_to.window(self.base_handle)
        return dictt

    def get_frxx(self, urlt):
        url = self.ComFR.format(urlt)
        self.drive.switch_to.window(self.test_handle)
        self.drive.get(url)
        time.sleep(0.5)
        dict_re = {}
        soup = BeautifulSoup(self.drive.page_source, 'html.parser')

        dict_re['name'] = soup.find('div', id='peopleHeader').div('div')[
            2].text.strip()

        dict_re['DRFR'] = self.get_t_dr(soup)
        dict_re['DWTZ'] = self.get_t_dw(soup)
        dict_re['ZWRZ'] = self.get_t_rz(soup)
        dict_re['KGQY'] = self.get_t_kg(soup)

        dict_re['BZXR'] = self.get_h_bz(soup)
        dict_re['SXZX'] = self.get_h_sx(soup)

        dict_re['LS'] = self.get_h_base(soup)

        self.drive.switch_to.window(self.base_handle)
        return dict_re

    def get_h_base(self, soup):
        """历史
        """

        dict_re = {}

        for k in soup.find('div', id='history').children:
            if isinstance(k, bs4.element.Tag):
                title = k('div')[0].find(class_='title').text
                nn = k.table
                if nn is None:
                    logger.info('no {}'.format(title))
                else:
                    dict_re[title] = {}
                    count = 0
                    lenlist = []
                    for tr in nn.children:
                        if isinstance(tr, bs4.element.Tag):
                            tds = tr('th')
                            for m in tds:
                                if count <= 1:
                                    count += 1
                                    continue
                                lenlist.append(m.text.strip().split('\n')[0])
                            break
                    conid = len(lenlist)
                    for tr in nn.tbody.children:
                        if isinstance(tr, bs4.element.Tag):
                            tds = tr('td')
                            if len(tds) == 0:
                                continue
                            rlist = tds[1].text.strip()
                            dict_re[title][rlist] = {}
                            count = 0
                            for n in lenlist:
                                tlist = tds[-conid +
                                            count].text.strip().split('\n')
                                tlist = [i.strip() for i in tlist if i != '']
                                tlist = [i for i in tlist if i != '']
                                dict_re[title][rlist][n] = tlist[0] if len(
                                    tlist) == 1 else tlist
                                count += 1
        return dict_re

    def get_h_sx(self, soup):
        """失信被执行人
        """
        count = 0
        lenlist = []
        dict_re = {}

        T_main_id = 'shixin'
        try:
            nn = soup.find('div', id=T_main_id).table.children
        except:
            logger.info('no 失信被执行人')
            return dict_re

        for tr in nn:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('th')
                for m in tds:
                    if count <= 1:
                        count += 1
                        continue
                    lenlist.append(m.text.strip().split('\n')[0])
                break
        conid = len(lenlist)

        for tr in soup.find('div', id=T_main_id).table.tbody.children:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('td')
                if len(tds) == 0:
                    continue
                rlist = tds[1].text.strip()
                dict_re[rlist] = {}
                count = 0
                for n in lenlist:
                    tlist = tds[-conid + count].text.strip().split('\n')
                    tlist = [i.strip() for i in tlist if i != '']
                    tlist = [i for i in tlist if i != '']
                    dict_re[rlist][n] = tlist[0] if len(tlist) == 1 else tlist
                    count += 1
        return dict_re

    def get_h_bz(self, soup):
        """被执行人
        """
        count = 0
        lenlist = []
        dict_re = {}

        T_main_id = 'zhixing'
        try:
            nn = soup.find('div', id=T_main_id).table.children
        except:
            logger.info('no 被执行人')
            return dict_re

        for tr in nn:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('th')
                for m in tds:
                    if count <= 1:
                        count += 1
                        continue
                    lenlist.append(m.text.strip().split('\n')[0])
                break
        conid = len(lenlist)

        for tr in soup.find('div', id=T_main_id).table.tbody.children:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('td')
                if len(tds) == 0:
                    continue
                rlist = tds[1].text.strip()
                dict_re[rlist] = {}
                count = 0
                for n in lenlist:
                    tlist = tds[-conid + count].text.strip().split('\n')
                    tlist = [i.strip() for i in tlist if i != '']
                    tlist = [i for i in tlist if i != '']
                    dict_re[rlist][n] = tlist[0] if len(tlist) == 1 else tlist
                    count += 1
        return dict_re

    def get_t_kg(self, soup):
        """控股企业
        """
        count = 0
        lenlist = []
        dict_re = {}

        T_main_id = 'holdcolist'
        try:
            nn = soup.find('div', id=T_main_id).table.children
        except:
            logger.info('no 控股企业')
            return dict_re
        islen = soup.find(
            'div', id=T_main_id).table.next_sibling.next_sibling('li')
        number = self.cal_number(islen)

        for tr in nn:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('th')
                for m in tds:
                    if count <= 1:
                        count += 1
                        continue
                    lenlist.append(m.text.strip().split('\n')[0])
                break
        conid = len(lenlist)

        for tr in soup.find('div', id=T_main_id).table.tbody.children:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('td')
                if len(tds) == 0:
                    continue
                rlist = tds[1].text.strip()
                dict_re[rlist] = {}
                count = 0
                for n in lenlist:
                    tlist = tds[-conid + count].text.strip().split('\n')
                    tlist = [i.strip() for i in tlist if i != '']
                    tlist = [i for i in tlist if i != '']
                    dict_re[rlist][n] = tlist[0] if len(tlist) == 1 else tlist
                    count += 1
        while True:
            if number == 0:
                break
            else:
                xpath_id = '//*[@id="{}"]/div[2]/nav/ul/li[{}]/a'.format(
                    T_main_id, number + 1)
                for i in range(3):
                    try:
                        js_ = self.drive.find_element_by_xpath(
                            xpath_id).get_attribute('href')
                        break
                    except:
                        time.sleep(0.5)
                self.drive.execute_script(js_)
                time.sleep(0.5)
                soup = BeautifulSoup(self.drive.page_source, 'html.parser')
                for tr in soup.find('div', id=T_main_id).table.tbody.children:
                    if isinstance(tr, bs4.element.Tag):
                        tds = tr('td')
                        if len(tds) == 0:
                            continue
                        rlist = tds[1].text.strip()
                        dict_re[rlist] = {}
                        count = 0
                        for n in lenlist:
                            tlist = tds[-conid +
                                        count].text.strip().split('\n')
                            tlist = [i.strip() for i in tlist if i != '']
                            tlist = [i for i in tlist if i != '']
                            dict_re[rlist][n] = tlist[0] if len(
                                tlist) == 1 else tlist
                            count += 1
                islen = soup.find(
                    'div', id=T_main_id).table.next_sibling.next_sibling('li')
                number = self.cal_number(islen)
        return dict_re

    def get_t_rz(self, soup):
        """在外任职
        """
        count = 0
        lenlist = []
        dict_re = {}

        T_main_id = 'postofficelist'
        try:
            nn = soup.find('div', id=T_main_id).table.children
        except:
            logger.info('no 在外任职')
            return dict_re
        islen = soup.find(
            'div', id=T_main_id).table.next_sibling.next_sibling('li')
        number = self.cal_number(islen)

        for tr in nn:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('th')
                for m in tds:
                    if count <= 1:
                        count += 1
                        continue
                    lenlist.append(m.text.strip().split('\n')[0])
                break
        conid = len(lenlist)

        for tr in soup.find('div', id=T_main_id).table.tbody.children:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('td')
                if len(tds) == 0:
                    continue
                rlist = tds[1].text.strip()
                dict_re[rlist] = {}
                count = 0
                for n in lenlist:
                    tlist = tds[-conid + count].text.strip().split('\n')
                    tlist = [i.strip() for i in tlist if i != '']
                    tlist = [i for i in tlist if i != '']
                    dict_re[rlist][n] = tlist[0] if len(tlist) == 1 else tlist
                    count += 1
        while True:
            if number == 0:
                break
            else:
                xpath_id = '//*[@id="{}"]/div[2]/nav/ul/li[{}]/a'.format(
                    T_main_id, number + 1)
                js_ = self.drive.find_element_by_xpath(
                    xpath_id).get_attribute('href')
                self.drive.execute_script(js_)
                time.sleep(0.5)
                soup = BeautifulSoup(self.drive.page_source, 'html.parser')
                for tr in soup.find('div', id=T_main_id).table.tbody.children:
                    if isinstance(tr, bs4.element.Tag):
                        tds = tr('td')
                        if len(tds) == 0:
                            continue
                        rlist = tds[1].text.strip()
                        dict_re[rlist] = {}
                        count = 0
                        for n in lenlist:
                            tlist = tds[-conid +
                                        count].text.strip().split('\n')
                            tlist = [i.strip() for i in tlist if i != '']
                            tlist = [i for i in tlist if i != '']
                            dict_re[rlist][n] = tlist[0] if len(
                                tlist) == 1 else tlist
                            count += 1
                islen = soup.find(
                    'div', id=T_main_id).table.next_sibling.next_sibling('li')
                number = self.cal_number(islen)
        return dict_re

    def get_t_dw(self, soup):
        """法人-对外投资
        """
        count = 0
        lenlist = []
        dict_re = {}
        T_main_id = 'investlist'

        try:
            nn = soup.find('div', id=T_main_id).table.children
        except:
            logger.info('no 对外投资')
            return dict_re
        islen = soup.find(
            'div', id=T_main_id).table.next_sibling.next_sibling('li')
        number = self.cal_number(islen)
        for tr in nn:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('th')
                for m in tds:
                    if count <= 1:
                        count += 1
                        continue
                    lenlist.append(m.text.strip().split('\n')[0])
                break
        conid = len(lenlist)

        for tr in soup.find('div', id=T_main_id).table.tbody.children:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('td')
                if len(tds) == 0:
                    continue
                rlist = tds[1].text.strip()
                dict_re[rlist] = {}
                count = 0
                for n in lenlist:
                    tlist = tds[-conid + count].text.strip().split('\n')
                    tlist = [i.strip() for i in tlist if i != '']
                    tlist = [i for i in tlist if i != '']
                    dict_re[rlist][n] = tlist[0] if len(tlist) == 1 else tlist
                    count += 1
        while True:
            if number == 0:
                break
            else:
                xpath_id = '//*[@id="{}"]/div[2]/nav/ul/li[{}]/a'.format(
                    T_main_id, number + 1)
                js_ = self.drive.find_element_by_xpath(
                    xpath_id).get_attribute('href')
                self.drive.execute_script(js_)
                time.sleep(0.5)
                soup = BeautifulSoup(self.drive.page_source, 'html.parser')
                for tr in soup.find('div', id=T_main_id).table.tbody.children:
                    if isinstance(tr, bs4.element.Tag):
                        tds = tr('td')
                        if len(tds) == 0:
                            continue
                        rlist = tds[1].text.strip()
                        dict_re[rlist] = {}
                        count = 0
                        for n in lenlist:
                            tlist = tds[-conid +
                                        count].text.strip().split('\n')
                            tlist = [i.strip() for i in tlist if i != '']
                            tlist = [i for i in tlist if i != '']
                            dict_re[rlist][n] = tlist[0] if len(
                                tlist) == 1 else tlist
                            count += 1
                islen = soup.find(
                    'div', id=T_main_id).table.next_sibling.next_sibling('li')
                number = self.cal_number(islen)
        return dict_re

    def get_t_dr(self, soup):
        """法人-担任法定代表人
        """
        count = 0
        lenlist = []
        dict_re = {}
        T_main_id = 'legallist'

        try:
            nn = soup.find('div', id=T_main_id).table.children
        except:
            logger.info('no 担任法定代表人')
            return dict_re
        islen = soup.find(
            'div', id=T_main_id).table.next_sibling.next_sibling('li')
        number = self.cal_number(islen)
        for tr in nn:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('th')
                for m in tds:
                    if count <= 1:
                        count += 1
                        continue
                    lenlist.append(m.text.strip().split('\n')[0])
                break
        conid = len(lenlist)

        for tr in soup.find('div', id=T_main_id).table.tbody.children:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('td')
                if len(tds) == 0:
                    continue
                rlist = tds[1].text.strip()
                dict_re[rlist] = {}
                count = 0
                for n in lenlist:
                    tlist = tds[-conid + count].text.strip().split('\n')
                    tlist = [i.strip() for i in tlist if i != '']
                    tlist = [i for i in tlist if i != '']
                    dict_re[rlist][n] = tlist[0] if len(tlist) == 1 else tlist
                    count += 1
        while(True):
            if number == 0:
                break
            else:
                xpath_id = '//*[@id="{}"]/div[2]/nav/ul/li[{}]/a'.format(
                    T_main_id, number + 1)
                js_ = self.drive.find_element_by_xpath(
                    xpath_id).get_attribute('href')
                self.drive.execute_script(js_)
                time.sleep(0.5)
                soup = BeautifulSoup(self.drive.page_source, 'html.parser')
                for tr in soup.find('div', id=T_main_id).table.tbody.children:
                    if isinstance(tr, bs4.element.Tag):
                        tds = tr('td')
                        if len(tds) == 0:
                            continue
                        rlist = tds[1].text.strip()
                        dict_re[rlist] = {}
                        count = 0
                        for n in lenlist:
                            tlist = tds[-conid +
                                        count].text.strip().split('\n')
                            tlist = [i.strip() for i in tlist if i != '']
                            tlist = [i for i in tlist if i != '']
                            dict_re[rlist][n] = tlist[0] if len(
                                tlist) == 1 else tlist
                            count += 1
                islen = soup.find(
                    'div', id=T_main_id).table.next_sibling.next_sibling('li')
                number = self.cal_number(islen)

        return dict_re

    def get_con(self, url):

        if url[:3] != '/pl':
            self.drive.switch_to.window(self.base_handle)
            return
        self.drive.switch_to.window(self.test_handle)
        url_base = 'https://www.qichacha.com/people_relatedlist?personid={}&p={}'

        self.drive.get(url_base.format(url[4:-5], 1))
        time.sleep(0.5)
        soup = BeautifulSoup(self.drive.page_source, 'html.parser')
        dic = {}
        lenlist = []
        count = 0

        try:
            nn = soup.find('nav', class_='text-right').children
        except:
            self.drive.switch_to.window(self.base_handle)
            return
        for tr in soup.find('nav', class_='text-right').children:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('li')
                tt = len(tds)
                number = 0
                for i in range(tt):
                    if tds[i].text == '>':
                        number = i
        if tt != 0:
            if tt != number + 1:
                tt = int(tds[tt - 1].text.split('.')[-1])
        nn = soup.find('table', class_='ntable ntable-odd').children
        for tr in nn:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('th')
                for m in tds:
                    if count <= 1:
                        count += 1
                        continue
                    lenlist.append(m.text.strip().split('\n')[0])
                break
        conid = len(lenlist)

        nn = soup.find('table', class_='ntable ntable-odd').tbody.children
        for tr in nn:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('td')
                if len(tds) == 0:
                    continue
                rlist = tds[1].text.strip()
                dic[rlist] = {}
                count = 0

                for n in lenlist:
                    tlist = tds[-conid + count].text
                    dic[rlist][n] = tlist.strip()
                    count += 1

        if tt != 0:
            for i in range(tt - 2):
                self.drive.get(url_base.format(url[4:-5], i + 2))
                time.sleep(0.5)
                soup = BeautifulSoup(self.drive.page_source, 'html.parser')
                count = 0
                nn = soup.find(
                    'table', class_='ntable ntable-odd').tbody.children
                for tr in nn:
                    if isinstance(tr, bs4.element.Tag):
                        tds = tr('td')
                        if len(tds) == 0:
                            continue
                        rlist = tds[1].text.strip()
                        dic[rlist] = {}
                        count = 0
                        for n in lenlist:
                            tlist = tds[-conid + count].text
                            dic[rlist][n] = tlist.strip()
                            count += 1
        self.drive.switch_to.window(self.base_handle)
        return dic

    def get_con_gd(self, url):
        dic = {}
        if url[:3] != '/pl':
            return dic
        self.drive.switch_to.window(self.other_handle)
        url_base = 'https://www.qichacha.com/people_relatedlist?personid={}&p={}'

        self.drive.get(url_base.format(url[4:-5], 1))
        time.sleep(0.5)
        soup = BeautifulSoup(self.drive.page_source, 'html.parser')

        lenlist = []
        count = 0
        for i in range(3):
            try:
                nn = soup.find('nav', class_='text-right').children
                break
            except:
                if i == 2:
                    return dic
                else:
                    continue
        for tr in nn:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('li')
                tt = len(tds)
                number = 0
                for i in range(tt):
                    if tds[i].text == '>':
                        number = i
        if tt != 0:
            if tt != number + 1:
                tt = int(tds[tt - 1].text.split('.')[-1])
        nn = soup.find('table', class_='ntable ntable-odd').children
        for tr in nn:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('th')
                for m in tds:
                    if count <= 1:
                        count += 1
                        continue
                    lenlist.append(m.text.strip().split('\n')[0])
                break
        conid = len(lenlist)

        nn = soup.find('table', class_='ntable ntable-odd').tbody.children
        for tr in nn:
            if isinstance(tr, bs4.element.Tag):
                tds = tr('td')
                if len(tds) == 0:
                    continue
                rlist = tds[1].text.strip()
                dic[rlist] = {}
                count = 0

                for n in lenlist:
                    tlist = tds[-conid + count].text
                    dic[rlist][n] = tlist.strip()
                    count += 1

        if tt != 0:
            for i in range(tt - 2):
                self.drive.get(url_base.format(url[4:-5], i + 2))
                time.sleep(0.5)
                soup = BeautifulSoup(self.drive.page_source, 'html.parser')
                count = 0
                nn = soup.find(
                    'table', class_='ntable ntable-odd').tbody.children
                for tr in nn:
                    if isinstance(tr, bs4.element.Tag):
                        tds = tr('td')
                        if len(tds) == 0:
                            continue
                        rlist = tds[1].text.strip()
                        dic[rlist] = {}
                        count = 0
                        for n in lenlist:
                            tlist = tds[-conid + count].text
                            dic[rlist][n] = tlist.strip()
                            count += 1
        self.drive.switch_to.window(self.test_handle)
        return dic

    def initial(self):
        option = webdriver.ChromeOptions()
        option.add_argument(
            r"user-data-dir=C:\Users\shkk1\AppData\Local\Google\Chrome\User Data")

        driver = webdriver.Chrome(
            executable_path=self.chromepath, options=option)

        self.drive = driver
        self.base_handle = driver.current_window_handle
        js = 'window.open("https://www.sogou.com");'
        driver.execute_script(js)
        handles = driver.window_handles
        self.test_handle = None
        for handle in handles:
            if handle != self.base_handle:
                self.test_handle = handle

        driver.switch_to.window(self.base_handle)

    def run(self):
        """通过已抓取链接爬取供应商所需要信息
        """
        driver = self.drive
        comdic = self.input('Com_ip_all.json')
        for t in comdic.keys():
            time1 = time.strftime("%Y-%m-%d %H:%M", time.localtime())
            if time1[11:13] == '07':
                return
            dict_re = {}
            filefolder = os.path.join(self.processplace, '{}'.format(t))
            if os.path.isdir(filefolder):
                """保存文件夹是否已存在
                """
                logger.info('page {} success'.format(t))
                continue
            os.makedirs(filefolder)
            self.filp = filefolder
            logger.info('{} start'.format(t))
            url = self.Comurl.format(comdic[t])
#             url = 'https://www.qichacha.com/firm_73cb63c2281515debac1dd18b3ba8d8a.html'
            driver.get(url)
            time.sleep(3)
            soup = BeautifulSoup(driver.page_source, "html.parser")

            dict_re['TYFX'] = self.add_ty(soup)
            dict_re['DWTZ'] = self.add_dw(soup)
            dict_re['GSXX'] = self.add_gs(soup, True)
            dict_re['GDXX'] = self.add_gd(soup)
            dict_re['ZZSY'] = self.add_sy(soup)
            dict_re['ZYRY'] = self.add_mp(soup)
            dict_re['BGJL'] = self.add_bg(soup)
            dict_re['KGQY'] = self.add_kg(soup)

            filename1 = os.path.join(self.filp, 'FRXX.json')
            filename2 = os.path.join(self.filp, 'GDXX.json')
            FZXX = self.add_fz(soup)
            filename3 = os.path.join(self.filp, 'FZXX.json')
            filesave4 = os.path.join(self.filp, 'MAIN.json')

            self.output(filename1, self.FRXX)
            self.output(filename2, self.GDXX)
            self.output(filename3, FZXX)
            self.output(filesave4, dict_re)
            time.sleep(1)


if __name__ == '__main__':

    t = SpiderSupplier()
    t.getcom_csv()
    t.getcom_ip()
    t.run()
