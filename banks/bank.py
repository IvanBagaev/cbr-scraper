# bank.py
import pandas as pd

from bs4 import BeautifulSoup
from urllib.request import urlopen

import dateparser as dtparser

from .utils import to_number

BANK_PAGE_URL_PATTERN = "http://www.cbr.ru/credit/coinfo.asp?id=%s"

class Bank:
    """


    Parameters
    ----------
    bank_id :
        ID on cbr.ru. Must be correct.
    license_number :
        Licence number. Must be correct
    name : str
        Name of bank. can be in free format.
    """

    def __init__(self, bank_id, license_number,name="Unknown"):
        self.name = name
        self.bank_id = bank_id
        self.license_number = license_number

    def __repr__(self):
        return "(%s) Лиц. № %s - %s" % (self.bank_id, self.license_number, self.name)

    def _open_bank_page(self):
        page = urlopen(BANK_PAGE_URL_PATTERN % self.bank_id)
        soup = BeautifulSoup(page.read(),'html.parser')
        return soup

    def get_main_info(self):
        """
        Parse main info about credit organization by given index.

        Parameters
        ----------
        bank : Bank class
            Bank class


        Returns
        -------
        main_info : DataFrame

        """

        soup = _open_bank_page()

        main_info = pd.read_html(str(soup.find('table')))[0]
        main_info.index = main_info[0]
        main_info = main_info.drop(0, axis=1).T
        main_info['id'] = [self.bank_id]
        main_info['Работающий на сегодня'] = ['да'] # TODO: write a check for this !

        return main_info


    def _find_form(self, form_name):
        """name in f_xxx format. For example f_101, f_102 etc."""
        soup = self._open_bank_page()
        reports =  soup.find('div' , {'class':'reports'})
        form = reports.find('div', {'id':form_name})
        return soup, form


    def get_form135(self, first_n=None):
        soup = self._open_bank_page()

        reports =  soup.find('div' , {'class':'reports'})
        f_135 = reports.find('div', {'id':'f_135'})

        if f_135 is None:
            return pd.DataFrame()

        # Get list of reporting dates
        dates = []
        for el in f_135.find('div',{'class':'switched'}).findAll('div',{'class':'normal'}):
            year = el['id'][-4:]
            dates.extend([dtparser.parse(a.text[3:] + ' ' + year) for a in el.findAll('a')])


        f_135 = f_135.findAll('a')

        urls = [('http://www.cbr.ru/credit/'+a['href']).replace('®','&reg') for a in f_135]

        if first_n is None:
            first_n = len(urls)

        f_135 = pd.DataFrame()

        for url, date in zip(urls[:first_n], dates[:first_n]):

            page = urlopen(url).read()
            tables = pd.read_html(page)

            transcripts = tables[1].drop(0)
            norms = tables[2][[0,1]].drop(0).fillna(0)
            norms[1] = norms[1].map(lambda x: int(str(x))/100)
            print(norms)
            ul = BeautifulSoup(page, 'html.parser').find('ul', {'class':'without_dash without_indent'})
            indicators = []
            for li in ul.findAll('li'):
                li = ''.join(str(li.text).split())
                indicators.append([li[:li.find('=')],li[li.find('=')+1:]])

            indicators = pd.DataFrame(indicators)
            table = pd.concat([transcripts, norms, indicators]).set_index(0).fillna(0)
            table[1] = table[1].astype(np.str).map(to_number)
            table = table.T
            table['date'] = date

            f_135 = pd.concat([f_135, table]).fillna(0)

        f_135['index'] = self.bank_id

        return f_135
