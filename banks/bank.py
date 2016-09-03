# bank.py
from bs4 import BeautifulSoup
from urllib.request import urlopen

import dateparser as dtparser

from .utils import to_number

BANK_PAGE_URL_PATTERN = "http://www.cbr.ru/credit/coinfo.asp?id=%s"

class Bank(object):
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

    def get_form101(self, first_n = None):
        """
        ...
        Parameters
        ----------
        first_n: Number of reporting dates to parse


        """
        soup, f_101 = self._find_form('f_101')

        if f_101 is None:
            return pd.DataFrame()

        # Get list of reporting dates
        dates = []
        for el in f_101.find('div',{'class':'switched'}).findAll('div',{'class':'normal'}):
            year = el['id'][-4:]
            dates.extend([dtparser.parse(a.text[3:] + ' ' + year) for a in el.findAll('a')])

        urls = [('http://www.cbr.ru/credit/'+a['href']) for a in f_101.findAll('a')]

        if first_n is None:
            first_n = len(urls)

        f_101 = pd.DataFrame()

        for url, date in zip(urls[:first_n], dates):
            page = urlopen(url).read()
            soup = BeautifulSoup(page,'html.parser')

            table = pd.read_html(page)[1]
            # For two case of form view
            if 'Код' in soup.find('h2').text:

                table = table.dropna()
                table = table.drop([3], axis=0)
                table.index = table[0]
                table = table[[12]]
                table.columns = ['balance']
                table['date'] = date
                table.rename(index={12:'number'}, inplace=True)

            elif 'Форма' in soup.find('h2').text:

                table = table.drop([0,1,2], axis=0).fillna(0)
                table[2] = table[2].map(to_number) + table[3].map(to_number)
                table.index = table[1]
                table = table[[2]]
                table.columns = ['balance']
                table['date'] = date
                table.rename(index={0:'number'}, inplace=True)

            f_101 = pd.concat([f_101, table])

        f_101 = f_101.reset_index()
        f_101.columns = ['number', 'balance','date']
        f_101.balance = f_101.balance.map(to_number)
        f_101 = f_101.groupby(['date','number'])['balance'].agg('sum').unstack().reset_index().fillna(0)
        f_101['index'] = self.bank_id

        return f_101

    def get_form102(self, first_n=None):
        """

        """

        soup = self._open_bank_page()

        reports =  soup.find('div' , {'class':'reports'})
        f_102 = reports.find('div', {'id':'f_102'})

        if f_102 is None:
            return pd.DataFrame()

        # Get list of reporting dates
        dates = []
        for el in f_102.find('div',{'class':'switched'}).findAll('div',{'class':'normal'}):
            year = el['id'][-4:]
            dates.extend([dtparser.parse(a.text[3:] + ' ' + year) for a in el.findAll('a')])


        f_102 = f_102.findAll('a')

        urls = [('http://www.cbr.ru/credit/'+a['href']) for a in f_102]

        if first_n is None:
            first_n = len(urls)
        f_102 = pd.DataFrame()

        for url, date in zip(urls[:first_n], dates[:first_n]):
            page = urlopen(url).read()
            soup = BeautifulSoup(page,'html.parser')

            table = pd.read_html(page)[1]

            if len(table.columns) > 3:
                table = table[[2,5]].dropna()
                table = table.drop(2)
                table.columns = ['symbol', 'balance']
            elif len(table.columns) == 3:
                table = table[[1,2]].dropna()
                table = table.drop(0)
                table.columns = ['symbol', 'balance']

            table['date'] = date
            f_102 = pd.concat([f_102, table])

        f_102.columns = ['symbol', 'balance','date']

        f_102.balance = f_102.balance.map(to_number)

        f_102 = f_102.groupby(['date','symbol'])['balance'].agg('sum').unstack().reset_index().fillna(0)
        f_102['index'] = self.bank_id

        return f_102

    def get_form123(self, first_n = None, form_type='f_123'):
        """
        Also can be used to get form 134, passing form_type = 'f_134' as argument

        """
        soup = self._open_bank_page()

        reports =  soup.find('div' , {'class':'reports'})
        f_123 = reports.find('div', {'id':form_type})

        if f_123 is None:
            return pd.DataFrame()

        # Get list of reporting dates
        dates = []
        for el in f_123.find('div',{'class':'switched'}).findAll('div',{'class':'normal'}):
            year = el['id'][-4:]
            dates.extend([dtparser.parse(a.text[3:] + ' ' + year) for a in el.findAll('a')])


        f_123 = f_123.findAll('a')

        urls = [('http://www.cbr.ru/credit/'+a['href']).replace('®','&reg') for a in f_123]

        if first_n is None:
            first_n = len(urls)
        f_123 = pd.DataFrame()

        for url, date in zip(urls[:first_n], dates[:first_n]):
            page = urlopen(url).read()

            table = pd.read_html(page)[1]
            table = table.drop(0)
            table = table.dropna()

            #table[0] = table[0]+' '+table[1]
            table = table[[0,2]]
            table['date'] = date

            f_123 = pd.concat([f_123, table])

        f_123.columns = ['symbol', 'balance','date']

        f_123.balance = f_123.balance.map(to_number)

        f_123 = f_123.groupby(['date','symbol'])['balance'].agg('sum').unstack().reset_index().fillna(0)
        f_123['index'] = self.bank_id

        return f_123

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
