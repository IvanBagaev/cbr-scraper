import os

import pandas as pd
import numpy as np
import dateparser as dtparser

from urllib.request import urlopen
from bs4 import BeautifulSoup
from io import StringIO

from ..utils import to_number
from .structures import FORM135


class Symbol:
    """
    Represents particular symbol.
    """
    def __init__(self, number, name, balance=0):
        self.number = number
        self.name = name

    def __repr__(self):
        return "({0}) - {1}".format(self.number, self.name)


class FormUnit:
    """
    Represents unit of reporting form's structure.
    Allow access to related symbols.

    """
    def __init__(self, form):
        self.form = form
        self.symbols = []

    @property
    def symbols_numbers(self):
        """Returns list of symbol's numbers"""
        return [acc.number for acc in self.symbols]

    @property
    def symbols_names(self):
        """Returns list of symbol's names"""
        return [acc.name for acc in self.symbols]

    def sum(self):
        """Return sum of all symbols in this unit"""
        return np.sum([s.balance for s in self.symbols])

    def to_dataframe(self):
        df = pd.DataFrame([acc.balance for acc in self.symbols]).T
        df.columns = [acc.number for acc in self.symbols]
        df['date'] = self.form.date
        df['bank'] = self.form.bank.bank_id
        return df

class Form135(FormUnit):
    """
    Represents whole structure of a reporting form.

    Can store data for multiple dates

    """
    date = None
    is_filled = False

    def __init__(self, bank):

        self.struct = pd.read_csv(StringIO(FORM135))
        self.bank = bank
        self.symbols = [Symbol(acc.number, acc.name) for acc in self.struct.itertuples(index=False)]
        for n in self.struct.name.unique():
            name = FormUnit(self)
            name.symbols = [s for s in self.symbols if s.name == n]
            setattr(self, n, name)
        self.form = self


    def fill(self, first_n=None):
        """
        Fill an empty initialized form with values. Load first n forms from
        cbr.ru site (2016->2015->...)

        """
        soup = self.bank._open_bank_page()

        reports =  soup.find('div' , {'class':'reports'})
        f_135 = reports.find('div', {'id':'f_135'})

        if not f_135:
            return pd.DataFrame()

        # Get list of reporting dates
        dates = []
        for el in f_135.find('div',{'class':'switched'}).findAll('div',{'class':'normal'}):
            year = el['id'][-4:]
            dates.extend([dtparser.parse(a.text[3:] + ' ' + year) for a in el.findAll('a')])

        urls = [('http://www.cbr.ru/credit/'+a['href']).replace('®','&reg') for a in f_135.findAll('a')]

        if not first_n or first_n > len(urls):
            first_n = len(urls)

        f_135 = pd.DataFrame()

        for url, date in zip(urls[:first_n], dates[:first_n]):

            page = urlopen(url).read()
            tables = pd.read_html(page)

            transcripts = tables[1].drop(0)
            norms = tables[2][[0,1]].drop(0).fillna(0)
            norms[1] = norms[1].map(lambda x: int(str(x))/100)
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

        f_135['index'] = self.bank.bank_id

        self.date = f_135.date.values
        for acc in self.symbols:
            try:
                acc.balance = f_135[str(acc.number)].values
            except KeyError:
                acc.balance = np.zeros(first_n)
        self.is_filled = True
        return self
