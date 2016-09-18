import os

import pandas as pd
import numpy as np
import dateparser as dtparser

from urllib.request import urlopen
from bs4 import BeautifulSoup
from io import StringIO

from ..utils import to_number
from .structures import FORM123, FORM134



class Section:
    """
    Represents particular symbol.
    """
    def __init__(self, number, name,  balance=0):
        self.number = number
        self.name = name
        self.balance = [balance]

    def __repr__(self):
        return "({0}) - {1}".format(self.number, self.name)


class FormUnit:
    """
    Represents unit of reporting form's structure.
    Allow access to related symbols.

    """
    def __init__(self, form):
        self.form = form
        self.sections = []

    @property
    def symbols_numbers(self):
        """Returns list of symbol's numbers"""
        return [acc.number for acc in self.sections]

    @property
    def symbols_names(self):
        """Returns list of symbol's names"""
        return [acc.name for acc in self.sections]


    def to_dataframe(self):
        """Convert form to pandas DataFrame format"""
        df = pd.DataFrame([acc.balance for acc in self.sections]).T
        df.columns = [acc.number for acc in self.sections]
        df['date'] = self.form.date
        df['bank'] = self.form.bank.bank_id
        return df

class Form123(FormUnit):
    """
    Represents whole structure of a reporting form.

    Can store data for multiple dates


    Also can be used to get form 134, passing form_type = 'f_134' as argument


    """
    date = None
    is_filled = False

    def __init__(self, bank, form_type='f_123'):
        self.form_type = form_type
        if self.form_type == 'f_123':
            self.struct = pd.read_csv(StringIO(FORM123))
        elif self.form_type == 'f_134':
            self.struct = pd.read_csv(StringIO(FORM134))
        self.bank = bank
        self.sections = [Section(acc.number, acc.name)
                        for acc in self.struct.itertuples(index=False)]

        self.form = self


    def fill(self, first_n=None):
        """
        Fill an empty initialized form with values. Load first n forms from
        cbr.ru site (2016->2015->...)

        """
        soup, f_123 = self.bank._find_form(self.form_type)

        if f_123 is None:
            return pd.DataFrame()

        # Get list of reporting dates
        dates = []
        for el in f_123.find('div',{'class':'switched'}).findAll('div',{'class':'normal'}):
            year = el['id'][-4:]
            dates.extend([dtparser.parse(a.text[3:] + ' ' + year) for a in el.findAll('a')])

        urls = [('http://www.cbr.ru/credit/'+a['href']).replace('®','&reg') for a in f_123.findAll('a')]

        if not first_n or first_n > len(urls):
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
        f_123['index'] = self.bank.bank_id

        #return f_123
        self.date = f_123.date.values
        for acc in self.sections:
            try:
                acc.balance = f_123[str(acc.number)].values
            except KeyError:
                acc.balance = np.zeros(first_n)
        self.is_filled = True
        return self
