import os

import pandas as pd
import numpy as np
import dateparser as dtparser

from urllib.request import urlopen
from bs4 import BeautifulSoup
from io import StringIO

from ..utils import to_number
from .structures import FORM123



class Section(object):
    """
    Represents particular symbol.
    """
    def __init__(self, number, name, chaper,part,section,subsection, balance=0):
        self.number = number
        self.name = name
        self.balance = [balance]

    def __repr__(self):
        return "({0}) - {1}".format(self.number, self.name)


class FormUnit(object):
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


    def to_dataframe(self):
        df = pd.DataFrame([acc.balance for acc in self.symbols]).T
        df.columns = [acc.number for acc in self.symbols]
        df['date'] = self.form.date
        df['bank'] = self.form.bank.bank_id
        return df

class Form123(FormUnit):
    """
    Represents whole structure of a reporting form.

    Can store data for multiple dates

    """
    date = None
    is_filled = False

    def __init__(self, bank):

        self.struct = pd.read_csv(StringIO(FORM123))
        self.bank = bank
        self.section = [Section(acc.number, acc.name)
                        for acc in self.struct.itertuples(index=False)]
        for ch in self.struct.chapter.unique():
            chapter = FormUnit(self)
            setattr(self, ch, chapter)



    def fill(self, first_n=None):
        """
        Fill an empty initialized form with values. Load first n forms from
        cbr.ru site (2016->2015->...)

        """
        soup = self.bank._open_bank_page()

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


        self.date = f_123.date.values
        for acc in self.sections:
            acc.balance = f_123[str(acc.number)].values
        self.is_filled = True
        return self
