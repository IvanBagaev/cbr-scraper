import os

import pandas as pd
import numpy as np
import dateparser as dtparser

from urllib.request import urlopen
from bs4 import BeautifulSoup
from io import StringIO

from ..utils import to_number
from .structures import FORM102


class Symbol:
    """
    Represents particular symbol.
    """
    def __init__(self, number, name, chaper,part,section,subsection, balance=0):
        self.number = number
        self.name = name
        self.chaper = chaper
        self.subsection = subsection
        self.section = section
        self.part = part
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

class Form102(FormUnit):
    """
    Represents whole structure of a reporting form.

    Can store data for multiple dates

    """
    date = None
    is_filled = False

    def __init__(self, bank):

        self.struct = pd.read_csv(StringIO(FORM102)).fillna('Далее')
        self.bank = bank
        self.symbols = [Symbol(acc.number, acc.name, acc.chapter,
                               acc.part, acc.section, acc.subsection)
                             for acc in self.struct.itertuples(index=False)]
        for ch in self.struct.chapter.unique():
            chapter = FormUnit(self)
            for p in self.struct.query('chapter == @ch').part.unique():
                part = FormUnit(self)
                for s in self.struct.query('part == @p').section.unique():
                    section = FormUnit(self)
                    for ss in self.struct.query('section == @s').subsection.unique():
                        subsection = FormUnit(self)
                        subsection.symbols = [acc for acc in self.symbols if acc.subsection == ss]
                        setattr(section, ss, subsection)
                        section.symbols.extend(subsection.symbols)
                    setattr(part, s, section)
                    part.symbols.extend(section.symbols)
                setattr(chapter, p, part)
                chapter.symbols.extend(part.symbols)
            setattr(self, ch, chapter)

        self.form = self


    def fill(self, first_n=None):
        """
        Fill an empty initialized form with values. Load first n forms from
        cbr.ru site (2016->2015->...)

        """
        soup = self.bank._open_bank_page()

        reports =  soup.find('div' , {'class':'reports'})
        f_102 = reports.find('div', {'id':'f_102'})

        if not f_102:
            return pd.DataFrame()

        # Get list of reporting dates
        dates = []
        for el in f_102.find('div',{'class':'switched'}).findAll('div',{'class':'normal'}):
            year = el['id'][-4:]
            dates.extend([dtparser.parse(a.text[3:] + ' ' + year)
                          for a in el.findAll('a')])


        f_102 = f_102.findAll('a')

        urls = [('http://www.cbr.ru/credit/'+a['href']) for a in f_102]

        if not first_n or first_n > len(urls):
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
        f_102['index'] = self.bank.bank_id


        self.date = f_102.date.values
        for acc in self.symbols:
            try:
                acc.balance = f_102[str(acc.number)].values
            except KeyError:
                acc.balance = np.zeros(first_n)
        self.is_filled = True
        return self
