import os

import pandas as pd
import numpy as np
import dateparser as dtparser

from urllib.request import urlopen
from bs4 import BeautifulSoup
from io import StringIO

from ..utils import to_number
from .structures import FORM102


class Symbol(object):
    """
    Represents particular sub-ledger symbol.
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
        return "{0} ({1}) - {2}".format(self.number, self.name)


class FormUnit(object):
    """
    Represents unit of reporting form's structure.
    Allow access to related symbols.

    """
    def __init__(self, bank=None,date=None):
        self.symbols = []

    def income_sum(self):
        """Returns the amount of assets in this unit"""
        return np.sum([acc.balance for acc in self.assets])

    def expenses_sum(self):
        """Returns the amount of liabilities in this unit"""
        return np.sum([acc.balance for acc in self.liabilities])

    @property
    def incomes(self):
        """Returns list of assets symbols in this section or part"""
        return [acc for acc in self.symbols if acc.chaper == 'Доходы']

    @property
    def expenses(self):
        """Returns list of liabilities symbols in this section or part"""
        return [acc for acc in self.symbols if acc.chaper == 'Расходы']

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
        df['date'] = self.date
        df['bank'] = self.bank.bank_id
        return df

class Form102(FormUnit):
    """
    Represents whole structure of a reporting form.

    """
    date = None

    def __init__(self, bank):

        self.struct = pd.read_csv(StringIO(FORM102)).fillna('Далее')
        self.bank = bank
        self.symbols = [Symbol(acc.number, acc.name, acc.chapter,
                               acc.part, acc.section, acc.subsection)
                             for acc in self.struct.itertuples(index=False)]
        for ch in self.struct.chapter.unique():
            chapter = FormUnit(self.bank, self.date)
            for p in self.struct.part.unique():
                part = FormUnit(self.bank, self.date)
                for s in self.struct.section.unique():
                    section = FormUnit(self.bank, self.date)
                    for ss in self.struct.subsection.unique():
                        subsection = FormUnit(self.bank, self.date)
                        subsection.symbols = [acc for acc in self.symbols if acc.subsection == ss]
                        setattr(section, ss, subsection)
                        section.symbols.extend(subsection.symbols)
                    setattr(part, s, section)
                    part.symbols.extend(section.symbols)
                setattr(chapter, p, part)
                chapter.symbols.extend(part.symbols)
            setattr(self, ch, chapter)



    def fill(self, first_n,):
        """
        Fill an empty initialized form with values. Load first n forms from
        cbr.ru site (2016->2015->...)

        """
        soup, f_101 = self.bank._find_form('f_101')

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
        f_101['index'] = self.bank.bank_id

        self.date = f_101.date.values
        for acc in self.symbols:
            acc.balance = f_101[str(acc.number)].values

        return self
