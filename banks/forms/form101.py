import os

import pandas as pd
import numpy as np

from io import StringIO
from .structures import FORM101


class Account(object):
    """
    Represents particular sub-ledger account.
    """
    def __init__(self, number, name, account_type, section, part, balance=0):
        self.number = number
        self.name = name
        self.account_type = account_type
        self.section = section
        self.part = part
        self.balance = balance

    def __repr__(self):
        return "{0} ({1}) - {2}".format(self.number, self.account_type, self.name)


class FormUnit(object):
    """
    Represents unit of reporting form's structure.
    Allow access to related accounts.

    """
    def __init__(self):
        self.accounts = []

    def assets_sum(self):
        """Returns the amount of assets in this unit"""
        return np.sum([acc.balance for acc in self.assets])

    def liabilities_sum(self):
        """Returns the amount of liabilities in this unit"""
        return np.sum([acc.balance for acc in self.liabilities])

    @property
    def assets(self):
        """Returns list of assets accounts in this section or part"""
        return [acc for acc in self.accounts if acc.account_type == 'А']

    @property
    def liabilities(self):
        """Returns list of liabilities accounts in this section or part"""
        return [acc for acc in self.accounts if acc.account_type == 'П']

    @property
    def accounts_numbers(self):
        """Returns list of account's numbers"""
        return [acc.number for acc in self.accounts]

    @property
    def accounts_names(self):
        """Returns list of account's names"""
        return [acc.name for acc in self.accounts]

class Form101(object):
    """
    Represents whole structure of a reporting form.

    """

    def __init__(self, date, bank = None):

        self.struct = pd.read_csv(StringIO(FORM101))

        self.all_accounts = [Account(acc.number, acc.name, acc.account_type, acc.section, acc.part)
                             for acc in self.struct.itertuples(index=False)]

        for s in self.struct.section.unique():
            section = FormUnit()
            for p in self.struct.part.unique():
                part = FormUnit()
                part.accounts = [acc for acc in self.all_accounts if acc.part == p]
                setattr(section, p, part)
                section.accounts.extend(part.accounts)
            setattr(self, s, section)

        self.date = date
        self.bank = bank

    def to_dataframe(self):
        df = pd.DataFrame([acc.balance for acc in self.all_accounts],
                           columns = [acc.number for acc in self.all_accounts])
        df['date'] = self.date
        return df
