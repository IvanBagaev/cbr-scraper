import pandas as pd

from bs4 import BeautifulSoup
from urllib.request import urlopen

from .bank import Bank


class ClosedBanks:
    """
    Parse closed banks data from banki.ru
    """

    def _get_closing_info(self, url):
        """Gather info about closed banks from a single page"""
        page = urlopen(url).read()
        closed_banks = pd.read_html(page)[2]
        closed_banks['link'] = pd.Series([a['href'] for a in BeautifulSoup(page, 'html.parser').\
                                       findAll('table')[2].find('tbody').findAll('a')])
        return closed_banks


    def _get_description(self, url):
        """Returns description for a single bank from it's memory page"""
        page = urlopen(url).read()
        bank_description = BeautifulSoup(page,'html.parser').find('dl')
        description = pd.DataFrame(data = [' '.join(dt.text.split()) for dt in bank_description.findAll('dd')],
                                  index=[' '.join(dt.text.split()) for dt in bank_description.findAll('dt')]).T
        description['index'] = bank_description.find('a')['href'][bank_description.find('a')['href'].find('=')+1:]

        description['Номер лицензии'] = description['Номер лицензии'].map(lambda x: x.split()[0])

        description.columns = ['full_name', 'city',
                              'license_number', 'reason_of_closing', 'date_of_closing',
                              'reason_extended', 'index']
        return description

    def __init__(self):

        # Load list of closed banks
        first_page = 'http://www.banki.ru/banks/memory/'
        all_pages = [(first_page + '?PAGEN_1=%d') % x for x in range(2,51)]

        self.closed_banks = self._get_closing_info(first_page)

        counter = 1
        for page_url in all_pages[:2]:
            print('\rProcessed %d of %d...' % (counter, len(all_pages)+1), end='')
            closed_banks = pd.concat([self.closed_banks, self._get_closing_info(page_url)])
            counter+=1

        self.closed_banks.columns = ['index', 'bank', 'license_number', 'reason_of_closing', 'date_of_closing', 'city','link']

        # Load descriptions of closed banks

        self.bank_descriptions = pd.DataFrame()
        for bank in self.closed_banks.itertuples(index=False):
            url = 'http://www.banki.ru/'+ bank.link
            self.bank_descriptions = pd.concat([self.bank_descriptions,self._get_description(url)])

    def as_bank(self):
        """Return list of Bank class instances"""
        return [Bank(bank['index'], bank['license_number'], bank['full_name'])
                    for bank in self.bank_descriptions.itertuples(index=False)]
