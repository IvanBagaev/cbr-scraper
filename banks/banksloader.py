import pandas as pd

from bs4 import BeautifulSoup
from urllib.request import urlopen

from .bank import Bank


class BanksLoader:
    """
    Parse closed banks data from banki.ru
    """

    def __init__(self):
        self.closed_descriptions = None
        self.closed_banks = None
        self.active_banks = None

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

    def load_closed(self):

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

        self.closed_descriptions = pd.DataFrame()
        for bank in self.closed_banks.itertuples(index=False):
            url = 'http://www.banki.ru/'+ bank.link
            self.closed_descriptions = pd.concat([self.closed_descriptions,self._get_description(url)])

        return self

    def load_active(self):
        cbr_bank_list_url = 'http://www.cbr.ru/credit/transparent.asp'

        page = urlopen(cbr_bank_list_url).read()

        cbr_bank_list_df = pd.read_html(page)[0]
        cbr_bank_list_df.drop([4,5,6],axis=1, inplace=True)
        cbr_bank_list_df.columns = cbr_bank_list_df.ix[0]
        cbr_bank_list_df.drop([0,1],axis=0, inplace=True)
        cbr_bank_list_df.drop(['Раскрытие информации'],axis=1, inplace=True)

        text = str(BeautifulSoup(page,'html.parser').find('table'))
        ids = re.findall('javascript:info\((.+?)\)', text)

        cbr_bank_list_df['№'] = ids
        cbr_bank_list_df.columns = ['id', 'license_number', 'name']

        self.active banks = cbr_bank_list_df

        return self

    def load_form(self, form_number, bank_list):

        pass

    @property
    def closed_list(self):
        """Returns list of Closed Bank class instances"""

        if self.closed_descriptions is None:
            raise ValueError('Closed banks should be loaded first!')

        return [Bank(bank.index, bank.license_number, bank.full_name)
                    for bank in self.closed_descriptions.itertuples(index=False)]
    @property
    def active_list(self):
        """Returns list of Active Bank class instances"""

        if self.active_banks is None:
            raise ValueError('Active banks should be loaded first!')

        return [Bank(bank.id,bank.license_number,bank.name)
                    for bank in self.active_banks.itertuples(index=False)]
