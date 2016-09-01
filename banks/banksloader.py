import re

import pandas as pd

from bs4 import BeautifulSoup
from urllib.request import urlopen

from .bank import Bank


class BanksLoader:
    """
    Parse closed banks data from banki.ru
    """

    def __init__(self):
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
        description = pd.DataFrame(
            data = [' '.join(dt.text.split()) for dt in bank_description.findAll('dd')],
            index=[' '.join(dt.text.split()) for dt in bank_description.findAll('dt')]).T

        if len(description.columns) <= 5:
            description['reason of closing'] = ""

        try:
            description['index'] = bank_description.find('a')['href'][bank_description.find('a')['href'].find('=')+1:]
        except TypeError:
            description['index'] = -1

        description['Номер лицензии'] = description['Номер лицензии'].map(lambda x: x.split()[0])
        description.columns = ['full_name', 'city',
                              'license_number', 'reason_of_closing', 'date_of_closing',
                              'reason_extended', 'index']
        return description

    def load_closed(self):

        # Load list of closed banks
        first_page = 'http://www.banki.ru/banks/memory/'
        all_pages = [(first_page + '?PAGEN_1=%d') % x for x in range(2,51)]

        closed_banks = self._get_closing_info(first_page)

        counter = 1
        print('\nLoading closed banks...')
        for page_url in all_pages:
            print('\rProcessed %d page of %d...' % (counter, len(all_pages)+1), end='')
            closed_banks = pd.concat([closed_banks, self._get_closing_info(page_url)])
            counter+=1

        closed_banks.columns = ['index', 'bank', 'license_number', 'reason_of_closing', 'date_of_closing', 'city','link']


        print('\nLoading descriptions for closed banks...')
        closed_descriptions = pd.DataFrame(columns = ['full_name', 'city','license_number',
                                                       'reason_of_closing', 'date_of_closing',
                                                       'reason_extended', 'index'])

        counter = 1
        for bank in closed_banks.itertuples(index=False):
            print('\rProcessed %d  of %d...' % (counter, len(closed_banks)+1), end='')
            url = 'http://www.banki.ru/'+ bank.link

            closed_descriptions = closed_descriptions.append(
                self._get_description(url),
                ignore_index=True)
            counter+=1

        self.closed_banks = closed_banks.merge(
            closed_descriptions['full_name','reason_of_closing','reason_extended','index'],
            on='license_number')

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

        self.active_banks = cbr_bank_list_df

        return self

    #def to

    def load_closed_from_csv(self, closed_path, descriptions_path):
        """Allow to load closed banks DataFrame from csv"""
        if active_path is None:
            raise AttributeError("You should specify path to csv first.")
        self.closed_descriptions = pd.read_csv(descriptions_path)
        self.closed_banks = pd.read_csv(closed_path)
        return self

    def load_active_from_csv(self, active_path):
        """
        Allow to load active banks DataFrame from csv.
        """
        if active_path is None:
            raise AttributeError("You should specify path to csv first.")
        self.active_banks = pd.read_csv(active_path)


    def load_forms(self, first_n):
        """
        Load first N forms for each bank.

        """
        if self.closed_descriptions is None or self.active_banks is None:
            raise ValueError('Banks should be loaded first!')

        banks_main_info = pd.DataFrame()
        form_101 = pd.DataFrame()
        form_102 = pd.DataFrame()
        form_123 = pd.DataFrame()
        form_134 = pd.DataFrame()
        form_135 = pd.DataFrame()

        forms = [form_101, form_102, form_123, form_134, form_135]

        counter = 1
        total_spent = 0

        for bank in self.active_banks_list.extend(self.closed_banks_list):
            start_time = time.time()

            print("{0} of {1} \t{2} - {3}.".format(counter,
                                                  len(cbr_bank_list),
                                                  bank.bank_id, bank.name), end=' ')
            # TODO: DRY
            print('Form 101...', end = ' ')
            f_101 = bank.get_form101(first_n)
            form_101 = pd.concat([form_101,f_101])

            print('Form 102...', end = ' ')
            f_102 = bank.get_form102(first_n)
            form_102 = pd.concat([form_102,f_102])

            print('Form 123...', end = ' ')
            f_123 = bank.get_form123(first_n)
            form_123 = pd.concat([form_123,f_123])

            print('Form 134...', end = ' ')
            f_134 = bank.get_form123(first_n,'f_134')
            form_134 = pd.concat([form_134,f_134])

            print('Form 135...', end = ' ')
            f_135 = bank.get_form135(first_n)
            form_135 = pd.concat([form_135,f_135])

            total_spent += time.time()-start_time
            print('. Time spent - %d sec.' % (time.time()-start_time))
            counter += 1
        print('Total spent - %d sec' % total_spent)

        return banks_main_info, forms

    @property
    def closed_banks_list(self):
        """Returns list of Closed Bank class instances"""

        if self.closed_descriptions is None:
            raise ValueError('Closed banks should be loaded first!')

        return [Bank(bank.index, bank.license_number, bank.full_name)
                    for bank in self.closed_descriptions.itertuples(index=False)
                        if bank.index > 0]
    @property
    def active_banks_list(self):
        """Returns list of Active Bank class instances"""

        if self.active_banks is None:
            raise ValueError('Active banks should be loaded first!')

        return [Bank(bank.id,bank.license_number,bank.name)
                    for bank in self.active_banks.itertuples(index=False)]
