import pandas as pd

from bs4 import BeautifulSoup
from urllib.request import urlopen


class ActiveBanks:

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


        cbr_bank_list = [Bank(bank.id,bank.license_number,bank.name)
                         for bank in cbr_bank_list_df.itertuples(index=False)]

        self.data_frame = cbr_bank_list_df
        self.bank_list = cbr_bank_list

        return self

    def load_form(self, form_number, bank_list=self.BankList):
        pass
