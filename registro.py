import string
import pandas as pd
import sqlite3
import re

from urllib.request import urlopen
from datetime import datetime
from bs4 import BeautifulSoup
from fundamentus import get_data
from tqdm import tqdm
from exception_util import exception, create_logger, retry

# Create instances of loggers
cvm_logger = create_logger('cvm_logger')
result_logger = create_logger('result_logger')
cvm2symbol_logger = create_logger('cvm2symbol_logger')

@retry()
@exception(cvm_logger)
def cvm():
	"""
	Get registration of all companies listed in CVM.

	This function is a crawler which get all registration information from companies on cvmweb page.cvmweb

	Parameters
	----------
	None

	Returns
	-------
	DataFrame
	    The dataframe with fields ['cnpj', 'name', 'type', 'cvm_code', 'situation']
	"""

	# Define url base
	url = 'http://cvmweb.cvm.gov.br/SWB/Sistemas/SCW/CPublica/CiaAb/FormBuscaCiaAbOrdAlf.aspx?LetraInicial='
	
	# Get alphanum uppercase set to use in page index
	alphanum =string.ascii_lowercase.upper() + ''.join(list(map(str,range(10))))
	# Attribute values to identify table lines of interest
	colors = ['Cornsilk','#FAEFCA']

	# Loop through index pages and append registration information to data list
	data = list()
	for letra_inicial in tqdm(alphanum, desc='Reading companies', unit='tabs'):
		# get html
		with urlopen(url+f'{letra_inicial}') as html:
			soup = BeautifulSoup(html, 'html.parser')
		try:
			# loop through table lines retrieving fields values
			for row in soup.find_all('tr', bgcolor=True):
				row_tup = tuple()
				# check the attribute matching
				if row['bgcolor'] in colors:
					for field in row.find_all('td'):
						row_tup += (field.get_text(),)
					data.append(row_tup)
		except:
			continue

	# Store data in dataframe
	columns = ['cnpj', 'name', 'type', 'cvm_code', 'situation']
	df =  pd.DataFrame(data, columns=columns)
	df['cvm_code'] = df['cvm_code'].apply(int)

	return df

@retry()
@exception(cvm2symbol_logger)
def cvm2symbol(cvm_codes, cvm_prices_and_liq):
	"""
	Get most relevant symbol price with cvm_code information

	This function is a crawler which get all symbols from companies listed in cvm and then retrieve between the symbols with same cvm_code the one with best liq.

	Parameters
	----------
	cvm_codes : list or numpy.array
		List of cvm_codes
	cvm_prices_and_liq : DataFrame or numpy.ndarray
		Table indexed by symbol name and with price and liq info
		
	
	Returns
	-------
	DataFrame
	    The dataframe with fields ['cvm_code', 'symbol', 'price', 'date']
	"""

	# Define cvm symbols source url
	url = 'http://bvmf.bmfbovespa.com.br/pt-br/mercados/acoes/empresas/ExecutaAcaoConsultaInfoEmp.asp?CodCVM='

	# Get symbols at url entering adding cmv_code to query
	cvm_symbol = []
	for code in tqdm(cvm_codes, desc='Reading prices', unit='codes'):
		with urlopen(url+f'{code}') as html:
			soup = BeautifulSoup(html, 'html.parser')
		liq = .0
		symbol = None
		# Take the symbol with best liq for this cvm_code
		for row in soup.find_all('a', "LinkCodNeg"):
			tmp_symbol = row.get_text().strip()

			# Evaluate if symbol exists
			if tmp_symbol in cvm_prices_and_liq.index:
				tmp_liq = convertNum(cvm_prices_and_liq.loc[tmp_symbol].liq)
				if tmp_liq < liq:
					continue
				liq = tmp_liq
				symbol = tmp_symbol

		# Skip when no symbol 
		if symbol:
			cvm_symbol.append((code, symbol, convertNum(cvm_prices_and_liq.loc[symbol].price), pd.to_datetime(cvm_prices_and_liq.loc[symbol].date)))

	return pd.DataFrame(cvm_symbol, columns=['cvm_code', 'symbol', 'price', 'date'])

@retry()
@exception(result_logger)
def get_result():
	"""
	Get a table with cotacao and liq info from fundamentus page.

	Parameters
	----------
	None
	
	Returns
	-------
	DataFrame
	    The dataframe with fields ['symbol', 'price', 'liq', 'date']
	"""

	global lista, dia

	# Use get_data from fundamentus to get list of stats by symbol
	resultado = []
	lista, dia = dict(get_data()), datetime.strftime(datetime.today(), '%d')
	
	# Save day of update
	date = datetime.strftime(datetime.today(), '%d-%m-%y %H:%M:%S')

	# Select just cotaco and liq values fields.
	for key, value in tqdm(lista.items(), desc='Retrieving info', unit='registers'):
		resultado.append((key, value['cotacao'], value['Liq.2m.'], date))

	return pd.DataFrame(resultado, columns=['symbol', 'price', 'liq', 'date'])

def convertNum(number_string):
	return float(re.sub(',', '.', re.sub('\.', '', number_string)))



if __name__ == '__main__':

	# test cvm2symbol
	lst = ['906', '9512']
	data = [
		['BBDC4', '2.00', '5', pd.to_datetime('10-04-2018')],
		['BBDC3', '3.00', '6', pd.to_datetime('10-04-2018')],
		['PETR3', '5.00', '5', pd.to_datetime('10-04-2018')],
		['PETR4', '6.00', '6', pd.to_datetime('10-04-2018')],
	]

	df = pd.DataFrame(data, columns=['symbol', 'price', 'liq', 'date'])

	print(cvm2symbol(lst, df.set_index('symbol')))

	# test convertNum
	print(convertNum('14.023,30'))

