#!/usr/bin/env python3
import pandas as pd
import pymysql

from registro import cvm, cvm2symbol, get_result
from datetime import datetime
from tqdm import tqdm
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR, FLOAT, DATETIME, BIGINT
from exception_util import sendMail
from time import sleep

pymysql.install_as_MySQLdb()

# Global variables
lista = []
dia = None

print(
'''
#############################################################################
#        Programa para atualizar os preços dos ativos da planilha           #
#############################################################################
'''
    )

# Load parameters
param = pd.read_csv('db_param.csv', header=0)
host=param.host.values[0]
user=param.user.values[0]
password=param.password.values[0]
db=param.db.values[0]

@sendMail()
def main():
    # Connect to the database
    conn = create_engine(f'mysql+mysqldb://{user}:{password}@{host}:3306/{db}', echo = False)
    print ('Connection success.')

    # Get registers table
    print('Reading companies registers.')
    columns = ['cnpj', 'name', 'type', 'cvm_code', 'situation']
    result = conn.execute(f'SELECT * FROM cvm_dfps.cvmregistro;').fetchall()
    reg = pd.DataFrame(result, columns=columns)

    # Get result table
    res = get_result()

    # Get cvm+symbol price table and save to MySQL db
    cs = cvm2symbol(reg.cvm_code, res.set_index('symbol'))
    types = dict(zip(cs.columns, [BIGINT, NVARCHAR(length=6), DATETIME]))
    cs.to_sql('cvm2symbol', conn, if_exists='replace', index=False, dtype=types)

if __name__ == '__main__':
    main()
        

