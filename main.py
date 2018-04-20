#!/usr/bin/env python3
import pandas as pd
import pymysql

from registro import cvm, cvm2symbol, get_result
from datetime import datetime
from tqdm import tqdm
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR, FLOAT, DATETIME, BIGINT
from exception_util import MySendMail

pymysql.install_as_MySQLdb()

# Global variables
lista = []
dia = None

print(
'''
#############################################################################
#        Programa para atualizar os preços dos ativos para planilha         #
#############################################################################
'''
    )


param = pd.read_csv('db_param.csv', header=0)
# Database parameters
host=param.host.values[0]
user=param.user.values[0]
password=param.password.values[0]
db=param.db.values[0]


if __name__ == '__main__':

    # Connect to the database
    conn = create_engine(f'mysql+mysqldb://{user}:{password}@{host}:3306/{db}', echo = False)
    print ('Connection success.')

    # Get register table
    reg = cvm()

    # Get result table
    res = get_result()

    try:
        # Get cvm+symbol price table na save to MySQL db
        cs = cvm2symbol(reg.cvm_code, res.set_index('symbol'))
        types = dict(zip(cs.columns, [BIGINT, NVARCHAR(length=6), FLOAT, DATETIME]))
        cs.to_sql('precos', conn, if_exists='replace', index=False, dtype=types)
    except Exception as e:
        MySendMail(e)


