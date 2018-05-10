#!/usr/bin/env python3
import pandas as pd
import pymysql

from registro import cvm
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR, FLOAT, DATETIME, BIGINT
from exception_util import sendMail
from time import sleep

pymysql.install_as_MySQLdb()

print(
'''
#############################################################################
#        Programa para atualizar os dados de registro das cias na CVM       #
#############################################################################
'''
    )

# Load database parameters
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
    reg = cvm()        
    reg.to_sql('cvmregistro', conn, if_exists='replace', index=False)

if __name__ == '__main__':
    main()
 


