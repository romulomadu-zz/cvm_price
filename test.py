import pandas as pd
import pymysql

from exception_util import exception, create_logger, retry, MySendMail
from registro import cvm, cvm2symbol, get_result
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR, FLOAT, DATETIME, BIGINT
from exception_util import MySendMail
from time import sleep

pymysql.install_as_MySQLdb()


param = pd.read_csv('db_param.csv', header=0)
# Database parameters
host=param.host.values[0]
user=param.user.values[0]
password=param.password.values[0]
db=param.db.values[0]

if __name__ == '__main__':
	'''
	logger =  create_logger()

	@retry(waiting_time=1)
	@exception(logger)
	def division_zero():
		return 1/0

	try:
		#division_zero()
		cvm()
	except Exception as e:
		print(e)
		MySendMail(e)

	'''

	try:    
		# Connect to the database
		conn = create_engine(f'mysql+mysqldb://{user}:{password}@{host}:3306/{db}', echo = False)
		print ('Connection success.')

		# Get register table
		result = conn.execute(f'SELECT * FROM cvm_dfps.cvmregistro;').fetchall()       
		print(pd.DataFrame(result))
    
	except Exception as e:
		MySendMail(e)