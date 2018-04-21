from exception_util import exception, create_logger, retry, MySendMail



if __name__ == '__main__':

	logger =  create_logger()

	@retry()
	@exception(logger)
	def division_zero():
		return 1/0

	try:
		division_zero()
	except Exception as e:
		print(e)
		MySendMail(e)