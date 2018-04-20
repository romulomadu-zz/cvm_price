import functools
import logging
import smtplib
import pandas as pd


from functools import wraps
from time import sleep 


def retry(times=3, waiting_time=30):
    """
    Decorator to retry any functions 'times' times.
    """
    def retry_decorator(func):
        @wraps(func)
        def retried_function(*args, **kwargs):
            for i in range(times):
                try:                           
                    return func(*args, **kwargs)
                except Exception as err:
                    print(f'Try nº {i+1}')
                    sleep(waiting_time)           

            func(*args, **kwargs)

        return retried_function

    return retry_decorator

# exception_decor.py 
 
def exception(logger):
    """
    A decorator that wraps the passed in function and logs 
    exceptions should one occur
 
    @param logger: The logging object
    """
 
    def decorator(func):
 
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except:
                # log the exception
                err = "There was an exception in "
                err += func.__name__
                logger.exception(err)
 
                # re-raise the exception
                raise
        return wrapper
    return decorator
 
def create_logger(log_name='example_logger'):
    """
    Creates a logging object and returns it
    """
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.INFO)
 
    # create the logging file handler
    fh = logging.FileHandler(r"service.log")
 
    fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)
    fh.setFormatter(formatter)
 
    # add handler to logger object
    logger.addHandler(fh)
    return logger


def MySendMail(message):

    param = pd.read_csv('mail_param.csv', header=0)
    fromaddr = param.fromaddr.values[0]
    toaddrs  = param.toaddrs.values[0]
    username = param.username.values[0]
    password = param.password.values[0]

    msg ="\r\n".join([
      f"From: {fromaddr}",
      f"To: {toaddrs}",
      "Subject: [APPLICATION] Error",
      "",
      f"{message}"
      ])

    server = smtplib.SMTP('smtp.gmail.com:587')
    server.ehlo()
    server.starttls()

    server.login(username,password)
    server.sendmail(fromaddr, toaddrs, msg)
    server.quit()

if __name__ == '__main__':
    # test MySendMail
    MySendMail('Hello!')