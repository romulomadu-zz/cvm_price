import functools
import logging
import smtplib
import pandas as pd

from functools import wraps
from time import sleep 

def sendMail():
    '''
    Decorator to send mail when some exception occurs.
    '''
    def mail_decorator(func):
        @wraps(func)
        def sendmail_funcion(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:

                #Senf mail function
                MySendMail(e)
        
        return sendmail_funcion

    return mail_decorator



def retry(times=3, waiting_time=30):
    '''
    Decorator to retry any functions 'times' times.
    
    Parameters
    ----------    
    times: int
        Number of times to retry to execute
    waiting_time: int
        Number of times to wait between retries

    '''
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

 
def exception(logger):
    '''
    Decorator that wraps the passed function and logs 
    exceptions when it occurs.
 
    Parameters
    ----------
    logger : logging object
        Object to use to log exceptions
    
    ''' 
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
    '''
    Create a logging object.

    Parameters
    ----------
    log_name : str
        Name of logging object
    
    Returns
    ----------
    logger : logging object
        Configured logging object

    '''

    # Attribute log name to object instance.
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.INFO)
 
    # Create the logging file handler
    fh = logging.FileHandler(r"service.log")
 
    fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)
    fh.setFormatter(formatter)
 
    # add handler to logger object
    logger.addHandler(fh)
    return logger

def MySendMail(message):
    '''
    Send email to report a message.

    Parameters
    ----------
    message : str
        Message to deliver
    
    Returns
    ----------
    None

    '''

    # Load email parameters from mail_param.csv
    param = pd.read_csv('mail_param.csv', header=0)
    fromaddr = param.fromaddr.values[0]
    toaddrs  = param.toaddrs.values[0]
    username = param.username.values[0]
    password = param.password.values[0]

    # Create email message
    msg ="\r\n".join([
      f"From: {fromaddr}",
      f"To: {toaddrs}",
      "Subject: [APPLICATION] Error",
      "",
      f"{message}"
      ])

    # Validate server
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.ehlo()
    server.starttls()

    # Login, send email and close
    server.login(username,password)
    server.sendmail(fromaddr, toaddrs, msg)    
    server.quit()

if __name__ == '__main__':
    # Create logger
    logger =  create_logger()
    
    # Add retry and exception decorators to a function which will give an error
    @sendMail()
    @retry(waiting_time=1)
    @exception(logger)
    def division_zero():
        return 1/0

    # Test    
    division_zero()
