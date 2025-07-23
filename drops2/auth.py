from drops2.utils import DropsCredentials
import logging

def set_credentials(url, user, password):
    """sets the authentication credentials for drops2"""
    DropsCredentials.set(url, user, password)

def init_credentials():
    """initialize the credentials for the current session"""
    DropsCredentials.load()
    logging.info("Credentials loaded from .drops.rc")

try:
    init_credentials()
except FileNotFoundError as err:
    #print("WARNING: could not read the config file " + err.filename + ". " + err.strerror)
    #print("You have to set the credentials using drops2.set_credentials in order to access the data from DDS.")
    pass
    