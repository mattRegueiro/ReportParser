'''
=========================================================================
* mt_misc.py                                                            *
=========================================================================
* This file contains general/misc. functions that are used throughout   *
* the report_parser program execution.                                  *
=========================================================================
'''
import os
import sys
import datetime as dt
import logging as log
import src.globals as _global



'''
=========================================================================
* initialize()                                                          *
=========================================================================
* This function initializes the report_parser program by creating       *
* logging, report, and output directories and setting up the program    *
* logger.                                                               *
*                                                                       *
*   INPUT:                                                              *
*         None                                                          *
*                                                                       *
*   OUPUT:                                                              *
*         None                                                          *
=========================================================================
'''
def initialize() -> None:
    mkdir(_global.LOGGING_DIR)              # Create /logging directory
    mkdir(_global.REPORTS_DIR)              # Create /reports directory
    mkdir(_global.OUTPUT_DIR)               # Create /output directory

    _global.PROG_LOGGER = setup_logger()    # Setup program logger

    return



'''
=========================================================================
* mkdir()                                                               *
=========================================================================
* This function will make a new folder directory if it does not already *
* exist.                                                                *
*                                                                       *
*   INPUT:                                                              *
*         folder (str) - The folder directory to be created.            *
*                                                                       *
*   OUPUT:                                                              *
*         None                                                          *
=========================================================================
'''
def mkdir(folder: str) -> None:
    # If directory does not already exist
    if not os.path.exists(folder):
        os.makedirs(folder)     # Make directory

    return



'''
=========================================================================
* setup_logger()                                                        *
=========================================================================
* This function sets up the program logger for errors and exceptions.   *
*                                                                       *
*   INPUT:                                                              *
*         None                                                          *
*                                                                       *
*   OUPUT:                                                              *
*         logger (logger)   - The program logger.                       *
=========================================================================
'''
def setup_logger() -> log.Logger:
    datetime_str = dt.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")  # Get current datetime as string
    log_file = f"{_global.LOGGING_DIR}/run_{datetime_str}.log"      # Create log file string

    # Create a file handler to write to log file
    file_handler = log.FileHandler(log_file, mode='w')
    file_handler.setLevel(log.INFO)
    file_formatter = log.Formatter("%(asctime)s | %(levelname)s | %(filename)s | %(funcName)s | %(message)s", datefmt="%Y-%m-%d %I:%M:%S %p")
    file_handler.setFormatter(file_formatter)

    # Create a stream handler to write to terminal
    stream_handler = log.StreamHandler(sys.stdout)
    stream_handler.setLevel(log.INFO)
    stream_formatter = log.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %I:%M:%S %p")
    stream_handler.setFormatter(stream_formatter)

    # Create root program logger
    log.basicConfig(level=log.INFO,                             # Set root logger level
                    handlers=[file_handler, stream_handler])    # Set root logger file and stream handlers

    return log.getLogger()



'''
=========================================================================
* press_key_to_continue()                                               *
=========================================================================
* This function prompts the user to press any key to continue program   *
* execution.                                                            *
*                                                                       *
*   INPUT:                                                              *
*         None                                                          *
*                                                                       *
*   OUPUT:                                                              *
*         None                                                          *
=========================================================================
'''
def press_key_to_continue() -> None:
    input("Press Enter to continue...")