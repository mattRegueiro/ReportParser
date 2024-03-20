'''
=====================================================================
* globals.py                                                        *
=====================================================================
* This file contains all global variables used by the report_parser *
* program.                                                          *
=====================================================================
'''
import datetime as dt


###################################################################
#   G L O B A L   P A T H S   A N D   V A R I A B L E S           #
###################################################################
PROG_LOGGER         = None
LOGGING_DIR         = f"logs/{dt.datetime.now().strftime('%Y_%m_%d')}"
REPORTS_DIR         = "reports"
OUTPUT_DIR          = f"output/{dt.datetime.now().strftime('%Y_%m_%d')}"
BATCH_FILE_SIZE     = 5

MONTH_NAMES         = [ "January",
                        "February",
                        "March",
                        "April",
                        "May",
                        "June",
                        "July",
                        "August",
                        "September",
                        "October",
                        "November",
                        "December" ]
