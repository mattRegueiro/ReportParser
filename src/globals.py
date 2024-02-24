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
LOGGING_DIR         = f"logs/{dt.datetime.now().strftime('%Y_%m_%d')}"
REPORTS_DIR         = "reports"
OUTPUT_DIR          = f"output/{dt.datetime.now().strftime('%Y_%m_%d')}"
BATCH_FILE_SIZE     = 5

PROG_LOGGER         = None

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

PROPERTY_ROOMS      = [ (101, 104),             # Rooms 101 - 104
                        (201, 204),             # Rooms 201 - 204
                        (301, 308),             # Rooms 301 - 308
                        (401, 406),             # Rooms 401 - 406
                        (501, 506),             # Rooms 501 - 506
                        (601, 608),             # Rooms 601 - 608
                        (701, 708),             # Rooms 701 - 708
                        (801, 808),             # Rooms 801 - 808
                        (901, 908),             # Rooms 901 - 908
                        (1001, 1008),           # Rooms 1001 - 1008
                        (1101, 1108),           # Rooms 1101 - 1108
                        (1201, 1208),           # Rooms 1201 - 1208
                        (1401, 1412),           # Rooms 1401 - 1412
                        (1500, -1) ]            # Room 1500
