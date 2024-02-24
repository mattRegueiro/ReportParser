'''
=========================================================================
* report_parser.py                                                      *
=========================================================================
* This is the main file for the report_parser program. The program will *
* parse data from generated pdf file reports and write them to excel    *
* files for accessibility and ease.                                     *
=========================================================================
'''
import sys
import datetime as dt
import src.misc as _misc
import src.globals as _global

from src.pdf import PDFParser



'''
=========================================================================
* main()                                                                *
=========================================================================
* This function is the main function of the report_parser program. This *
* function will initialize the program and PDFParser class object. Then *
* the PDFParser object will parse all pdf file reports located in the   *
* /reports directory. Once all pdf file reports have been parsed, it    *
* will then output excel tables to the user.                            *
*                                                                       *
*   INPUT:                                                              *
*         None                                                          *
*                                                                       *
*   OUPUT:                                                              *
*         None                                                          *
=========================================================================
'''
if __name__ == "__main__":
    program_start_time = dt.datetime.now()  # Start program execution timer

    _misc.initialize()                      # Initialize program

    # Initialize PDFParser object
    parser = PDFParser(pdf_dir=_global.REPORTS_DIR, batch_size=_global.BATCH_FILE_SIZE)
    
    # If pdf report files not found
    if not parser._has_pdf_files():
        _global.PROG_LOGGER.error(f"No PDF report files found in {_global.REPORTS_DIR}")
        _misc.press_key_to_continue()
        sys.exit(1)

    parser._get_pdf_table_data()            # Extract pdf table data

    # If error occurred during execution
    if not parser._is_complete():
        _misc.press_key_to_continue()
        sys.exit(1)

    parser._build_excel_tables()            # Build consolidated data table
    parser._output_tables()                 # Output tables to excel

    program_end_time = (dt.datetime.now() - program_start_time).total_seconds()
    _global.PROG_LOGGER.info(f"Program execution completed in {program_end_time:.2f} seconds")
