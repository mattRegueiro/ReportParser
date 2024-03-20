'''
=========================================================================
* pdf.py                                                                *
=========================================================================
* This file contains the PDFParser class and all functions associated   *
* with extracting data from pdf file reports.                           *
=========================================================================
'''
import os
import re
import glob
import threading
import numpy as np
import pandas as pd
import logging as log
import datetime as dt
import src.misc as _misc
import concurrent.futures
import tabula.io as tabula
import src.globals as _global

from PyPDF2 import PdfReader
from collections import defaultdict



###################################################################
#   P D F P A R S E R   C L A S S                                 #
###################################################################
class PDFParser:
    '''
    =========================================================================
    * __init__()                                                            *
    =========================================================================
    * This function initializes all appropriate flags, lists, and tables    *
    * used to handle the processing and extraction of data from pdf file    *
    * reports.                                                              *
    *                                                                       *
    *   INPUT:                                                              *
    *           pdf_dir (str) - The directory where the pdf file reports    *
    *                           are located.                                *
    *        batch_size (int) - The number of pdf file reports to process   *
    *                           in a single batch.                          *
    *                                                                       *
    *   OUPUT:                                                              *
    *         None                                                          *
    =========================================================================
    '''
    def __init__(self, pdf_dir: str="", batch_size: int=1) -> None:
        self._reports_dir           : str                       = pdf_dir                   # Initialize pdf report files directory
        self._batch_size            : int                       = batch_size                # Initialize batch size

        self._logger                : log                       = _global.PROG_LOGGER       # Initialize program logger
        self._pdf_files             : list[str]                 = self._get_pdf_files()     # Initialize list of pdf files
        self._runtime_ms            : float                     = 0                         # Initialize pdf parser runtime (ms)

        self._pdf_reports           : dict[str, pd.DataFrame]   = {}                        # Initialize dataframe of extracted pdf report data
        self._excel_room_revenue    : dict[str, pd.DataFrame]   = {}                        # Initialize excel dataframe for revenue per room
        self._excel_room_booking    : dict[str, pd.DataFrame]   = {}                        # Initialize excel dataframe for bookings per room

        self._pdf_lock              : threading.Lock            =  threading.Lock()         # Initialize thread lock for extraction of pdf tables
        self._exec_complete         : bool                      = False                     # Initialize execution complete status flag
        
        return



    '''
    =========================================================================
    * _get_pdf_files()                                                      *
    =========================================================================
    * This function will return a list of all the pdf files located in the  *
    * /reports directory.                                                   *
    *                                                                       *
    *   INPUT:                                                              *
    *         None                                                          *
    *                                                                       *
    *   OUPUT:                                                              *
    *         list[str] - Returns a list containing the names of the pdf    *
    *                     files in the /reports directory.                  *
    =========================================================================
    '''
    def _get_pdf_files(self) -> list[str]:
        return glob.glob(self._reports_dir + '/*.pdf')      # Get all pdf reports in /reports directory



    '''
    =========================================================================
    * _has_pdf_files()                                                      *
    =========================================================================
    * This function will check to see if there are any pdf file reports in  *
    * the /reports directory.                                               *
    *                                                                       *
    *   INPUT:                                                              *
    *         None                                                          *
    *                                                                       *
    *   OUPUT:                                                              *
    *         True/False (bool) - True if pdf files found, False otherwise. *
    =========================================================================
    '''
    def _has_pdf_files(self) -> bool:
        return True if self._pdf_files else False           # Return True if pdf reports found, else False



    '''
    =========================================================================
    * _is_complete()                                                        *
    =========================================================================
    * This function will return the completion status of the report parsing *
    * execution.                                                            *
    *                                                                       *
    *   INPUT:                                                              *
    *         None                                                          *
    *                                                                       *
    *   OUPUT:                                                              *
    *         _exec_complete (bool) - Report parsing execution status flag. *
    =========================================================================
    '''
    def _is_complete(self) -> bool:
        return self._exec_complete                              # Return execution complete status flag



    '''
    =========================================================================
    * _update_column_datatype()                                             *
    =========================================================================
    * This function will update the datatype of a dataframe column.         *
    *                                                                       *
    *   INPUT:                                                              *
    *          column_idx (int) - The index of the column whose datatype    *
    *                             will be updated.                          *
    *           data_type (str) - The updated datatype.                     *
    *         table (DataFrame) - The dataframe with column to be updated.  *
    *                                                                       *
    *   OUPUT:                                                              *
    *         None                                                          *
    =========================================================================
    '''
    def _update_column_datatype(self, column_idx: int, data_type: str, table: pd.DataFrame) -> None:
        column_dtype = table[table.columns[column_idx]].dtype   # Get data type of dataframe column

        # If the column contains string values and the string values contain commas
        if pd.api.types.is_string_dtype(column_dtype) and table[table.columns[column_idx]].str.contains(',').any():
            table[table.columns[column_idx]] = table[table.columns[column_idx]].str.replace(",","")

        table[table.columns[column_idx]] = table[table.columns[column_idx]].fillna(-1).astype(data_type)
        
        return



    '''
    =========================================================================
    * _get_pdf_table_data()                                                 *
    =========================================================================
    * This function will extract the pdf file reports data in batches on    *
    * individual threads and combine them into a single dataframe.          *
    *                                                                       *
    *   INPUT:                                                              *
    *         None                                                          *
    *                                                                       *
    *   OUPUT:                                                              *
    *         None                                                          *
    =========================================================================
    '''
    def _get_pdf_table_data(self) -> None:
        batch_n = 1                                             # Initialize number of batches
        batch_results = []                                      # Initialize list of parsed data dictionaries
        start_time = dt.datetime.now()                          # Start batch processing timer

        # Use threads to process batches of pdf report files
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []

            # Iterate over pdf report file batches
            for i in range(0, len(self._pdf_files), self._batch_size):

                self._logger.info(f"Processing pdf report file batch #{batch_n}... please wait.")

                # Process each batch of pdf report files
                batch_files = self._pdf_files[i:i+self._batch_size]
                futures.append(executor.submit(self._process_batch, batch_n, batch_files))

                batch_n += 1

            # Add completed batches to result list
            for future in concurrent.futures.as_completed(futures):
                n, elapsed_time, result_dict = future.result()
                self._logger.info(f"Finished processing pdf report file batch #{n} in {elapsed_time:.2f} ms ({elapsed_time/1000:.2f} sec.)")
                batch_results.append(result_dict)

        # If batch results list is empty
        if not batch_results:
            self._logger.error("No batch results generated !!!")
            return

        # Combine batch results
        self._combine_results(batch_results)

        self._exec_complete = True

        # Stop batch processing timer (ms)
        self._runtime_ms = (dt.datetime.now() - start_time).total_seconds() * 1000
        self._logger.info(f"Processed {len(self._pdf_files)} pdf reports in {self._runtime_ms:.2f} ms ({self._runtime_ms/1000:.2f} sec.)")
        
        return 



    '''
    =========================================================================
    * _process_batch()                                                      *
    =========================================================================
    * This function will extract the pdf file reports data from a single    *
    * batch.                                                                *
    *                                                                       *
    *   INPUT:                                                              *
    *         batch_number (int) - The batch number.                        *
    *    batch_files (list[str]) - The list of pdf file reports to be parsed*
    *                              in a single batch.                       *
    *                                                                       *
    *   OUPUT:                                                              *
    *      tuple[int, int, dict] - A tuple containing the batch number, the *
    *                              elapsed time and a dictionary containing *
    *                              the combined parsed data from the        *
    *                              current batch.                           *
    =========================================================================
    '''
    def _process_batch(self, batch_number: int, batch_files: list[str]) -> tuple[int, int, defaultdict]:
        start_time = dt.datetime.now()      # Initialize batch processing start timer

        # Initialize batch dictionary of parsed data {year: month: room: room data}
        parsed_dict_batch = defaultdict(
                                lambda: defaultdict( 
                                    lambda: defaultdict(
                                        lambda: defaultdict(
                                            float
                                        )
                                    )
                                )
                            )

        # Iterate over each pdf file in our batch
        for file in batch_files:
            
            # Get the year the report was generated for
            year = self._get_report_year(file)

            # Read all pages of pdf file and extract all table information
            with self._pdf_lock:
                df_list = tabula.read_pdf(file, pages="all", lattice=False)

            # Iterate over each table from each pdf page
            for page, pdf_df in enumerate(df_list):
            
                # If dataframe is empty
                if pdf_df.empty:
                    self._logger.warning(f"'{file}' - PDF page #{page} dataframe is empty")                    
                    continue

                # Find and remove all "Unnamed" columns
                unnamed_columns = [col for col in pdf_df.columns if col.startswith("Unnamed")]
                if unnamed_columns:
                    pdf_df.drop(columns=unnamed_columns, inplace=True)

                col_idx = 0                                                                         # Initialize pdf dataframe column index
                row_start = pdf_df[pdf_df.columns[col_idx]].first_valid_index()                     # Set the index of the first sub-row in a pdf file table row
                row_end = pdf_df[pdf_df.columns[col_idx]].drop(row_start).first_valid_index()       # Update index of last sub-row

                # If last sub-row index not found
                if not row_end:
                    row_end = len(pdf_df)-1                                                         # Set the index of the last sub-row to the length of the pdf dataframe
                
                # Calculate the number of sub-rows in a pdf file table row
                n_sub_rows = row_end - row_start

                # Iterate over all pdf file table rows
                for row_idx in range(0, len(pdf_df), n_sub_rows):

                    # If we are on the last row of the last page
                    if row_idx == len(pdf_df)-1 and page == len(df_list)-1:
                        break   # Exit loop

                    # Get current row's room number
                    self._update_column_datatype(col_idx, "int32", pdf_df)
                    room_number = pdf_df[pdf_df.columns[col_idx]][row_idx]
                    col_idx = (col_idx + 1) % len(pdf_df.columns)

                    # Get list of current row's months
                    self._update_column_datatype(col_idx, "object", pdf_df)
                    months = pdf_df[pdf_df.columns[col_idx]][row_idx+1:row_idx+n_sub_rows-1]
                    col_idx = (col_idx + 1) % len(pdf_df.columns)

                    # Get list of number of arrivals for each month of current row's room number
                    self._update_column_datatype(col_idx, "int32", pdf_df)
                    n_arrivals = pdf_df[pdf_df.columns[col_idx]][row_idx+1:row_idx+n_sub_rows-1]
                    col_idx = (col_idx + 1) % len(pdf_df.columns)

                    # Get list of number of nights for each month of current row's room number
                    self._update_column_datatype(col_idx, "int32", pdf_df)
                    n_nights = pdf_df[pdf_df.columns[col_idx]][row_idx+1:row_idx+n_sub_rows-1]
                    col_idx = (col_idx + 1) % len(pdf_df.columns)

                    # Get list of room revenue for each month of current row's room number
                    self._update_column_datatype(col_idx, "float32", pdf_df)
                    room_revenue = pdf_df[pdf_df.columns[col_idx]][row_idx+1:row_idx+n_sub_rows-1]
                    col_idx = (col_idx + 1) % len(pdf_df.columns)

                    # Get list of room ADR for each month of current row's room number
                    self._update_column_datatype(col_idx, "float32", pdf_df)
                    room_adr = pdf_df[pdf_df.columns[col_idx]][row_idx+1:row_idx+n_sub_rows-1]
                    col_idx = (col_idx + 1) % len(pdf_df.columns)

                    # Iterate over each month for current room number
                    for i, month in enumerate(months.values):
                        # Skip over "Room No." and "Month" columns
                        col_idx = (col_idx + 2) % len(pdf_df.columns)

                        # Get number of room arrivals for current year/month/room number
                        parsed_dict_batch[year][month][room_number][pdf_df.columns[col_idx]] = n_arrivals.values[i]
                        col_idx = (col_idx + 1) % len(pdf_df.columns)

                        # Get number of room nights for current year/month/room number
                        parsed_dict_batch[year][month][room_number][pdf_df.columns[col_idx]] = n_nights.values[i]
                        col_idx = (col_idx + 1) % len(pdf_df.columns)

                        # Get room revenue for current year/month/room number
                        parsed_dict_batch[year][month][room_number][pdf_df.columns[col_idx]] = room_revenue.values[i]
                        col_idx = (col_idx + 1) % len(pdf_df.columns)

                        # Get room ADR for current year/month/room number
                        parsed_dict_batch[year][month][room_number][pdf_df.columns[col_idx]] = room_adr.values[i]
                        col_idx = (col_idx + 1) % len(pdf_df.columns)

        # Calculated elapsed time to complete batch (ms)
        elapsed_time = (dt.datetime.now() - start_time).total_seconds() * 1000

        return (batch_number, elapsed_time, parsed_dict_batch)



    '''
    =========================================================================
    * _get_report_year()                                                    *
    =========================================================================
    * This function will find and return the year the pdf report was        *
    * generated for.                                                        *
    *                                                                       *
    *   INPUT:                                                              *
    *         file (str) - The pdf report file.                             *
    *                                                                       *
    *   OUPUT:                                                              *
    *         year (str) - The pdf report year or the current year if the   *
    *                      pdf report year was not found.                   *
    =========================================================================
    '''
    def _get_report_year(self, file: str) -> str:
        reader = PdfReader(file)        # Initialize pdf reader
        first_page = reader.pages[0]    # Extract the text from the first page of the pdf report

        # Find and extract the filter year the report was generated for
        year_str = first_page.extract_text().split("\n")[2]
        match = re.search(r'\b\d{4}\b', year_str)

        return match.group() if match else dt.date.today().strftime("%Y")



    '''
    =========================================================================
    * _combine_results()                                                    *
    =========================================================================
    * This function will take a list of batch processing results and        *
    * combine them into a dataframe table. Each year processed will have    *
    * its own dataframe table.                                              *
    *                                                                       *
    *   INPUT:                                                              *
    *         batch_results (list) - List of batch processing results.      *
    *                                                                       *
    *   OUPUT:                                                              *
    *         None                                                          *
    =========================================================================
    '''
    def _combine_results(self, batch_results: list) -> None:
        # Iterate over each batch dictionary in batch results list
        for batch_dict in batch_results:

            # Iterate over each year / month dictionary pair
            for year, month_dict in batch_dict.items():

                # Iterate over each month / room dictionary pair
                for month, room_dict in month_dict.items():
                    _df = pd.DataFrame.from_dict(room_dict, orient="index") # Convert room dictionary for current year to dataframe
                    _df["Month"] = [month] * len(_df.index)                 # Add current month to dataframe
                    _df = _df.applymap(lambda x: [x])                       # Convert each column's values to lists
                    
                    # If current year does not exist in pdf reports dataframe
                    if year not in self._pdf_reports:
                        self._pdf_reports[year] = _df                       # Add dataframe to pdf reports dataframe
                        continue

                    # Concatenate pdf reports dataframe and new dataframe (group by index and aggregate values into lists)
                    concat_df = pd.concat([self._pdf_reports[year], _df])
                    self._pdf_reports[year] = concat_df.groupby(level=0).agg(lambda x: list(np.concatenate(x.tolist())))

        return



    '''
    =========================================================================
    * _build_excel_tables()                                                 *
    =========================================================================
    * This function will build the excel tables from the parsed pdf report  *
    * files data.                                                           *
    *                                                                       *
    *   INPUT:                                                              *
    *         None                                                          *
    *                                                                       *
    *   OUPUT:                                                              *
    *         None                                                          *
    =========================================================================
    '''
    def _build_excel_tables(self) -> None:
        # Iterate over each yearly dataframe
        for year, _df in self._pdf_reports.items():

            # Create excel dataframes for revenue and bookings per room for current year
            self._excel_room_revenue[year] = pd.DataFrame(index=_df.index)
            self._excel_room_booking[year] = pd.DataFrame(index=_df.index)

            # Iterate over each row in dataframe
            for room, row in _df.iterrows():

                # Iterate over each month in dataframe "Month" column
                for i, month in enumerate(row["Month"]):

                    # Add revenue and nightly stay for each room corresponding to the current month
                    self._excel_room_revenue[year].at[room, month] = row["Room Revenue"][i]
                    self._excel_room_booking[year].at[room, month] = row["Room Nights"][i]

            # Reindex the month columns to be in the correct order and calculate the yearly total revenue
            self._excel_room_revenue[year]["Yearly Total"] = self._excel_room_revenue[year].sum(axis=1, skipna=True)
            self._excel_room_revenue[year] = self._excel_room_revenue[year].reindex(_global.MONTH_NAMES + ["Yearly Total"], axis=1).fillna(0).astype("float32")

            # Reindex the month columns to be in the correct order and calculate the yearly total bookings
            self._excel_room_booking[year]["Yearly Total"] = self._excel_room_booking[year].sum(axis=1, skipna=True)
            self._excel_room_booking[year] = self._excel_room_booking[year].reindex(_global.MONTH_NAMES + ["Yearly Total"], axis=1).fillna(0).astype("int32")

        return



    '''
    =========================================================================
    * _output_tables()                                                      *
    =========================================================================
    * This function will output all dataframe tables as excel files to the  *
    * /output directory.                                                    *
    *                                                                       *
    *   INPUT:                                                              *
    *         None                                                          *
    *                                                                       *
    *   OUPUT:                                                              *
    *         None                                                          *
    =========================================================================
    '''
    def _output_tables(self) -> None:
        self._output_df_table(name="pdfData", table=self._pdf_reports)               # Output pdf report data to excel file
        self._output_df_table(name="roomRevenue", table=self._excel_room_revenue)    # Output room revenue data to excel file
        self._output_df_table(name="roomBooking", table=self._excel_room_booking)    # Output room booking data to excel file
        
        return



    '''
    =========================================================================
    * _output_df_table()                                                    *
    =========================================================================
    * This function will output a single dataframe table as an excel file   *
    * to the /output directory.                                             *
    *                                                                       *
    *   INPUT:                                                              *
    *           name (str) - The name of the excel file.                    *
    *         table (dict) - The dictionary containing the dataframe(s) to  *
    *                        write to excel file.                           *
    *                                                                       *
    *   OUPUT:                                                              *
    *         None                                                          *
    =========================================================================
    '''
    def _output_df_table(self, name: str, table: dict[str, pd.DataFrame]) -> None:
        # Iterate over each yearly dataframe for current table
        for year, _df in table.items():

            _misc.mkdir(f"{_global.OUTPUT_DIR}/{year}")                             # Make output directory for current year
            excel_file_path = f"{_global.OUTPUT_DIR}/{year}/{name}.xlsx"            # Set excel output file path for pdf data table 

            # If excel data file already exists AND output table is revenue or booking
            if os.path.exists(excel_file_path) and name != "pdfData":
                excel_df = pd.read_excel(excel_file_path, index_col=0)              # Read the excel file and store it into a dataframe
                excel_df = excel_df.astype(excel_df.dtypes.value_counts().idxmax()) # Set the excel dataframe datatype
                columns_to_add = _df.columns[(_df != excel_df).any()]               # Find columns to add to excel dataframe

                # Iterate over all missing columns (exclude last column ("Yearly Total"))
                for i in range(len(columns_to_add)-1):
                    
                    # If column does not contain all zero data
                    if (excel_df[columns_to_add[i]] != 0).any():
                        continue                                                    # Skip over column

                    excel_df[columns_to_add[i]] = _df[columns_to_add[i]]            # Replace excel dataframe column values
                
                # Calculate new yearly total
                excel_df["Yearly Total"] = excel_df.iloc[:, :-1].sum(axis=1, skipna=True)
                _df = excel_df

            # Write the DataFrame to Excel with accounting formatting
            excel_writer = pd.ExcelWriter(excel_file_path, engine='xlsxwriter')
            _df.to_excel(excel_writer, sheet_name=name)

            workbook = excel_writer.book                                            # Get the excel workbook 
            worksheet = excel_writer.sheets[name]                                   # Get the excel worksheet

            # Apply accounting format to desired columns
            for col_num, col_name in enumerate(_df.columns):
                col_format = workbook.add_format()                                  # Add formatting for excel column
                col_format.set_num_format(0)                                        # Initialize excel column formatting to generic

                # If column datatype is floating point
                if np.issubdtype(_df[col_name].dtype, np.floating):
                    col_format.set_num_format(44)                                   # Set excel formatting to accounting

                # Else if column datatype is 
                elif np.issubdtype(_df[col_name].dtype, np.integer):
                    col_format.set_num_format(1)                                    # Set excel formatting to numeric
                
                col_width = max(_df[col_name].astype(str).map(len).max(), len(col_name))
                worksheet.set_column(col_num + 1, col_num + 1, col_width + 5, col_format)
            
            # Save the excel file
            excel_writer.close()

        return
