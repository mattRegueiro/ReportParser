'''
=========================================================================
* pdf.py                                                                *
=========================================================================
* This file contains the PDFParser class and all functions associated   *
* with extracting data from pdf file reports.                           *
=========================================================================
'''
import glob
import tabula
import numpy as np
import pandas as pd
import logging as log
import datetime as dt
import concurrent.futures
import src.globals as _global

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
        self._reports_dir           : str           = pdf_dir                   # Initialize pdf report files directory
        self._batch_size            : int           = batch_size                # Initialize batch size

        self._logger                : log           = _global.PROG_LOGGER       # Initialize program logger

        self._pdf_files             : list[str]     = self._get_pdf_files()     # Initialize list of pdf files
        self._runtime_ms            : int           = 0                         # Initialize pdf parser runtime (ms)
        self._pdf_reports_df        : pd.DataFrame  = pd.DataFrame()            # Initialize dataframe of extracted pdf report data
        self._excel_room_revenue_df : pd.DataFrame  = None                      # Initialize excel dataframe for revenue per room
        self._excel_room_booking_df : pd.DataFrame  = None                      # Initialize excel dataframe for bookings per room

        self._exec_complete         : bool          = False                     # Initialize execution complete status flag
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
        return glob.glob(self._reports_dir + '/*.pdf')          # Get all pdf reports in /reports directory



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
        return True if self._pdf_files else False               # Return True if pdf reports found, else False



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

            # Iterate over pdf report file batches
            for i in range(0, len(self._pdf_files), self._batch_size):

                self._logger.info(f"Processing pdf report file batch #{batch_n}...")

                # Process each batch of pdf report files
                batch_files = self._pdf_files[i:i+self._batch_size]
                futures = [executor.submit(self._process_batch, batch_n, batch_files)]

                # Add completed batches to result list
                for future in concurrent.futures.as_completed(futures):
                    n, result_dict = future.result()
                    self._logger.info(f"Finished processing pdf report file batch #{n}")
                    batch_results.append(result_dict)

        # If batch results list is empty
        if not batch_results:
            self._logger.error("No batch results generated !!!")
            return

        parsed_dict_combined = batch_results[0]             # Set the first parsed data dictionary as the base dictionary

        # Iterate over the remaining batch dictionaries
        for i in range(1, len(batch_results)):

            # Iterate over each batch dictionary key/value pair
            for key, value in batch_results[i].items():
                parsed_dict_combined[key].extend(value)     # Add batch dictionary data to base dictionary
        

        # Convert batch dictionary to dataframe
        self._pdf_reports_df = pd.DataFrame(parsed_dict_combined)
        self._exec_complete = True

        # Stop batch processing timer
        self._runtime_ms = (dt.datetime.now() - start_time).total_seconds() * 1000
        self._logger.info(f"Processed {len(self._pdf_files)} pdf reports in {self._runtime_ms:.2f} ms")
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
    *           tuple[int, dict] - A tuple containing the batch number and  *
    *                              a dictionary containing the combined     *
    *                              parsed data from the current batch.      *
    =========================================================================
    '''
    def _process_batch(self, batch_number: int, batch_files: list[str]) -> tuple[int, defaultdict(list)]:
        parsed_dict_batch = defaultdict(list)                   # Initialize batch dictionary of parsed data

        # Iterate over each pdf file in our batch
        for file in batch_files:

            # Read all pages of pdf file and extract all table information
            df_list = tabula.read_pdf(file, pages="all", lattice=False)

            # Iterate over each table from each pdf page
            for page, pdf_df in enumerate(df_list):
            
                # If dataframe is empty
                if pdf_df.empty:
                    self._logger.warning(f"'{file}' - PDF page #{page} dataframe is empty")                    
                    continue

                col_idx = 0                                                                         # Initialize pdf dataframe column index
                row_start = pdf_df[pdf_df.columns[col_idx]].first_valid_index()                     # Set the index of the first sub-row in a pdf file table row
                row_end = len(pdf_df)-1                                                             # Set the index of the last sub-row in a pdf file table row

                # If we are not at the last page of pdf report file
                if page < len(df_list)-1:
                    row_end = pdf_df[pdf_df.columns[col_idx]].drop(row_start).first_valid_index()   # Update index of last sub-row
                
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
                    parsed_dict_batch[pdf_df.columns[col_idx]].append(room_number)                  # Add room number to parsed data dictionary
                    col_idx = (col_idx + 1) % len(pdf_df.columns)                                   # Increment pdf dataframe column index

                    # Get list of current row's months
                    self._update_column_datatype(col_idx, "object", pdf_df)
                    months = pdf_df[pdf_df.columns[col_idx]][row_idx+1:row_idx+n_sub_rows-1]
                    parsed_dict_batch[pdf_df.columns[col_idx]].append(months.values)                # Add list of months to parsed data dictionary
                    col_idx = (col_idx + 1) % len(pdf_df.columns)                                   # Increment pdf dataframe column index

                    # Get list of number of arrivals for each month of current row's room number
                    self._update_column_datatype(col_idx, "int32", pdf_df)
                    n_arrivals = pdf_df[pdf_df.columns[col_idx]][row_idx+1:row_idx+n_sub_rows-1]
                    parsed_dict_batch[pdf_df.columns[col_idx]].append(n_arrivals.values)            # Add number of arrivals to parsed data dictionary

                    # Get total number of arrivals for each month of current row's room number
                    try:
                        total_arrivals = pdf_df[pdf_df.columns[col_idx]][row_idx+n_sub_rows-1]
                    except KeyError:
                        total_arrivals = sum(n_arrivals)
                    parsed_dict_batch[f"Total {pdf_df.columns[col_idx]}"].append(total_arrivals)    # Add total number of arrivals to parsed data dictionary
                    col_idx = (col_idx + 1) % len(pdf_df.columns)                                   # Increment pdf dataframe column index

                    # Get list of number of nights for each month of current row's room number
                    self._update_column_datatype(col_idx, "int32", pdf_df)
                    n_nights = pdf_df[pdf_df.columns[col_idx]][row_idx+1:row_idx+n_sub_rows-1]
                    parsed_dict_batch[pdf_df.columns[col_idx]].append(n_nights.values)              # Add number of nights to parsed data dictionary

                    # Get total number of nights for each month of current row's room number
                    try:
                        total_nights = pdf_df[pdf_df.columns[col_idx]][row_idx+n_sub_rows-1]
                    except KeyError:
                        total_nights = sum(n_nights)
                    parsed_dict_batch[f"Total {pdf_df.columns[col_idx]}"].append(total_nights)      # Add total number of nights to parsed data dictionary
                    col_idx = (col_idx + 1) % len(pdf_df.columns)                                   # Increment pdf dataframe column index

                    # Get list of room revenue for each month of current row's room number
                    self._update_column_datatype(col_idx, "float32", pdf_df)
                    room_revenue = pdf_df[pdf_df.columns[col_idx]][row_idx+1:row_idx+n_sub_rows-1]
                    parsed_dict_batch[pdf_df.columns[col_idx]].append(room_revenue.values)          # Add room revenue to parsed data dictionary

                    # Get total revenue of current row's room number
                    try:
                        total_revenue = pdf_df[pdf_df.columns[col_idx]][row_idx+n_sub_rows-1]
                    except KeyError:
                        total_revenue = sum(room_revenue)
                    parsed_dict_batch[f"Total {pdf_df.columns[col_idx]}"].append(total_revenue)     # Add total room revenue to parsed data dictionary
                    col_idx = (col_idx + 1) % len(pdf_df.columns)                                   # Increment pdf dataframe column index

                    # Get list of room ADR for each month of current row's room number
                    self._update_column_datatype(col_idx, "float32", pdf_df)
                    room_adr = pdf_df[pdf_df.columns[col_idx]][row_idx+1:row_idx+n_sub_rows-1]
                    parsed_dict_batch[pdf_df.columns[col_idx]].append(room_adr.values)              # Add room ADR to parsed data dictionary

                    # Get total ADR of current row's room number
                    try:
                        total_adr = pdf_df[pdf_df.columns[col_idx]][row_idx+n_sub_rows-1]
                    except KeyError:
                        total_adr = sum(room_adr)
                    parsed_dict_batch[f"Total {pdf_df.columns[col_idx]}"].append(total_adr)         # Add total room ADR to parsed data dictionary
                    col_idx = (col_idx + 1) % len(pdf_df.columns)                                   # Increment pdf dataframe column index

        return (batch_number, parsed_dict_batch)



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
        room_numbers = []       # Initialize list of room numbers

        # Generate room numbers for each range
        for start, end in _global.PROPERTY_ROOMS:
            if end == -1:
                room_numbers.append(start)
            else:
                room_numbers.extend(range(start, end + 1))

        # Create excel dataframes for revenue and bookings per room
        self._excel_room_revenue_df = pd.DataFrame(index=room_numbers, columns=_global.MONTH_NAMES).astype("float32")
        self._excel_room_booking_df = pd.DataFrame(index=room_numbers, columns=_global.MONTH_NAMES).fillna(0).astype("int32")

        # Iterate over room numbers
        for room in room_numbers:
            # Get current room data table
            room_df = self._pdf_reports_df[self._pdf_reports_df[self._pdf_reports_df.columns.values[0]] == room]

            # Iterate over room data table rows
            for _, row in room_df.iterrows():

                # Iterate over room months
                room_months = [month for month in row["Month"]]
                for i, month in enumerate(room_months):
                    # Set room revenue and bookings per month
                    self._excel_room_revenue_df.at[row["Room No."], month] = row["Room Revenue"][i]
                    self._excel_room_booking_df.at[row["Room No."], month] = row["Room Nights"][i]

        # Calculate the yearly total revenue
        self._excel_room_revenue_df["Yearly Total"] = self._excel_room_revenue_df.sum(axis=1, skipna=True)
        self._excel_room_revenue_df["Yearly Total"] = self._excel_room_revenue_df["Yearly Total"].astype("float32")

        # Calculate the yearly total bookings
        self._excel_room_booking_df["Yearly Total"] = self._excel_room_booking_df.sum(axis=1, skipna=True)
        self._excel_room_booking_df["Yearly Total"] = self._excel_room_booking_df["Yearly Total"].fillna(0).astype("int32")

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
        self._output_df_table(name="pdfData", table=self._pdf_reports_df)               # Output pdf report data to excel file
        self._output_df_table(name="roomRevenue", table=self._excel_room_revenue_df)    # Output room revenue data to excel file
        self._output_df_table(name="roomBooking", table=self._excel_room_booking_df)    # Output room booking data to excel file
        return



    '''
    =========================================================================
    * _output_df_table()                                                    *
    =========================================================================
    * This function will output a single dataframe table as an excel file   *
    * to the /output directory.                                             *
    *                                                                       *
    *   INPUT:                                                              *
    *                name (str) - The name of the excel file.               *
    *         table (DataFrame) - The dataframe to write to excel file.     *
    *                                                                       *
    *   OUPUT:                                                              *
    *         None                                                          *
    =========================================================================
    '''
    def _output_df_table(self, name: str, table: pd.DataFrame) -> None:
        datetime_str = dt.datetime.today().strftime("%m_%d_%y_%H_%M_%S")        # Initialize datetime string
        output_file_path = f"{_global.OUTPUT_DIR}/{name}_{datetime_str}.xlsx"   # Set output file path for pdf data table 

        # Write the DataFrame to Excel with accounting formatting
        excel_writer = pd.ExcelWriter(output_file_path, engine='xlsxwriter')
        table.to_excel(excel_writer, sheet_name=name)

        workbook = excel_writer.book                                            # Get the excel workbook 
        worksheet = excel_writer.sheets[name]                                   # Get the excel worksheet

        # Apply accounting format to desired columns
        for col_num, col_name in enumerate(table.columns):
            col_format = workbook.add_format()                                  # Add formatting for excel column
            col_format.set_num_format(0)                                        # Initialize excel column formatting to generic

            # If column datatype is floating point
            if np.issubdtype(table[col_name].dtype, np.floating):
                col_format.set_num_format(44)                                   # Set excel formatting to accounting

            # Else if column datatype is 
            elif np.issubdtype(table[col_name].dtype, np.integer):
                col_format.set_num_format(1)                                    # Set excel formatting to numeric
            
            col_width = max(table[col_name].astype(str).map(len).max(), len(col_name))
            worksheet.set_column(col_num+1, col_num+1, col_width + 5, col_format)
        
        # Save the Excel file
        excel_writer.close()

        return