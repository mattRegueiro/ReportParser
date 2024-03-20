# ReportParser

This script is designed to extract table data from pdf files and organize the data into individual excel files.

## Description

The ReportParser program is designed to extract table data from pdf files and organize the extracted data into excel files. Before execution, the user will place all of their pdf files into the "/reports" directory. Using multithreading, ReportParser will process the pdf files from the "/reports" directory in batches. Each completed batch of pdf files will be consolidated into a dataframe associated with the year of the pdf report. The dataframes for each year processed will be used to generate pre-determined excel files.

## Installation/Quick Start

1. Clone the `ReportParser` github repository
2. Install dependencies using `requirements.txt`:
   ```
   
   pip install -r requirements.txt
   
   ```
3. Copy all pdf files to the `/reports` directory
4. Run `ReportParser` using __Python 3.10+__:
   ```
   
   python3 report_parser.py
   
   ```

