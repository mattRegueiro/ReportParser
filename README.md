# ReportParser

This script is designed to extract table data from pdf files and organize the data into individual excel files.

## Description

The ReportParser program is designed to extract table data from pdf files and organize the extracted data into excel files. Before execution, the user will place all of their pdf files into the "/reports" directory. Using multithreading, ReportParser will process the pdf files from the "/reports" directory in batches. Each completed batch of pdf files will be consolidated into a dataframe associated with the year of the pdf report. The dataframes for each year processed will be used to generate pre-determined excel files.

## Get ReportParser Working
### (Windows 10)
1. If you don’t have it already, install [Java](https://www.java.com/en/download/manual.jsp)
2. Open a new terminal window and type `java`. If the command line says "'java' is not recognized as an internal or external command, operable program or batch file", you need to set your `PATH` environment variable to point to the Java directory.
3. If step #2 gave you an error, find the main Java folder in either **C:\Program Files\Java** or **C:\Program Files x86\Java**
4. Go to `Control Panel -> System and Security -> System -> Advanced System Settings -> Environment Variables -> Select PATH –> Edit`
5. Add the Java bin folder to your `PATH` environment variable (it's located within **C:\Program Files\Java** or **C:\Program Files x86\Java**) and hit OK
6. Exit the `Environment Variables` window when you see the Java Bin folder in the `PATH` variable
7. Open a new terminal window and type `java`. The command line should now print a list of options and ReportParser should run.

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

