Comit Hydro web scraper

Logs into the NIWA COMIT Hydro webpage, fills out appropriate forms to
download daily storage and daily inflows.

Saves data in csv format for import into the EA Data Warehouse.

Kicks of a 7:05am each morning. 

Currently running on a linux server 

TODO: Currently daily data is downloaded for each catchment since 1926 every day.
      The process takes around 45 seconds to run and returns two ~4Mbyte files which 
      overwrite the previous days files.  We could just return yesterdays data and 
      append to the appropriate dataframe and save.  Tag this as a possible future improvement

D J Hume, 11 July, 2013
