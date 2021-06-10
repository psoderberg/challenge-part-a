
This program processes avro, csv, and json files of the same schema.  It combines and deduplicates one or more files and returns some calculations as well as the final dataset.  It was written in Python using pandas and fastavro then compiled with pyinstaller.
	
Usage: Download the executable and run the file_proc with the filenames as arguments.  Ex:
  
  `file_proc CityList.csv CityListA.json CityListB.avro`
 
Remember to chmod+x and allow unidentified developers to run on a mac. It takes a few seconds to show progress.

Limitations:
  
This package will not work well on larger datasets that don't fit in memory and will not work on data that doesn't fit on disk.  If larger datasets are required, we would decouple the storage from processing and store chunks of the datasets across a distributed cluster or cloud object storage.  The processing would then be divided among workers sharing their intermediate results from subsets of the data.  A distributed OLAP solution like Druid would work marvelously.   

The solution already benefits from using the fastavro library to speed processing of avro files.  Other improvements could be found by increasing parallelization and streaming of ingest and by doing the heavy lifting in C using something like Cython. It could be made more efficient by skipping duplicate rows when ingesting and creating the output dataset rather than combining everything and then deduping. Also, only use 'avro', 'csv', or 'json' in the filenames' extensions.
		

Results
1. What is the count of all rows? 2083
2. What is the city with the largest population? Mumbai (Bombay)
3. What is the total population of all cities in Brazil (CountryCode == BRA)? 55955012
