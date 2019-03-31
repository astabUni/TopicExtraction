# Installation

lxml:
> pip install lxml
	
bz2file:
> pip install bz2file


# Needed files:

The join between categorylinks and page, dumped into a .csv file:
> SELECT * FROM page JOIN categorylinks ON categorylinks.cl_from = page.page_id INTO OUTFILE 'E:/wikidumps/categorylinkspage-join.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n'

A [wikimedia article dump](https://dumps.wikimedia.org/backup-index.html), e.g. enwiki-20190101-pages-articles-multistream.xml


# Usage 

Navigate to dataRetrievel:

> cd dataRetrieval

Place the .csv dump and the unzipped article dump in the /resources folder

Configure the following variables in dataRetrieval.py:

* FILENAME_CSV: your categorylinks + page .csv dump
* FILENAME_ARTICLES_XML: the wikimedia article dump (unzipped, as .xml file)
* WANTEDCATEGORY: the starting category for which you want to extract articles from
* MAXDEPTH: the maximum depth of subcategories you want to extract articles from

Run dataRetrieval.py

> python dataRetrieval.py

This may take a while. During the first execution the .csv file is processed and the most relevant information is extracted.
The results are saved in /resources and reused in subsequent executions of dataRetrieval.py as long as the files are not deleted or FILENAME_CSV is changed.

Afterwards run WikiExtractor with the output from dataRetrieval.py as input (choose SIZE bigger than the input file to receive a single output file):

> python wikiextractor\WikiExtractor.py output\[FILENAME] -o [OUTPUTPATH] -b [SIZE] --json

For example:
> python wikiextractor\WikiExtractor.py output\Computer_hardware-d5\Computer_hardware_articles-d5.xml.bz2 -o output/Computer_hardware-d5 -b 5G --json

# To continue with step 2 (topicExtraction) move the generated files from dataRetrieval/output to TopicExtraction\topicExtraction\src\main\resources\in

Hint:
Extracting articles from the full wikimedia article dump takes a while. If you want to extract articles for the same category but limited to a smaller MAXDEPTH faster it is possible
to reuse the unzipped output as input. For example you can extract "Computer_hardware" for MAXDEPTH=10, unzip /output/Computer_hardware-d10/Computer_hardware_articles-d10.xml.bz2,
move Computer_hardware_articles-d10.xml to /resources, set FILENAME_ARTICLES_XML = "Computer_hardware_articles-d10.xml" and extract the articles for "Computer_hardware" and MAXDEPTH=5.
