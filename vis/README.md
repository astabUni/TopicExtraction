# Installation

## Prerequisites

* Pandas
* MatplotLib
* graphviz

## Required files

Generated files from dataRetrieval:
categorylinks.json and category_to_depth.json, placed in:
> TopicExtraction\topicExtraction\src\main\resources\in

Output from topicExtraction:
> TopicExtraction\topicExtraction\src\main\resources\out\nouns\

and

> TopicExtraction\topicExtraction\src\main\resources\out\lda\

## Usage

Navigate to topicExtraction

> cd vis

Run plots.py

> python plots.py

Output: noun-plots/

Run graph.py

> python graph.py

Output: data.gv and data.gv.png
