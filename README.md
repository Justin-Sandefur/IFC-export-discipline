The repository contains replication files for the following post on 'chatGDP' by Justin Sandefur: https://www.chat-gdp.org/development-finance-needs-export-discipline/

There are two pieces of python code: both scrape the IFC project disclosures pages. 
The first script attempts to correct some clearly misleading dollar figures in the IFC projects database, mostly cases where default values of $1 billion are left instead of actual values, sometimes mentioned in the text.
The second looks for references to exporting among manufacturing projects.

The Stata do-files mostly clean and reshape the data and export CSV files, which are then used to make graphs in Flourish.
