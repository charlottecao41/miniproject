Jupyter Notebook(PredictionIndiction): 
Where our main analysis and results are. 
Some cells in the 'data pre-processing' section is not required to run, because those cells involves downloading csv to a googledrive, which may over-ride our cleaned files. 
Just loading the 2 files at the beginning of EDA will be faster.


App.py: 
This is our proposed recommender. 
Given a user ID, and a city name, we can find top X number of recommendations for this user. 
However, in this app, we did not use PCA, because that will require the whole business file. 
For some users, this may exceed their ram.
This app requires installation of pyathena. 
A set of userID and city that can be used to test is 'ZJ6sj1IjdwmPPL_ZxmRKgw', and 'Airdrie'.

Where our data and cleaned files is: 
https://drive.google.com/drive/folders/10qhF7-iNaenqXqm8hP7LQqIQMbsSaFGU?usp=sharing

Our file on googledrive, where every cell is ran: https://colab.research.google.com/drive/1ipK2EsMcqVbHUeKwiiqg2PnS1LL3LvSD?usp=sharing

