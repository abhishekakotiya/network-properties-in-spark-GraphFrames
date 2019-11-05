# sudo pip install powerlaw
# csv files should be in the same folder as this script

import powerlaw
import pandas as pd

files = ['amazon.graph.large.csv', 'amazon.graph.small.csv', 'dblp.graph.large.csv', 'dblp.graph.small.csv', 'youtube.graph.large.csv', 'youtube.graph.small.csv', 'gnm1.csv', 'gnm2.csv', 'gnp1.csv', 'gnp2.csv']

for f in files:
	df = pd.read_csv(f)	
	count = df['count']
	power_alpha = powerlaw.Fit(count).power_law.alpha
	print 'alpha value for ' + f + ' : ' + str(power_alpha)
