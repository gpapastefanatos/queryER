import pandas as pd
import numpy as np 
import sys, getopt, os



def initialize_args(argv):	
	inputdir = ""
	linksfile = ""
	outputfile = ""
	try:
		opts, args = getopt.getopt(argv,"hi:l:o:",["idir=","lfile=", "odir="])
	except getopt.GetoptError:
		print( 'converter.py -i <inputdir> -l <linksfile> -o <outputdir>')
		sys.exit(2)
	for opt, arg in opts:
		if opt == '-h':
				print( 'converter.py -i <inputdir> -l <linksfile> -o <outputdir>')
				sys.exit()
		elif opt in ("-i", "--idir"):
			inputdir = arg
		elif opt in ("-l", "--links"):
			linksfile = arg
		elif opt in ("-o", "--ofile"):
			outputdir = arg

	return (inputdir, linksfile, outputfile)



def main(argv):
	inputdir, linksfile, outputdir = initialize_args(argv)
	if(os.path.exists(outputdir)):
		os.remove(outputdir)

	_, _, filenames = next(os.walk(inputdir))
	for inputfile in filenames:
		if("paper" in inputfile): continue
		df = pd.read_csv(inputdir + "/" + inputfile, sep='\t')[["id", "orig_id"]]
		
		df_links = pd.read_csv(linksfile, sep='\t')
		df_links["id_s"] = df_links["id_s"].astype(str)
		df_links["id_d"] = df_links["id_d"].astype(str)
		dictionary = df.set_index('orig_id').to_dict()['id']
		#df_links = df_links.loc[(df_links['id_d'].isin(df["orig_id"])) & (df_links['id_s'].isin(df["orig_id"]))]
		id_set = list(df["orig_id"])
		df_links = df_links.loc[lambda row : row['id_d'].isin(id_set) & row['id_s'].isin(id_set)]
		df_links = df_links.replace({"id_d": dictionary})
		df_links = df_links.replace({"id_s": dictionary})
		df_links.to_csv(outputdir + "/ground_truth_" + inputfile, sep='\t')

if __name__ == "__main__":
   main(sys.argv[1:])	