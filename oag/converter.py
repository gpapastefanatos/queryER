import sys, getopt, os
import json
import codecs
import csv


def initialize_args(argv):	
	inputdir = ""
	outputfile = ""
	count = 0
	try:
		opts, args = getopt.getopt(argv,"hi:o:c:",["idir=","ofile=", "count="])
	except getopt.GetoptError:
		print( 'converter.py -i <inputdir> -o <outputfile> -c <count>')
		sys.exit(2)
	for opt, arg in opts:
		if opt == '-h':
			print('converter.py -i <inputdir> -o <outputfile> -c <count>')
			sys.exit()
		elif opt in ("-i", "--idir"):
			inputdir = arg
		elif opt in ("-o", "--ofile"):
			outputfile = arg
		elif opt in ("-c", "--count"):
			count = int(arg)
	return (inputdir, outputfile, count)

def convert_authors(inputdir, outputfile, count, cols):
	if(os.path.exists(outputfile)):
		os.remove(outputfile)
	with codecs.open(outputfile.replace(".csv", "") + "_links.csv", 'a', 'utf-8-sig') as o_links:
		csv_links_writer = csv.writer(o_links, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
		csv_links_writer.writerow(["id", "orig_id"])		
		with codecs.open(outputfile, 'a', 'utf-8-sig') as o:
			csv_writer = csv.writer(o, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
			header = []
			for col in cols:
				header.append(col)
			csv_writer.writerow(header) # write header
			_, _, filenames = next(os.walk(inputdir))
			os.chdir(inputdir)
			id_no = 0
			for inputfile in filenames:
				element = 0
				with open(inputfile, 'r') as f:
					for line in f:
						row = []
						if(element == count): break 
						obj = json.loads(line)
						for col in cols:
							try:
								if(col in "tags"):
									tag = str(obj[col][0]["t"].replace("\t", " ").replace("\n", " ").replace("\r", " "))
									row.append(tag)
								elif(col == "id"):
									row.append(id_no)
									csv_links_writer.writerow([id_no, obj["id"]])
								elif(col == "org"):
									row.append(obj["orgs"][0])
								else:
									row.append(str(obj[col]).replace("\t", " ").replace("\n", " ").replace("\r", " "))
							except:
								row.append(None)
						csv_writer.writerow(row)
						element+=1
						id_no += 1

def convert_papers(inputdir, outputfile, count, cols):
	if(os.path.exists(outputfile)):
		os.remove(outputfile)

	with codecs.open(outputfile.replace(".csv", "") + "_links.csv", 'a', 'utf-8-sig') as o_links:
		csv_links_writer = csv.writer(o_links, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
		csv_links_writer.writerow(["id", "orig_id"])
		with codecs.open(outputfile, 'a', 'utf-8-sig') as o:
			csv_writer = csv.writer(o, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
			header = []
			for col in cols:
				header.append(col)
			csv_writer.writerow(header) # write header

			_, _, filenames = next(os.walk(inputdir))
			os.chdir(inputdir)
			id_no = 0
			for inputfile in filenames:
				element = 0; #give new id to elements 
				with open(inputfile, 'r') as f:
					for line in f:
						if(id_no >= count): break
						obj = json.loads(line)
						for author in obj["authors"]: # for each author create a new element
							row = []
							for col in cols:
								try:
									if(col in "keywords"):
										key = str(obj[col][0]["k"].replace("\t", " ").replace("\n", " ").replace("\r", " "))
										row.append(key)
									elif(col == "id"):
										row.append(id_no)
										csv_links_writer.writerow([id_no, obj["id"]])
									elif(col == "author_name"):
										row.append(author["name"].replace("\t", " ").replace("\n", " ").replace("\r", " "))
									elif(col == "author_id"):
										row.append(author["id"])
									elif(col == "venue_name"):
										row.append(obj["venue"]["raw"].replace("\t", " ").replace("\n", " ").replace("\r", " "))
									else:
										row.append(str(obj[col]).replace("\t", " ").replace("\n", " ").replace("\r", " "))
								except:
									row.append(None)

							element += 1
							id_no += 1
							csv_writer.writerow(row)

def convert_file(inputdir, outputfile, count):
	if("author" in inputdir):
		cols = ["id", "name", "org",  "position", "n_pubs", "n_citation", "tags"]
		convert_authors(inputdir, outputfile, count, cols)
	elif("paper" in inputdir):
		cols = ["id",  "title",  "author_name", "author_id", "venue_name", "year", "keywords", "n_citation",  "doc_type", "lang", "publisher", "volume", "issue", "issn", "isbn", "doi", "pdf", "page_start", "page_end", "abstract"]
		convert_papers(inputdir, outputfile, count, cols)
	    
def main(argv):
	inputfile, outputfile, count = initialize_args(argv)
	convert_file(inputfile, outputfile, count)










if __name__ == "__main__":
   main(sys.argv[1:])