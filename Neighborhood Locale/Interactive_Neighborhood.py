import csv, json

with open('Data/linhas_por_bairro.csv', 'rb') as csvfile:
	spamreader = csv.reader(csvfile)
	
	data_dict = []
	for row in spamreader:
		data_dict.append(row)

	result = {}
	for element in data_dict:
		result[element[0]] = element[1:]

js_file = open("bus_lines_per_neighborhood.js", "w")
js_file.write("var lines = " + json.dumps(result) + ";\n")
js_file.close()	
