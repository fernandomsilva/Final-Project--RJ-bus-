import csv, json

max = 0
min = -1

with open('Data/linhas_por_bairro.csv', 'rb') as csvfile:
	spamreader = csv.reader(csvfile)
	
	data_dict = []
	for row in spamreader:
		data_dict.append(row)

	result = {}
	for element in data_dict:
		if (len(element)-1 > max):
			max = len(element)-1
		if (len(element)-1 < min or min == -1):
			min = len(element)-1
		result[element[0]] = len(element) - 1

	js_file = open("bus_density.js", "w")
	js_file.write("var density = " + json.dumps(result) + ";\nvar max_density = " + json.dumps(max) + ";\nvar min_density = " + json.dumps(min) + ";")
	js_file.close()	
	#linha422 = search(data_dict, "Linha", '422')
	
#	map_points = convertToMap(data_dict, "latitude", "longitude")
#	bounds = max_min_mean(data_dict, "latitude", "longitude")
#	mean = (bounds[4], bounds[5])
	
#	js_file = open("new_points.js", "w")
#	js_file.write("var map_center = " + json.dumps(mean) + ";\nvar points = " + json.dumps(map_points) + ";")
#	js_file.close()
