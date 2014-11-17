import csv, json

def search(data, field, value):
	result = []
	
	for element in data:
		if element[field] == value:
			result.append(element)
	
	return result
	
def convertToMap(data, fieldx, fieldy):
	result = {}
	i = 0
	for element in data:
		result[str(i)] = (float(element[fieldx]), float(element[fieldy]), 0.15)
		
		i = i + 1

	return result

def max_min_mean(data, fieldx, fieldy):
	max_x = ''
	max_y = ''
	min_x = ''
	min_y = ''
	mean_x = 0.0
	mean_y = 0.0

	for element in data:
		value = float(element[fieldx])
		mean_x = mean_x + value
		if value > max_x or max_x == '':
			max_x = value
		if value < min_x or min_x == '':
			min_x = value
			
		value = float(element[fieldy])
		mean_y = mean_y + value
		if value > max_y or max_y == '':
			max_y = value
		if value < min_y or min_y == '':
			min_y = value
	
	mean_x = mean_x / len(data)
	mean_y = mean_y / len(data)
	
	return (min_x, max_x, min_y, max_y, mean_x, mean_y)

with open('Data/onibus_IBM0150.csv', 'rb') as csvfile:
	spamreader = csv.DictReader(csvfile)
	
	data_dict = []
	for row in spamreader:
		data_dict.append(row)

	linha422 = search(data_dict, "Linha", '422')
	
	map_points = convertToMap(linha422, "LatitudePonto", "LongitudePonto")
	bounds = max_min_mean(linha422, "LatitudePonto", "LongitudePonto")
	mean = (bounds[4], bounds[5])
	
	js_file = open("points.js", "w")
	js_file.write("var map_center = " + json.dumps(mean) + ";\nvar points = " + json.dumps(map_points) + ";")
	js_file.close()
	

	
