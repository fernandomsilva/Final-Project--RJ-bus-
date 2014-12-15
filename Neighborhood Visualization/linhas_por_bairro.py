import csv, json

max = 0
min = -1
top5 = [0, 0, 0, 0, 0]
top10 = [0,0,0,0,0,0,0,0,0,0]
less10 = [-1,-1,-1,-1,-1,-1,-1,-1,-1,-1]

mean = 0

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

		mean = mean + result[element[0]]
		
		for i in range(0, 5):
			if len(element)-1 > top5[i]:
				top5.insert(i, len(element) - 1)
				break

with open('Data/populacao.csv', 'rb') as csvfile:
	spamreader = csv.reader(csvfile)
	
	data_dict = []
	for row in spamreader:
		data_dict.append(row)

	data_dict = data_dict[1:]

	result2 = {}
	for element in data_dict:
		result2[element[0]] = int(element[2])


mean = 0

for key in result:
	result2[key] = result2[key]/result[key]
	mean = mean + result2[key]

	for i in range(0, 10):
		if result2[key] > top10[i]:
			top10.insert(i, result2[key])
			break

	for i in range(0, 10):
		if result2[key] < less10[i] or less10[i] < 0:
			less10.insert(i, result2[key])
			break

keys_delete = []
for key in result2.keys():
	if not key in result:
		keys_delete.append(key)

for key in keys_delete:
	del result2[key]
	
train_and_subway = {}
with open('Data/gtfs_supervia-todas_paradas.csv', 'rb') as csvfile:
	spamreader = csv.reader(csvfile)
	
	data_dict = []
	for row in spamreader:
		data_dict.append(row)

	data_dict = data_dict[1:]

	train_and_subway["train"] = []
	for element in data_dict:
		train_and_subway["train"].append((element[3], element[4]))

with open('Data/metro_paradas.csv', 'rb') as csvfile:
	spamreader = csv.reader(csvfile)
	
	data_dict = []
	for row in spamreader:
		data_dict.append(row)

	data_dict = data_dict[1:]

	train_and_subway["subway"] = []
	for element in data_dict:
		train_and_subway["subway"].append((element[3], element[4]))

js_file = open("bus_density.js", "w")
js_file.write("var density = " + json.dumps(result) + ";\nvar max_density = " + json.dumps(max) + ";\nvar min_density = " + json.dumps(min) + ";\nvar second_max_density = " + json.dumps(top5[1]) + ";\n")
js_file.write("var population_per_bus_lines = " + json.dumps(result2) + ";\nvar min_population_density = " + json.dumps(less10[0]) + ";\nvar max_population_density = " + json.dumps(top10[0]) + ";\nvar population_median = " + json.dumps((top10[0]+less10[0])/2) + ";\n")
js_file.write("var train_subway_points = " + json.dumps(train_and_subway) + ";\n")
js_file.close()	
