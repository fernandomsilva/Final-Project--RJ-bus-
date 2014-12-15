import csv

def pointIsInNeighborhood(point, polygon_point_list):
	c = False
	j = len(polygon_point_list) - 1
	for i in range(0, len(polygon_point_list)):
		edge_p1 = (float(polygon_point_list[j]['longitude']), float(polygon_point_list[j]['latitude']))
		edge_p2 = (float(polygon_point_list[i]['longitude']), float(polygon_point_list[i]['latitude']))
		
		if ((edge_p2[1] > point[1]) != (edge_p1[1] > point[1])):
			if (point[0] < (edge_p1[0]-edge_p2[0]) * (point[1]-edge_p2[1]) / (edge_p1[1]-edge_p2[1]) + edge_p2[0]):
				c = not c
		
		j = i

	return c

with open('Data/Tijuca.csv', 'rb') as csvfile:
	spamreader = csv.DictReader(csvfile)

	data_dict = []
	for row in spamreader:
		data_dict.append(row)
		
	point = (-43.231421, -22.909333)
	print 'Maracana: ', pointIsInNeighboorhood(point,data_dict)
	point = (-43.232509, -22.924299)
	print 'Praca Saens Pena: ', pointIsInNeighborhood(point,data_dict)