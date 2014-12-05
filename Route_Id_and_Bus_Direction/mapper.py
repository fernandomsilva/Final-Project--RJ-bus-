"""
Algorithm Description:

Step 1: 
Load the coordinates for all bus stops for a given line using the official <route-id, bus stop> mapping

Step 2:
For each bus_id in bus ids:
    For each 1-day-window/time frame:
        For each coordinate traversed by bus_id in that window/time frame:
            Assign the top-k closest bus stops (from the official <route-id, bus stop> mapping) to the coordinate. Add item <cordinate, route_id> to a list
            Ignore all distances greater than a certain limit  
        Group the coordinates that have the same route-id.
        The route-id that has the majority of coordinates is the route id for that bus in that 1-day window.
"""

import sys
import csv
import numpy as np
import os

TOP_CLOSEST_PER_COORD = 30
DIST_LIMIT = 1.0
stops_folder = '/home/rfn/finalproject/linhas/'
#stops_folder = 'C:/NYU/BigData/FinalProject/linhas/'
data_file_path = 'C:/NYU/BigData/FinalProject/data_all.csv'

def create_dir_stop_dic(dir_path):
    stops_dic = {} # the final dic format is <key:route_id, value: <key:direction, value:list of stops> >    
    stops_lst = []
    #print "considering only 100 routes! remove"
    #for file_name in [os.path.join(dp, f) for dp, dn, filenames in os.walk(dir_path) for f in filenames if os.path.splitext(f)[1] == '.csv'][:100]:
    for file_name in [os.path.join(dp, f) for dp, dn, filenames in os.walk(dir_path) for f in filenames if os.path.splitext(f)[1] == '.csv']:
    
        route_lst = list(csv.DictReader(open(file_name, 'rb'), delimiter=','))[1:]
    
        #construct the stops array for directions a and b separated          
        for stop_row in route_lst:
            if not stop_row['linha'] in stops_dic: #insert route_id key in case it does not exist
                stops_dic[stop_row['linha']] ={}
            if not stop_row['shape_id'] in stops_dic[stop_row['linha']]: #insert direction key in case it does not exist
                stops_dic[stop_row['linha']][stop_row['shape_id']] = []
            stops_dic[stop_row['linha']][stop_row['shape_id']].append([float(stop_row['latitude']),float(stop_row['longitude']),float(stop_row['sequencia'])])
        stops_lst.extend(route_lst) #merge two lists
    return stops_dic, stops_lst
        
#f = open(data_file_path,'rb')

#construct the stops array for directions a and b separated. It will be used in the bus direction calculation
stops_dic, stops_lst = create_dir_stop_dic(stops_folder)
#convert stop_rows to array:
stop_arr = np.zeros((len(stops_lst),3),dtype=np.float32)
for i, stop_row in enumerate(stops_lst):
    stop_arr[i,0] = stop_row['latitude'] 
    stop_arr[i,1] = stop_row['longitude']
    stop_arr[i,2] = stop_row['sequencia']

dic_closest_routes = {}
rows={}

for line in sys.stdin: #iterate in each time frame, but start from the last row of the outer loop
#for line in f: #iterate in each time frame, but start from the last row of the outer loop
    row_id, point_name, route_id, bus_id , hour, daymonth, month, year,speed,latitude,longitude,timestamp,desc = line.split(';') 
    key = str(bus_id) +'_' +str(hour)+'_' +str(daymonth)+'_'+str(month)+'_'+str(year)
    #if key != 'B10502_11_6_1_2014': #DEBUG
    #    continue
    if bus_id !='C13095':
        continue
    
    if key not in dic_closest_routes:
        dic_closest_routes[key] = {}
        rows[key] = []
    
    rows[key].append(line.strip()) #append the rows used in this key    
    coord = np.array([[float(latitude),float(longitude)]], np.float32) #convert coordinates to numpy array
    dist = np.sqrt(np.sum((coord-stop_arr[:,:2])**2,axis=1)) #calculate distance to the all stop coordinates
    sorted_dist = np.argsort(dist)[:TOP_CLOSEST_PER_COORD] #sort by distances and get the first top_closest indices
    sorted_dist = sorted_dist[dist[sorted_dist]<DIST_LIMIT] #ignore all distances greater than DIST_LIMIT
    
    for idx in sorted_dist:
        route_id = stops_lst[idx]['linha'] #get route id from the bus stop coordinate
        if not route_id in dic_closest_routes[key]:
            dic_closest_routes[key][route_id] = [] #case the route_id key does not exists, insert it
        #dic_closest_routes[key][route_id].append(coord) #append the coordinates visited to that route
        dic_closest_routes[key][route_id].append([latitude, longitude]) #append the coordinates visited to that route

#after iterating through all records...
for key, value in dic_closest_routes.items():
    print str(key)+"\t"+ repr([value, rows[key]]) #output the key value pairs
    