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

import csv
import numpy as np
import os

DEBUG=1
TOP_CLOSEST_PER_COORD = 30
TOP_CLOSEST_PER_FRAME = 5
DIST_LIMIT = 1.0
out_file_path = 'C:/NYU/BigData/FinalProject/data_all_predict.csv'
data_file_path = 'C:/NYU/BigData/FinalProject/data_all.csv'
stops_folder = 'C:/NYU/BigData/FinalProject/linhas/'

def get_majority_route(dic_routes):
    vals = dic_routes.values()
    arr = np.zeros((len(vals)),dtype=np.int32)
    #convert the list of coordinates to an array that contains the length of the lists only
    for i,coords_lst in enumerate(vals):
        arr[i] = len(coords_lst)
        
    idx = np.argsort(arr)[-TOP_CLOSEST_PER_FRAME:]
    items = list(dic_routes.items())
    ordered_items=[]
    for i in idx[::-1]:
        ordered_items.append(items[i])
    return ordered_items

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

def add_to_file(data_rows, row_pointers, directions, file_path):
    with open(file_path, 'ab') as fp:
        
        csvfile = csv.writer(fp, delimiter=',')
        lst_data = []
        
        for row in row_pointers:
            data = data_rows[row]
            if len(directions)>0:
                data['Linha Predicao'] = directions[0].split("_")[0] #get only the first route_id
                data['Direcao'] = directions[0].split("_")[1] #get only the direction of the first route_id
            else:
                data['Linha Predicao'] = 'no_prediction'
                data['Direcao'] = 'no_prediction'
            lst_data.append(data.values())
        csvfile.writerows(lst_data)

def infer_bus_directions(lst):
    output_list =[]
    for item in lst:
        if len(stops_dic[item[0]].keys())==1:
            output_list.append(item[0]+'_circular')
        else:
            #get the stops array for directions a and b separated
            dir_a_name = stops_dic[item[0]].keys()[0] #get the first key as direction A
            dir_b_name = stops_dic[item[0]].keys()[1]
            stop_arr_a = np.asarray(stops_dic[item[0]][dir_a_name]) 
            stop_arr_b = np.asarray(stops_dic[item[0]][dir_b_name])
            
            seq_a = []
            seq_b = []
            for coord in item[1]:
                #get the closest stop in a to coord
                dist = np.sqrt(np.sum((coord-stop_arr_a[:,:2])**2,axis=1)) #calculate distance to the all stop coordinates
                idx = np.argsort(dist)[0] #sort by distances and get the first index
                seq_a.append(stop_arr_a[idx,2]) #append the sequence
                
                #get the closest stop in b to coord
                dist = np.sqrt(np.sum((coord-stop_arr_b[:,:2])**2,axis=1)) #calculate distance to the all stop coordinates
                idx = np.argsort(dist)[0] #sort by distances and get the first index
                seq_b.append(stop_arr_b[idx,2]) #append the sequence
                
            seq_a = np.asarray(seq_a)
            seq_b = np.asarray(seq_b)
            ma = np.polyfit(np.arange(len(seq_a)), seq_a, deg=1) #fit a line for seq a
            mb = np.polyfit(np.arange(len(seq_b)), seq_b, deg=1) #fit a line for seq b
            if ma[0]>mb[0]: #if equation a have a greater angle than equation b, the bus goes in direction a
                output_list.append(item[0]+'_'+dir_a_name)
            else:
                output_list.append(item[0]+'_'+dir_b_name)
    return output_list
        
from datetime import datetime
tstart = datetime.now()

print 'loading data...' 

data_rows = list(csv.DictReader(open(data_file_path, 'rb'), delimiter=';'))[1:]

print 'loaded... time to bus data:', datetime.now()-tstart

tbusstops = datetime.now()

#construct the stops array for directions a and b separated. It will be used in the bus direction calculation
stops_dic, stops_lst = create_dir_stop_dic(stops_folder)
#convert stop_rows to array:
stop_arr = np.zeros((len(stops_lst),3),dtype=np.float32)
for i, stop_row in enumerate(stops_lst):
    stop_arr[i,0] = stop_row['latitude'] 
    stop_arr[i,1] = stop_row['longitude']
    stop_arr[i,2] = stop_row['sequencia']

print 'loaded... time to load bus stops definition file:', datetime.now()-tbusstops
print 'predicting...'

#create output csv file with header
with open(out_file_path, 'wb') as fp:
    fp.write('Hora,Velocidade,DescricaoPonto,DiaMes,LinhaPredicao,TimeStamp,Ano,Onibus,Linha,NomePonto,Direcao,LatitudePonto,Mes,LongitudePonto,ID\n') #write header

dic_closest_routes = {}
row_pointers={}
data_rows=data_rows[:10000] #uncomment to get a smaller chunck for debugging
for j, time_row in enumerate(data_rows): #iterate in each time frame, but start from the last row of the outer loop 
    if DEBUG>=1:
        print 'step 1, iteration', j, 'of', len(data_rows)
        t=datetime.now()
    key = str(time_row['Onibus']) +'_' +str(time_row['Hora'])+'_' +str(time_row['DiaMes'])+'_'+str(time_row['Mes'])+'_'+str(time_row['Ano'])
    #if key != 'B10502_11_6_1_2014': #DEBUG
    #    continue
    #if time_row['Onibus'] !='C13095':
    #    continue
    
    if key not in dic_closest_routes:
        dic_closest_routes[key] = {}
        row_pointers[key] = []
    
    row_pointers[key].append(j) #append the rows used in this key    
    coord = np.array([[float(time_row['LatitudePonto']),float(time_row['LongitudePonto'])]], np.float32) #convert coordinates to numpy array
    if DEBUG>=2:
        print 'others', datetime.now()-t
        t=datetime.now()
    dist = np.sqrt(np.sum((coord-stop_arr[:,:2])**2,axis=1)) #calculate distance to the all stop coordinates
    if DEBUG>=2:
        print 'calc dist', datetime.now()-t
        t=datetime.now()
    sorted_dist = np.argsort(dist)[:TOP_CLOSEST_PER_COORD] #sort by distances and get the first top_closest indices
    if DEBUG>=2:
        print 'arg sort', datetime.now()-t
        t=datetime.now()
    sorted_dist = sorted_dist[dist[sorted_dist]<DIST_LIMIT] #ignore all distances greater than DIST_LIMIT
    if DEBUG>=2:
        print 'dist limit', datetime.now()-t
        t=datetime.now()
    for idx in sorted_dist:
        route_id = stops_lst[idx]['linha'] #get route id from the bus stop coordinate
        if not route_id in  dic_closest_routes[key]:
            dic_closest_routes[key][route_id] = [] #case the route_id key does not exists, insert it
        dic_closest_routes[key][route_id].append(coord) #append the coordinates visited to that route
    if DEBUG>=2:
        print 'add to dic', datetime.now()-t
#after iterating through all records, get the closest routes and directions for each key of type "bus_id + time_frame"
i =0
total_iter = len(dic_closest_routes.items())
for key, value in dic_closest_routes.items():
    i=i+1
    if DEBUG>=1:
        print 'step 2, iteration', i, 'of', total_iter
    closest_routes = get_majority_route(value)
    route_ids_directions = infer_bus_directions(closest_routes)
    add_to_file(data_rows, row_pointers[key], route_ids_directions, out_file_path)
    
#print the results
#print '<busId_day_month_year, list(route_id + direction) >'
#for item in bus_time_cleaned.items():
#    print item             

print 'total time', datetime.now()-tstart
print 'finished'