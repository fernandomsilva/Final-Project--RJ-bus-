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
import pickle

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
    for file_name in [os.path.join(dp, f) for dp, dn, filenames in os.walk(dir_path) for f in filenames if os.path.splitext(f)[1] == '.csv']:
        route_lst = list(csv.DictReader(open(file_name, 'rb'), delimiter=','))[1:]
    
        #construct the stops array for directions a and b separated          
        for stop_row in route_lst:
            if not stop_row['linha'] in stops_dic: #insert route_id key in case it does not exist
                stops_dic[stop_row['linha']] ={}
            if not stop_row['shape_id'] in stops_dic[stop_row['linha']]: #insert direction key in case it does not exist
                stops_dic[stop_row['linha']][stop_row['shape_id']] = []
            stops_dic[stop_row['linha']][stop_row['shape_id']].append([float(stop_row['latitude']),float(stop_row['longitude']),float(stop_row['sequencia'])])
            #stops_lst.append(stop_row)
        stops_lst.extend(route_lst) #merge two lists
    return stops_dic, stops_lst

"""
def infer_bus_stops_directions(stop_rows):
    #construct the stops array for directions a and b separated
    #CORRECT THIS: assuming that the file is sorted by sequence. It should order the file before executing the algorithm
    stops_dic = {} # the final dic format is <key:route_id, value: <key:direction, value:list of stops> >  
    for stop_row in stop_rows:
        if stop_row['linha'] != '627':
            continue
        curr_bus_stop = np.array([float(stop_row['latitude']),float(stop_row['longitude']),float(stop_row['sequencia'])])
        if not stop_row['linha'] in stops_dic: #insert route_id key in case it does not exist
            stops_dic[stop_row['linha']] ={}
            
        if not 'a' in stops_dic[stop_row['linha']]: 
            stops_dic[stop_row['linha']]['a'] = [curr_bus_stop]
        elif not 'b' in stops_dic[stop_row['linha']]:
            stops_dic[stop_row['linha']]['b'] = [curr_bus_stop]
        else:
            
            #check the distance of the current bus stop to all the bus stop in each direction. The inferred direction will be the one that has majority of lowest distances
            stop_arr_a = np.asarray(stops_dic[stop_row['linha']]['a'])
            #stop_arr_b = np.asarray(stops_dic[stop_row['linha']]['b'])
            dist_a = np.sqrt(np.sum((curr_bus_stop[:2].reshape((1,2))-stop_arr_a[:,:2])**2,axis=1)) #calculate distance to the all stop coordinates
            #dist_b = np.sqrt(np.sum((curr_bus_stop[:2].reshape((1,2))-stop_arr_b[:,:2])**2,axis=1)) #calculate distance to the all stop coordinates
            
            avg_dist_a = dist_a.sum()/len(dist_a)
            if len(stop_arr_a) ==1:
                last_array = stop_arr_a[0,:2]
            else:
                last_array = stop_arr_a[:-1,:2]
            last_dist_a = np.sqrt(np.sum((stop_arr_a[-1,:2].reshape((1,2))-last_array)**2,axis=1)) #calculate distance to the all stop coordinates
            avg_last_dist_a = last_dist_a.sum()/len(last_dist_a)
            
            if avg_dist_a-avg_last_dist_a>0:
                stops_dic[stop_row['linha']]['a'].append(curr_bus_stop)
            else:
                stops_dic[stop_row['linha']]['b'].append(curr_bus_stop)
    
    myfile = open('c:/temp/627.csv', 'wb')
    wr = csv.writer(myfile)
    lst = []
    for row in stops_dic['627']['a']:
        lst.append(row.tolist())
    wr.writerows(lst)
    myfile.close()
    return stops_dic
"""

def infer_bus_directions(lst):
    output_list =[]
    for item in lst:
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

TOP_CLOSEST_PER_COORD = 30
TOP_CLOSEST_PER_FRAME = 5
DIST_LIMIT = 1.0

print 'loading data...' 
data_file_path = 'C:/NYU/BigData/FinalProject/data_all.csv'
data_rows = list(csv.DictReader(open(data_file_path, 'rb'), delimiter=';'))[1:]

stops_file_dir_path = 'C:/NYU/BigData/FinalProject/linhas/'
stops_file=stops_file_dir_path+'dic_lst_stops.pkl'

create_and_save = False
if create_and_save:
    #construct the stops array for directions a and b separated. It will be used in the bus direction calculation
    stops_dic, stops_lst = create_dir_stop_dic(stops_file_dir_path)
    #convert stop_rows to array:
    #CONVERT IT USING A BUILD-IN METHOD!!!
    stop_arr = np.zeros((len(stops_lst),3),dtype=np.float32)
    for i, stop_row in enumerate(stops_lst):
        stop_arr[i,0] = stop_row['latitude'] 
        stop_arr[i,1] = stop_row['longitude']
        stop_arr[i,2] = stop_row['sequencia']

    pickle.dump((stops_dic, stops_lst, stop_arr),open(stops_file,'wb'))
else:
    stops_dic, stops_lst, stop_arr = pickle.load(open(stops_file,'r'))

print 'loaded'
print 'predicting...'

bus_time_cleaned = {} #store the <bus_id, hour> that were already cleaned
for k, bus_row in enumerate(data_rows): #iterate in each bus id:
    #print 'iteration', k, 'of', len(data_rows)    
    current_key = None
    dic_closest_routes = {}
    if bus_row['Onibus'] !='B10502': #DEBUG
        continue
    for j, time_row in enumerate(data_rows[k:]): #iterate in each time frame, but start from the last row of the outer loop 
        if time_row['Onibus'] == bus_row['Onibus']: #if it is the same bus id           
            key = str(bus_row['Onibus']) +'_' +str(time_row['Hora'])+'_' +str(time_row['DiaMes'])+'_'+str(time_row['Mes'])+'_'+str(time_row['Ano'])
            #key = str(bus_row['Onibus']) +'_' +str(time_row['DiaMes'])+'_'+str(time_row['Mes'])+'_'+str(time_row['Ano'])
            
            #if key != 'B10502_11_6_1_2014': #DEBUG
            #    continue
            
            if not key in bus_time_cleaned and current_key==None:
                current_key = key
            
            #For optimization purposes only: break the loop if the time stamp is superior from the current time stamp.
            if current_key !=None:
                date = current_key.split('_')
                if int(date[-1])>int(time_row['Ano']):
                    break
                elif int(date[-2])>int(time_row['Mes']):
                    break
                elif int(date[-3])>int(time_row['DiaMes']):
                    break
            
            if current_key == key:
                coord = np.array([[float(time_row['LatitudePonto']),float(time_row['LongitudePonto'])]], np.float32) #convert coordinates to numpy array
                dist = np.sqrt(np.sum((coord-stop_arr[:,:2])**2,axis=1)) #calculate distance to the all stop coordinates
                sorted_dist = np.argsort(dist)[:TOP_CLOSEST_PER_COORD] #sort by distances and get the first top_closest indices
                sorted_dist = sorted_dist[dist[sorted_dist]<DIST_LIMIT] #ignore all distances greater than DIST_LIMIT 
                for idx in sorted_dist:
                    route_id = stops_lst[idx]['linha'] #get route id from the bus stop coordinate
                    if not route_id in  dic_closest_routes:
                        dic_closest_routes[route_id] = [] #case the route_id key does not exists, insert it
                    dic_closest_routes[route_id].append(coord) #append the coordinates visited to that route
    
    #after iterating through all time frames, add the key of bus_id+time to the dic to indicate that this time frame is clean
    closest_routes = get_majority_route(dic_closest_routes)
    route_ids_directions = infer_bus_directions(closest_routes)
    bus_time_cleaned[current_key] = route_ids_directions
    
#print the results
print '<busId_day_month_year, list(route_id + direction) >'
for item in bus_time_cleaned.items():   
    print item             
    
print 'total time', datetime.now()-tstart
print 'finished'