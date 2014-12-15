import csv
import numpy as np
import os
import dateutil.parser
from datetime import datetime

DEBUG=1
TOP_CLOSEST_PER_COORD = 100
INFER = 'direction' #possible values are 'direction' or 'route'
#INFER = 'route' #possible values are 'direction' or 'route'
out_file_path = 'C:/NYU/BigData/FinalProject/data_all_predict.csv'
data_file_path = 'C:/NYU/BigData/FinalProject/data_all.csv'
stops_folder = 'C:/NYU/BigData/FinalProject/linhas/'

def get_majority_route(dic_routes):
    vals = dic_routes.values()
    
    arr_len = np.zeros((len(vals)),dtype=np.float32)
    arr_dist = np.zeros((len(vals)),dtype=np.float32)
    #convert the list of coordinates to an array that is the number of coordinates
    for i,coords_dist_lst in enumerate(vals):
        arr_len[i] = len(coords_dist_lst)
        sum_dist=0
        for coord_dist in coords_dist_lst:
            sum_dist = sum_dist+coord_dist[1]
        arr_dist[i] = sum_dist    
    
    arr_avg = arr_dist/arr_len
    max_len = arr_len.max()
    crit_idx = arr_len<0.7*max_len
    arr_avg[crit_idx] = np.inf*arr_avg[crit_idx]
    return dic_routes.keys()[arr_avg.argmin()]

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

def add_to_file(data_rows, row_pointers, file_path, route_id = None, direction=None):
    with open(file_path, 'ab') as fp:
        
        csvfile = csv.writer(fp, delimiter=',')
        lst_data = []
        
        for row in row_pointers:
            data = data_rows[row]
            if route_id!=None:
                data['Linha Predicao'] = route_id #get only the first route_id
            if direction!=None:
                data['Direcao'] = direction #get only the first direction
            
            lst_data.append(data.values())
        csvfile.writerows(lst_data)

def infer_bus_directions(dic):
    for key,value in dic.items():
        if len(stops_dic[key].keys())!=2:
            return 'circular'
        else:
            #get the stops array for directions a and b separated
            dir_a_name = stops_dic[key].keys()[0] #get the first key as direction A
            dir_b_name = stops_dic[key].keys()[1]
            stop_arr_a = np.asarray(stops_dic[key][dir_a_name]) 
            stop_arr_b = np.asarray(stops_dic[key][dir_b_name])
            
            seq_a = []
            seq_b = []
            timestamps = []
            for coord_dist in value:
                
                timestamps.append(coord_dist[2])   
                coord = coord_dist[0]
                #get the closest stop in a to coord
                dist = np.sqrt(np.sum((coord-stop_arr_a[:,:2])**2,axis=1)) #calculate distance to the all stop coordinates
                idx = np.argmin(dist) #get the min distance
                seq_a.append(stop_arr_a[idx,2]) #append the sequence
                
                #get the closest stop in b to coord
                dist = np.sqrt(np.sum((coord-stop_arr_b[:,:2])**2,axis=1)) #calculate distance to the all stop coordinates
                idx = np.argmin(dist) #get the min distance
                seq_b.append(stop_arr_b[idx,2]) #append the sequence
                
            seq_a = np.asarray(seq_a)
            seq_b = np.asarray(seq_b)
            timestamps = np.asarray(timestamps)
            timestamps = timestamps - timestamps.mean() #normalize
            ma = np.polyfit(timestamps, seq_a, deg=1) #fit a line for seq a
            mb = np.polyfit(timestamps, seq_b, deg=1) #fit a line for seq b
            if ma[0]>mb[0]: #if equation a have a greater angle than equation b, the bus goes in direction a
                return dir_a_name
            else:
                return dir_b_name
        
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
#data_rows=data_rows[:10000] #uncomment to get a smaller chunk for debugging
for j, time_row in enumerate(data_rows): #iterate in each time frame, but start from the last row of the outer loop 
    if DEBUG>=1:
        print 'step 1, iteration', j, 'of', len(data_rows)
    
    if INFER.lower()=='route':
        key = str(time_row['Onibus']) +'_' +str(time_row['DiaMes'])+'_'+str(time_row['Mes'])+'_'+str(time_row['Ano'])
    elif INFER.lower()=='direction':
        if time_row['Linha'] =='': 
            continue #skip records that do not have a route id
        key = str(time_row['Onibus']) +'_' +str(time_row['Hora'])+'_' +str(time_row['DiaMes'])+'_'+str(time_row['Mes'])+'_'+str(time_row['Ano'])
    else:
        print 'INFER type not recognized. Valid options are ''route'' or ''direction'''
        break
    
    #if key != 'C87019_13_6_1_2014': #DEBUG
    #    continue
    if time_row['Onibus'] !='C87019':
        continue
    
    if key not in dic_closest_routes:
        dic_closest_routes[key] = {}
        row_pointers[key] = []
    
    row_pointers[key].append(j) #append the rows used in this key    
    coord = np.array([[float(time_row['LatitudePonto']),float(time_row['LongitudePonto'])]], np.float32) #convert coordinates to numpy array
    
    if INFER.lower()=='route':
        dist = np.sqrt(np.sum((coord-stop_arr[:,:2])**2,axis=1)) #calculate distance to the all stop coordinates    
        sorted_dist = np.argsort(dist) #sort by distances and get the first top_closest indices
        
        dic_aux={}
        for idx in sorted_dist:
            route_id = stops_lst[idx]['linha'] #get route id from the bus stop coordinate
            if route_id in dic_aux:
                continue
            
            if not route_id in  dic_closest_routes[key]:
                dic_closest_routes[key][route_id] = [] #case the route_id key does not exists, insert it
            dic_closest_routes[key][route_id].append([coord,dist[idx],None]) #append the coordinates visited to that route and the distance to the closest point
            
            dic_aux[route_id] = True
            if len(dic_aux) >=TOP_CLOSEST_PER_COORD:
                break
    else:
        
        x = dateutil.parser.parse(time_row['TimeStamp'])
        time_diff = (datetime(x.year,x.month,x.day,x.hour,x.minute,x.second)-datetime(1970,1,1,0,0,0)).total_seconds()
            
        if not time_row['Linha'] in  dic_closest_routes[key]:
            dic_closest_routes[key][time_row['Linha']] = [] #case the route_id key does not exists, insert it
        dic_closest_routes[key][time_row['Linha']].append([coord,None,time_diff]) #append the coordinates visited to that route
        
#after iterating through all records, get the closest routes and directions for each key of type "bus_id + time_frame"
i =0
total_iter = len(dic_closest_routes.items())
for key, value in dic_closest_routes.items():
    i=i+1
    if DEBUG>=1:
        print 'step 2, iteration', i, 'of', total_iter
    if INFER.lower() =='route':
        route_id = get_majority_route(value)
        add_to_file(data_rows, row_pointers[key], out_file_path, route_id=route_id, direction=None)
    else:
        direction = infer_bus_directions(value)
        add_to_file(data_rows, row_pointers[key], out_file_path, route_id=None, direction=direction)           

print 'total time', datetime.now()-tstart
print 'finished'