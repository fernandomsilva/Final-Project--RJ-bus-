import ast
import sys
import csv
import numpy as np
import os

AMAZON=False
INFER = 'direction' #possible values are 'direction' or 'route'
stops_folder = '/home/rfn/finalproject/linhas/'

def get_majority_route(dic_routes):
    vals = dic_routes.values()
    
    arr_len = np.zeros((len(vals)),dtype=np.float32)
    arr_dist = np.zeros((len(vals)),dtype=np.float32)
    #convert the list of coordinates to an array that is the number of coordinates
    for i,coords_dist_lst in enumerate(vals):
        arr_len[i] = len(coords_dist_lst)
        sum_dist=0
        for coord_dist in coords_dist_lst:
            sum_dist = sum_dist+float(coord_dist[1])
        arr_dist[i] = sum_dist    
    
    arr_avg = arr_dist/arr_len
    max_len = arr_len.max()
    crit_idx = arr_len<0.7*max_len
    arr_avg[crit_idx] = np.inf*arr_avg[crit_idx]
    return dic_routes.keys()[arr_avg.argmin()]

def create_dir_stop_dic(dir_path):
    stops_dic = {} # the final dic format is <key:route_id, value: <key:direction, value:list of stops> >    
    stops_lst = []
    if AMAZON:
        from boto.s3.connection import S3Connection
        conn = S3Connection('AKIAJJZNUBE63HJDGAYQ','SeIebAas6Lby1COMqNUZ6cmzx0KuFow5SmPXJKbx')
        bucket = conn.get_bucket('linhasrj')
        lst_files = [] 
        for key in bucket.list():
            file_name = key.name.encode('utf-8')
            if '.csv' in file_name:
                lst_files.append(key) 

    else:
        lst_files = [os.path.join(dp, f) for dp, _, filenames in os.walk(dir_path) for f in filenames if os.path.splitext(f)[1] == '.csv']
    for file_name in lst_files: 
        if AMAZON:
            file_pointer = file_name.get_contents_as_string().split('\n')
        else:
            file_pointer = open(file_name, 'rb')
        route_lst = list(csv.DictReader(file_pointer, delimiter=','))[1:]
    
        #construct the stops array for directions a and b separately         
        for stop_row in route_lst:
            if not stop_row['linha'] in stops_dic: #insert route_id key in case it does not exist
                stops_dic[stop_row['linha']] ={}
            if not stop_row['shape_id'] in stops_dic[stop_row['linha']]: #insert direction key in case it does not exist
                stops_dic[stop_row['linha']][stop_row['shape_id']] = []
            stops_dic[stop_row['linha']][stop_row['shape_id']].append([float(stop_row['latitude']),float(stop_row['longitude']),float(stop_row['sequencia'])])
        stops_lst.extend(route_lst) #the merge two lists
    return stops_dic, stops_lst

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
                
                timestamps.append(float(coord_dist[2]))    
                coord_str = coord_dist[0]
                
                coord = np.array([[float(coord_str[0]),float(coord_str[1])]])
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
        
#construct the stops array for directions a and b separated. It will be used in the bus direction calculation
stops_dic, stops_lst = create_dir_stop_dic(stops_folder)
#convert stop_rows to array:
stop_arr = np.zeros((len(stops_lst),3),dtype=np.float32)
for i, stop_row in enumerate(stops_lst):
    stop_arr[i,0] = stop_row['latitude'] 
    stop_arr[i,1] = stop_row['longitude']
    stop_arr[i,2] = stop_row['sequencia']

#get the closest routes and directions for each key of type "bus_id + time_frame"
dic_all = {}
for line in sys.stdin:
    key, value = line.split("\t")
    dic, rows = ast.literal_eval(value) #deserialize. there is a dic and a list in value
    if key not in dic_all:
        dic_all[key] = [{},[]]
    dic_all[key][1].extend(rows)
    
    for key2, value2 in dic.items():
        if key2 not in dic_all[key][0]:
            dic_all[key][0][key2] = []
        dic_all[key][0][key2].extend(value2)

for key,value in dic_all.items():
    dic, rows = value
    
    route_id, direction = None, None
    if INFER.lower() =='route':
        route_id = get_majority_route(dic)
    else:
        direction = infer_bus_directions(dic)
    #print key,direction,dic     
    for row in rows:
        print str(row)+';'+str(route_id)+';'+str(direction)

