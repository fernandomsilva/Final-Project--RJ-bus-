import sys
import csv
import numpy as np
import os
import dateutil.parser
from datetime import datetime
AMAZON=False
INFER = 'direction' #possible values are 'direction' or 'route'
TOP_CLOSEST_PER_COORD = 100
stops_folder = '/home/rfn/finalproject/linhas/'
#stops_folder = 'C:/NYU/BigData/FinalProject/linhas/'
#data_file_path = 'C:/NYU/BigData/FinalProject/data_all_small.csv'

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
    row_id, point_name, linha, bus_id , hour, daymonth, month, year,speed,latitude,longitude,timestamp,desc = line.split(';') 
    
    if INFER.lower()=='route':
        key = str(bus_id)+'_'+str(daymonth)+'_'+str(month)+'_'+str(year)
    elif INFER.lower()=='direction':
        if linha =='': 
            continue #skip records that do not have a route id
        key = str(bus_id)+'_' +str(hour)+'_' +str(daymonth)+'_'+str(month)+'_'+str(year)
    else:
        print 'INFER type not recognized. Valid options are ''route'' or ''direction'''
        break
    
    
    #if key != 'C87019_9_6_1_2014': #DEBUG
    #    continue
    #if bus_id !='C87019' and bus_id !='C41346' :
    if bus_id !='C87019':
        continue
    
    if key not in dic_closest_routes:
        dic_closest_routes[key] = {}
        rows[key] = []
    
    rows[key].append(line.strip()) #append the rows used in this key
    try:    
        coord = np.array([[float(latitude),float(longitude)]], np.float32) #convert coordinates to numpy array
    except:
        continue
    
    if INFER.lower()=='route':
    
        dist = np.sqrt(np.sum((coord-stop_arr[:,:2])**2,axis=1)) #calculate distance to the all stop coordinates
        sorted_dist = np.argsort(dist)
        
        dic_aux={}
        for idx in sorted_dist[:TOP_CLOSEST_PER_COORD]:
            route_id = stops_lst[idx]['linha'] #get route id from the bus stop coordinate
            if route_id in dic_aux:
                continue
            
            if not route_id in  dic_closest_routes[key]:
                dic_closest_routes[key][route_id] = [] #case the route_id key does not exists, insert it
            dic_closest_routes[key][route_id].append([[latitude, longitude],dist[idx],None]) #append the coordinates visited to that route
            
            dic_aux[route_id] = True
            if len(dic_aux) >=TOP_CLOSEST_PER_COORD:
                break
    else:
        x = dateutil.parser.parse(timestamp)
        time_diff = (datetime(x.year,x.month,x.day,x.hour,x.minute,x.second)-datetime(2010,1,1,0,0,0)).total_seconds()        
        if not linha in  dic_closest_routes[key]:
            dic_closest_routes[key][linha] = [] #case the route_id key does not exists, insert it
        dic_closest_routes[key][linha].append([[latitude, longitude],None,time_diff]) #append the coordinates visited to that route       
#after iterating through all records...
for key, value in dic_closest_routes.items():
    print str(key)+"\t"+ repr([value, rows[key]]) #output the key value pairs
    