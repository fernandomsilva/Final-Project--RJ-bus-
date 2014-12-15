import csv
import numpy as np
import os
import dateutil.parser
import datetime
import collections
from sklearn.svm import OneClassSVM
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

import cPickle

MAX_LINES=200000000 #155M is the number of lines in the raw file
#MAX_LINES=1000000
wd_size = 20
data_path_small = 'C:/NYU/BigData/FinalProject/raw_data_small'
data_path = 'C:/NYU/BigData/FinalProject/raw_data_all'
TRAIN=True
route_train ='360'
route_test ='360' 

def load_dataset(file_name, route_id):
    X = []
    dic = {}
    with open(file_name, 'rb') as f:
        i = 0
        header=f.readline()#skip the first line
        for line in f:
            
            i=i+1
            if i>=MAX_LINES:
                break
            
            row_id, point_name, linha, bus_id , hour, daymonth, month, year,speed,latitude,longitude,timestamp,desc = line.split(',')
            
            if route_id !=linha:
                continue
            #if bus_id!='C87079':
            #    continue
            x = dateutil.parser.parse(timestamp)
            time_diff = (datetime.datetime(x.year,x.month,x.day,x.hour,x.minute,x.second)-datetime.datetime(x.year,x.month,x.day,0,0,0)).total_seconds()
            
            key=bus_id+'_'+daymonth+'_'+month+'_'+year
            if key not in dic:
                dic[key] = collections.deque(maxlen=wd_size)
                
            dic[key].append([time_diff,float(latitude),float(longitude)])
            
            if len(dic[key]) >= wd_size:
                X.append(np.asarray(dic[key]).reshape(-1))
                   
    return np.asarray(X)
print 'loading...'

print 'wd_size', wd_size
print 'route_train', route_train
print 'route_test', route_test
X_train = load_dataset(data_path, route_train)
X_test = load_dataset(data_path_small, route_test)

print 'X_train.shape',X_train.shape
print 'X_test.shape',X_test.shape

print 'training...'
norm = StandardScaler(copy=True)
X_train = norm.fit_transform(X_train)
X_test = norm.transform(X_test)
if TRAIN:
    clf = OneClassSVM(kernel='linear')
    U, s, Vh = linalg.svd(a)
    clf = RandomizedPCA(n_components=10, whiten=True)
    #clf = KMeans(n_clusters=1,n_init=10, max_iter=300, tol=0.0001)
    clf = clf.fit(X_train)
    
    with open('clf.pkl', 'wb') as f:
        cPickle.dump(clf, f)
else:    
    clf= cPickle.load(open('clf.pkl', 'rb'))

X_train_transf = clf.transform(X_train)
X_test_transf = clf.transform(X_test)

if 

#y_train_pred = clf.predict(X_train)
#y_train_score = clf.score(X_train)
#y_test_pred = clf.predict(X_test)
#y_test_score = clf.score(X_test)
#print 'y_train_predy_pred train', y_train_pred
#print 'y_score train', y_train_score
print clf
 
print '(y_train_pred==-1).sum()',(y_train_pred==-1).sum()
print '(y_train_pred==1).sum()',(y_train_pred==1).sum()
#print 'y_test_pred', y_test_pred
#print 'y_score test', y_test_score
print '(y_test_pred==-1).sum()',(y_test_pred==-1).sum()
print '(y_test_pred==1).sum()',(y_test_pred==1).sum()

print 'Finished'