import csv
import numpy as np
import os
import dateutil.parser
import datetime
import collections
from sklearn.svm import OneClassSVM
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from scipy.linalg import svd
import cPickle
from scipy import stats

PCA=True #use PCA or One-ClassSVM
n_components=10
SVM=False
#MAX_LINES=200000000 #155M is the number of lines in the raw file
MAX_LINES=1000000 #use this for debug
wd_size = 20
confidence = 0.9 #between 0 and 1
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

if SVM:
    if TRAIN:
        clf = OneClassSVM(kernel='linear')
        
        clf = clf.fit(X_train)
        
        with open('clf.pkl', 'wb') as f:
            cPickle.dump(clf, f)
    else:    
        clf= cPickle.load(open('clf.pkl', 'rb'))
    
    print clf
 
if PCA:
    U, S, V = svd(X_train)
    print 'U.shape',U.shape
    print 'S.shape',S.shape
    print 'V.shape',V.shape
    X_train_rot = np.dot(X_train,V)
    X_test_rot = np.dot(X_test,V)
    X_train_residual = X_train_rot[:,n_components:]
    std = X_train_residual.std(axis=0)
    mean = X_train_residual.mean(axis=0)
    multiplier = stats.norm.interval(alpha=confidence, loc=mean, scale=std)
    
    idx = ((X_train_residual < (mean + multiplier[0]*std).reshape((1,-1))).any(axis=1) & (X_train_residual > (mean + multiplier[1]*std).reshape((1,-1))).any(axis=1)) 
    y_test_pred = X_test_rot[idx,:]
    print 'y_test_pred',y_test_pred
    print 'y_test_pred.shape[0]',y_test_pred.shape[0]
    
else:
    y_train_pred = clf.predict(X_train)
    #y_train_score = clf.score(X_train)
    y_test_pred = clf.predict(X_test)
    #y_tst_score = clf.score(X_test)
    #print 'y_score train', y_train_score    
    print '(y_train_pred==-1).sum()',(y_train_pred==-1).sum()
    print '(y_train_pred==1).sum()',(y_train_pred==1).sum()
    #print 'y_score test', y_test_score
    print '(y_test_pred==-1).sum()',(y_test_pred==-1).sum()
    print '(y_test_pred==1).sum()',(y_test_pred==1).sum()

print 'Finished'