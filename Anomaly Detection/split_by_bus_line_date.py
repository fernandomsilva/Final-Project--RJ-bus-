import csv
import os.path

#data_file_path = '/home/rfn/NYU/BusRio/raw_data_all'
#output_dir = '/home/rfn/NYU/BusRio/Splitted_Data/'
data_file_path = '/home/rfn/NYU/BusRio/raw_data_small'
output_dir = '/home/rfn/NYU/BusRio/Splitted_Data_Small/'
data_rows = csv.reader(open(data_file_path, 'rb'), delimiter=',')


for i,row in enumerate(data_rows):
    print i
    #if i >=200:
    #    break
    #if row['Linha']!='':
    if row[2]!='':
        #out_file_name = row['Linha']+'_'+row['Onibus']+'_'+row['Ano']+'_'+row['Mes']+'_'+row['DiaMes']+'.csv'
        out_file_name = row[2]+'_'+row[3]+'_'+row[7]+'_'+row[6]+'_'+row[5]+'.csv'
        
        #write_header=False
        #if not os.path.isfile(output_dir+'/'+out_file_name): #write headers if file does not exists
        #    write_header=True
            
        with open(output_dir+'/'+out_file_name,'a') as f:
            csvfile = csv.writer(f, delimiter=';', lineterminator='\n')
            #if write_header:
            #    csvfile.writerow(row.keys())
            #csvfile.writerow(row.values())
            csvfile.writerow(row)
            
print 'finished'