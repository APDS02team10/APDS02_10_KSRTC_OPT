import os, sys
import time
os.getcwd()
# os.pardir
# sys.path.insert(0,os.path.pardir)
print(os.getcwd())
from src.data import DataProcessingModule as dp

dir_dict = {
        'raw' : './data/raw',
        'clean' : './data/clean_data',
        'process' : './data/processed',
        'parser' : './src/models/datastructure.json',
        'db' : './data/db'
}

start_time = time.time()
if len(sys.argv) == 2:
        if sys.argv[1].lower() not in ['csv', 'txt']:
                print("\n{} format not allowed \n".format(sys.argv[1]))
        else:
                print("\n ------------ Starts Processing -------------------")
                dp_class = dp.DataPreprocessing(dir_dict)
                dp_class.processData(format=sys.argv[1].lower())
                print("\n ------------ Data cleaned in : {} seconds --------\n ".format(time.time() - start_time))
else:
        print("\n ------------ Starts Processing -------------------")
        dp_class = dp.DataPreprocessing(dir_dict)
        dp_class.processData()
        print("\n ------------ Data cleaned in : {} seconds --------\n ".format(time.time() - start_time))
        start_time = time.time() 

        print("\n ------------ Start db writing db --------\n ")
        dp_class.write_processeddata_db('ksrctc.db', 'KSRCTC_BUS')
