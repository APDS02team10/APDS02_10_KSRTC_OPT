#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 15 12:44:49 2019

@author: i074765
"""

class DataPreprocessing:
    
    def __init__(self, d):
        import json as js
        from string import Template as t
        import shutil
        import os
        import pandas as pd
        import sqlite3 as sql
        import traceback
        
        self.js = js
        self.t = t
        self.shell_utill = shutil
        self.os_mod = os
        self.src_dir = d['raw']
        self.clean_data_dir = d['clean']
        self.process_dir = d['process']
        self.datastructure_json = d['parser']
        self.db_dir = d['db']
        self.pd = pd
        self.db = sql
        self.traceback = traceback         
#        self.src_dir = '../data/raw'
#        self.clean_data_dir = '../data/clean_data'
#        self.process_dir = '../data/processed'
#        self.datastructure_json = '../src/models/datastructure.json'  
        self.readDataParser()
        
    def readDataParser(self):
        with open(self.datastructure_json) as f:
            self.data_parser = self.js.load(f)
            return self.data_parser
        
    def getTemplate(self, seperator=","):
        p = seperator+"$"
        return self.t("$"+p.join(self.data_parser['data_cols'].keys()))
    
    def parseString(self, a):
        p = self.data_parser['data_cols']
        return {k:a[v['start'] - 1:v['size']+v['start'] - 1].strip() for k,v in p.items()}
    
    def parseTemplateForString(self, a, seperator=","):
        g = self.getTemplate(seperator)
        return g.substitute(self.parseString(a))
        
    def read_process_cleandata(self, buffersize=50, filename='full_data.csv'):
        csv_reader = None
        path = self.os_mod.path.join(self.clean_data_dir,filename)
        if not self.os_mod.path.exists(path):
            print("NOT FOUND clean data file: {}".format(path))
            return None
        print("Found clean data file: {}".format(path))
        csv_reader = self.pd.read_csv(path, parse_dates={'ETD_DATETIME':['ETD_DATE','ETD_TD_TIME']}, chunksize=buffersize)
        print("csv chunk reader created {}".format(csv_reader))
        return csv_reader

    def connect_db(self, db_name):
        if not self.os_mod.path.exists(self.db_dir):
            print("Db directory is not correct")
            return False
        conn = self.db.connect(self.os_mod.path.join(self.db_dir,db_name))
        print('connection established : {}'.format(conn))
        return conn

    
    def get_table_rowcount(self, conn, tablename):
        p = conn.execute("select count(*) from {}".format(tablename))
        g = p.fetchall()
        print(" Count has been selected from table {}".format(tablename))
        print(" Count has been selected from table is here {}".format(g))        
        return g


    def clean_db_table(self, conn, tablename):
        conn.execute("delete from {}".format(tablename))
        conn.commit()
        print("Table has been truncated number of rows are {}".format(self.get_table_rowcount(conn, tablename)))
        

    def is_table_exists(self, conn, tablename):
        print(" TABLE EXISTS ?? ")
        s = "SELECT name FROM sqlite_master WHERE type='table' AND name='{}'".format(tablename)
        print(" firing query {}".format(s))
        p = conn.execute(s)
        return len(p.fetchall()) == 1
    
    def write_to_db(self, db_name, tablename, csv_reader):
        count_chnk = 1
        conn = self.connect_db(db_name)
        print(" Here got the DB: {}".format(conn))
        print(" Here got the Table: {}".format(tablename))
        print(" Here got the db name: {}".format(db_name))
        if not conn and not tablename and not csv_reader:
            print("write_to_db >> one or more object is missing")
            return False
        
        try:
            print(" First cleaning table: {}".format(tablename))
            if self.is_table_exists(conn, tablename):
                print(" TABLE EXISTS !!! ")
                self.clean_db_table(conn, tablename)
                print("{} table has rows : {}".format(tablename, self.get_table_rowcount(conn, tablename)))
                
            for chunk in csv_reader:
                chunk.to_sql(tablename, con=conn, if_exists='append', index_label='ID')
                print(" writing chunk : {}".format(count_chnk))
                count_chnk+=1
            conn.commit()
        
            print("Table {} has been fetched with data".format(tablename))
            print("{} table has rows : {}".format(tablename,self.get_table_rowcount(conn, tablename)))
        except Exception as ex:
            print("   ****  EXCEPTION ***  ")
            print(''.join(self.traceback.format_exception(etype=type(ex), value=ex, tb=ex.__traceback__)))
            conn.close()
        else:
            conn.close()
        
        
        
        
    def write_processed_data(self, data_file, clean_data_file):       
        read_flag = False        
        if self.os_mod.path.exists(clean_data_file):
            fw = open(clean_data_file,'a+')
            print(">>> File already exists")
        else:
            fw = open(clean_data_file,'w+')
            print(">>> Created New output File")
            fw.write(",".join(self.data_parser['data_cols'].keys()))
            fw.write("\n")
        
        with open(data_file) as f:
            p = f.readline()
            while p:
                if 'ETD_WAYBILL_NO' in p:
                    p = f.readline()
                    p = f.readline()
                    read_flag = True
                    continue
                if read_flag:
                    if len(p) < 10:
                        read_flag = False
                    else:
                        g = self.parseTemplateForString(p)
                        fw.write(g)
                        fw.write("\n")
                p = f.readline()
                
        fw.close()
        
    def processData(self, format='csv', isOneFile=True):
        ext = "."+format
        if isOneFile:
            q = self.os_mod.path.join(self.clean_data_dir,'full_data'+'.'+format)
            
        for file in self.os_mod.listdir(self.src_dir):
            if file.lower().endswith(".txt"):
                p = self.os_mod.path.join(self.src_dir,file)
                if not isOneFile:
                    q = self.os_mod.path.join(self.clean_data_dir,file)
                    q = q.replace(".txt",ext)
                print("new format is {}".format(q))
                self.write_processed_data(p,q)
                self.shell_utill.move(p,self.process_dir)
                
    
    def write_processeddata_db(self, db_name, tablename):
        reader = self.read_process_cleandata()
        self.write_to_db(db_name, tablename, reader)
        
        
            
        