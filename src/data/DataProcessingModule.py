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
        
        self.js = js
        self.t = t
        self.shell_utill = shutil
        self.os_mod = os
        self.src_dir = d['raw']
        self.clean_data_dir = d['clean']
        self.process_dir = d['process']
        self.datastructure_json = d['parser']          
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
    
    def write_processed_data(self, data_file, clean_data_file):       
        read_flag = False
        fw = open(clean_data_file,'w')
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
        
    def processData(self, format='txt'):
        ext = "."+format
        self.os_mod.listdir(self.src_dir)
        for file in self.os_mod.listdir(self.src_dir):
            if file.lower().endswith(".txt"):
                p = self.os_mod.path.join(self.src_dir,file)
                q = self.os_mod.path.join(self.clean_data_dir,file)
                q = q.replace(".txt",ext)
                print("new format is {}".format(q))
                self.write_processed_data(p,q)
                self.shell_utill.move(p,self.process_dir)        
            
        