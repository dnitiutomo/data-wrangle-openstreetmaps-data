# -*- coding: utf-8 -*-
"""
Created on Thu Apr 02 10:12:38 2015

@author: Daniel
"""

import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import json
import operator
import pandas as pd

#Filenames to access
osm_file = "C:\\Users\\Daniel\\Documents\\OnlineLearning\\MongoDB\\New York\\new-york_new-york.osm"
json_file = "C:\\Users\\Daniel\\Documents\\OnlineLearning\\MongoDB\\New York\\new_york.json"
test_file = "C:\\Users\\Daniel\\Documents\\OnlineLearning\\MongoDB\\New York\\test_new.json"
bor_csv = 'C:\\Users\\Daniel\\Documents\\OnlineLearning\\MongoDB\\New York\\NYC_Borough_Mappings.csv'
top_level_file = 'C:\\Users\\Daniel\\Documents\\OnlineLearning\\MongoDB\\New York\\top_level_fields.txt'

#Dataframe that contains all of the neighborhood to borough mappings
hoods = pd.read_csv(bor_csv)
HOOD_LIST = hoods['Neighbourhood'].tolist()
HOOD_LIST = [x.lower() for x in HOOD_LIST]
hoods['lower']=pd.Series(HOOD_LIST)

#Regular expressions used to match patterns for keys
lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
post_code_reg = re.compile(r'([\d]){5}')
post_code_ext = re.compile(r'([\d]){5}-([\d]){4}')
int_num = re.compile(r'([\d])+(,([\d])*)*')
float_num = re.compile(r'([\d])+(\.)*([\d])*')

#The following keys should be grouped together as they fall into the same category
CREATED = [ "version", "changeset", "timestamp", "user", "uid"]
CONTACT = ["email","phone","contact:phone","contact:fax","fax","contact:email"]
MEDIA = ["website","contact:website","link:google_plus","facebook","yelp","twitter","url","media:commons"]

#The following are used to check for values that should be the same
NEW_YORK = ["ny","nyc","new york","new york city","new york ny"]
BOROUGHS = {"manhattan":"Manhattan","queens:":"Queens","bronx":"Bronx","brooklyn":"Brooklyn","staten island":"Staten Island"}

#The following lists are used to determine the proper names for street endings
STREETS = ['st','st.', 'street','steet']
AVENUES = ['ave', 'ave.', 'avenue','aveneu','avene']
BOULEVARDS = ['blvd.','blvd','boulevard','blv.']
ROADS = ['road','rd','rd.']
OTHER_STREETS = { 'ct':'Court',
                 'plz':'Plaza',
                 'dr':'Drive',
                 'pl':'Place',
                 'tpke':'Turnpike',
                 'hwy':'Highway',
                 'pkwy':'Parkway',
                 'pky':'Parkway'}

KEY = 'k'
VALUE = 'v'

#Format the city field and determine if the city specified is NY, a borough, a neighborhood, or another city
def get_city_name(bad_city, node): 
    city = bad_city.split(',')[0].lower()
    if city in NEW_YORK:
        node['address']['city'] = "New York"
    elif city in HOOD_LIST:
        node['address']['city'] = "New York"
        node['address']['borough'] = hoods[hoods['lower']==city].iloc[0]['Borough']
        node['address']['neighbourhood'] = hoods[hoods['lower']==city].iloc[0]['Neighbourhood']
    elif city in BOROUGHS.keys():
        node['address']['city'] = "New York"
        node['address']['borough'] = BOROUGHS[city]
    else:
        node['address']['city'] = bad_city.split(',')[0]

#Normalize the state names
def get_state_name(bad_state, node):
    if bad_state.lower() in ["ny", "new york"]:
        node['address']['state'] = "NY"
    elif bad_state.lower() in ["nj", "new jersey"]:
        node['address']['state'] = "NJ"
    elif bad_state.lower() in ["ct", "connecticut"]:
        node['address']['state'] = "CT"
    else:  
        node['address']['state'] = bad_state

#Normalize the primary postal code and add a field for the zop code extension
def get_post_code(bad_code, node):
    main_post = ''
    m1 = post_code_ext.search(bad_code)
    if m1:
        code = m1.group().split('-')    
        main_post = code[0] 
        node['address']['postcode_ext'] = code[1]
    else:
        m2 = post_code_reg.search(bad_code)
        if m2:
            main_post = m2.group()    
    node['address']['postcode'] = main_post 
    
    if main_post in HOOD_LIST:
        node['address']['borough'] = hoods[hoods['Neighbourhood']==main_post].iloc[0]['Borough']

#Normalize the street names
def get_street_name(bad_street, node):
    name = bad_street.split()
    end = name[len(name)-1]
    if end.lower() in STREETS:
        end = 'Street'
    elif end.lower() in AVENUES:
        end = 'Avenue'
    elif end.lower() in BOULEVARDS:
        end = 'Boulevard'
    elif end.lower() in ROADS:
        end = 'Road'
    elif end.lower() in OTHER_STREETS.keys():
        end = OTHER_STREETS[end.lower()]
    new = ''
    for x in range(len(name)-1):
        new += name[x]
        new += ' '
    new += end
    
    node['address']['street'] = new    

def get_int_field(bad_number, node, key):
    m = int_num.search(bad_number)
    if m:
        sp = m.group().split(',')
        node[key] = int("".join(sp))

def fix_numeric_field(bad_number, node, key):
    m = float_num.match(bad_number)
    if m:
        num = m.group()
        node[key] = float(num)
        metric = bad_number[m.end():].strip().lower()
        if len(metric) > 0:
            if metric in ['\'', 'ft','feet']:
                node[key+'_metric'] = 'feet'
            elif metric in ['meter','m','met']:
                node[key+'_metric'] = 'meter'            
            elif metric in ['\"','inches']:
                node[key+'_metric'] = 'inches'
            elif ';' in metric:
                node[key+'_2'] = float(bad_number.split(';')[1])
            else:
                node[key+'_metric'] = metric
            

#Analyze the element and create the dictionary to store within a JSON object
def shape_element(element):
    node = {}
    pos = [0,0]
    
    #Only interested in looking at NODE and WAY elements
    if element.tag == "node" or element.tag == "way":
        node['element_type'] = element.tag
        
        #Iterate through all of the child 
        for child in element:
            if KEY in child.attrib:
                key_v = child.attrib[KEY]
                problem = problemchars.match(key_v)
                
                #Do not include any problem characters, gnis, nor tiger attributes
                if (not problem) and ('gnis:' not in key_v) and ('tiger:' not in key_v):
                    #Create the contact dictionary for contact attributes
                    if key_v.lower() in CONTACT:
                        if 'contact' not in node.keys():
                            node['contact'] = {}                            
                        a = key_v.split(':')
                        if len(a) > 1:
                            node['contact'][a[1]] = child.attrib[VALUE]
                        else:
                            node['contact'][key_v] = child.attrib[VALUE]
                    
                    #Create the contact dictionary for media attributes
                    elif key_v.lower() in MEDIA:
                        if 'media' not in node.keys():
                            node['media'] = {}                            
                        a = key_v.split(':')
                        if len(a) > 1:
                            node['media'][a[1]] = child.attrib[VALUE]
                        else:
                            node['media'][key_v] = child.attrib[VALUE]
                    else:
                        #split the key on ':' and analyze them based on their nested structure
                        val_arr = key_v.split(':')
                        length = len(val_arr)                    
                        
                        if length == 1:
                            if 'cityracks.' in key_v:
                                if 'cityracks' not in node.keys():
                                    node['cityracks'] = {}
                                a = key_v.split('.')
                                node['cityracks'][a[1]] = child.attrib[VALUE]
                            elif key_v in ['population','capacity','frequency']:
                                get_int_field(child.attrib[VALUE], node, key_v)
                            elif key_v in ['height','min_height','maxspeed','minspeed']:
                                fix_numeric_field(child.attrib[VALUE], node, key_v)
                            else:
                                node[key_v] = child.attrib[VALUE]
    
                        elif length == 2:
                            #create the address dictionary
                            if 'addr' == val_arr[0]:
                                if 'address' not in node.keys():
                                    node['address'] = {}
                                
                                key = val_arr[1]
                                value = child.attrib[VALUE]
                                
                                if key == 'city':
                                    get_city_name(value, node)
                                elif key == 'state':
                                    get_state_name(value, node)
                                elif key == 'postcode':
                                    get_post_code(value, node)
                                elif key == 'street':
                                    get_street_name(value, node)
                                else:
                                    node['address'][val_arr[1]] = value
                            
                            else:
                                #Must create a new dictionary for a nested list if not created already
                                key = val_arr[0]
                                if key not in node.keys():
                                    node[key] = {}
                                #if the value for key is only a string, make it a dictionary
                                elif type(node[key]) is not dict:
                                    temp = node[key]
                                    node[key] = {}
                                    node[key][key] = temp
  
                                node[key][val_arr[1]] = child.attrib[VALUE]
                                
                        #Handle nested key that had 3 keys
                        elif length == 3:
                            key_1 = val_arr[0]
                            key_2 = val_arr[1]
                            
                            if key_1 not in node.keys():
                                node[key_1] = {}
                            elif type(node[key_1]) is not dict:
                                    temp = node[key_1]
                                    node[key_1] = {}
                                    node[key_1][key_1] = temp
                                    
                            if key_2 not in node[key_1].keys():
                                node[key_1][key_2] = {}                                
                            elif type(node[key_1][key_2]) is not dict:
                                    temp = node[key_1][key_2]
                                    node[key_1][key_2] = {}
                                    node[key_1][key_2][key_2] = temp
    
                            node[key_1][key_2][val_arr[2]] = child.attrib[VALUE]                      
            else:
                #Add node references to a list
                if 'ref' in child.attrib:
                    if 'node_refs' not in node.keys():
                        node['node_refs'] = [] 
                    node['node_refs'].append(child.attrib['ref'])
        
        #iterate through all of the attributes of the elements                    
        for attr in element.attrib:
            if attr in CREATED:
                if 'created' not in node.keys():
                    node['created'] = {}  
                node['created'][attr] = element.attrib[attr]
            elif attr == 'lon':
                pos[1] = float(element.attrib[attr])
            elif attr == 'lat':
                pos[0] = float(element.attrib[attr])
            else:
                node[attr] = element.attrib[attr]              

        node['pos'] = pos
        
        return node
    else:
        return None

#Counts the number of times a node has a unique key
def count_tags(filename):
    tags=defaultdict(int)
    for event, elem in ET.iterparse(filename):
        if event == 'end':
            tags[elem.tag]+=1
            # discard the element if it is an end element
            elem.clear()   
    print tags

#Get a count for all of the top level keys within the OSM file    
def analyze_map(filename):
    tags=defaultdict(int)
    f = open(top_level_file, 'w')
    #get the iterable object
    context = ET.iterparse(filename, events=("start", "end"))
    #turn the iterable object into an iterable instance    
    context = iter(context)
    #get the root element
    event, root = context.next()
    
    for event, element in context:
        if event == 'end':
            while True:
                try:
                    js = shape_element(element)
                    if js:
                        for key in js:
                            tags[key]+=1
                    break
                except Exception as excp:
                    #Print out why the element could not be processed
                   print 'wrong: ', excp
                   print element.attrib['id']
                   for child in element:
                        if KEY in child.attrib:
                            print child.attrib[KEY],': ' ,child.attrib[VALUE]
                   print element
                   break
            root.clear()
        
    sorted_line= sorted(tags.items(), key=operator.itemgetter(1), reverse=True)
    for entry in sorted_line:
        f.write('{0}, {1}\n'.format(entry[0], entry[1]))

#Create the JSON file to input into MongoDB
def process_map(osm_filename, json_filename):
    f = open(json_filename, 'w')
    
    #get the iterable object
    context = ET.iterparse(osm_filename, events=("start", "end"))
    #turn the iterable object into an iterable instance    
    context = iter(context)
    #get the root element
    event, root = context.next()
    
    for event, element in context:
        if event == 'end':
            while True:
                try:
                   js = shape_element(element) 
                   if js:
                       json.dump(js,f)
                       f.write('\n')
                   break
                except Exception as excp:
                    #Print out why the element could not be processed
                   print 'wrong: ', excp
                   print element.attrib['id']
                   for child in element:
                        if KEY in child.attrib:
                            print child.attrib[KEY],': ' ,child.attrib[VALUE]
                   print element
                   break
            root.clear() #clear the element to manage memory usage
            
    f.close()

#analyze_map(osm_file)
process_map(osm_file, json_file)