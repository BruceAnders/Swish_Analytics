# -*- coding: utf-8 -*-
#############################################
"""
Created on Fri Sep 17 20:39:29 2021

Bruce Anders 
MAVS assingment 
"""
#############################################
"""
Import packages
"""
#############################################

import xml.etree.ElementTree as ET
import pandas as pd 
import numpy as np
from xml.dom import minidom 
import time
import datetime as dt
from pandasql import sqldf


#############################################
"""
Assign Variables 
"""
###BOX SCORE###
boxscore_XML =r"C:\Users\peyto\Desktop\Mavs_Assignement\0021801216_boxscore.xml"
BXS_tree = ET.parse(boxscore_XML)
BXS_root = BXS_tree.getroot()
###   Q1   ###
Q1_XML =r"C:\Users\peyto\Desktop\Mavs_Assignement\0021801216_pbp_Q1.xml"
Q1_tree = ET.parse(Q1_XML)
Q1_root = Q1_tree.getroot()
###   Q2   ###
Q2_XML =r"C:\Users\peyto\Desktop\Mavs_Assignement\0021801216_pbp_Q2.xml"
Q2_tree = ET.parse(Q2_XML)
Q2_root = Q2_tree.getroot()
###   Q3   ###
Q3_XML =r"C:\Users\peyto\Desktop\Mavs_Assignement\0021801216_pbp_Q3.xml"
Q3_tree = ET.parse(Q3_XML)
Q3_root = Q3_tree.getroot()
###   Q4   ###
Q4_XML =r"C:\Users\peyto\Desktop\Mavs_Assignement\0021801216_pbp_Q4.xml"
Q4_tree = ET.parse(Q4_XML)
Q4_root = Q4_tree.getroot()

#############################################
"""
BOX Score Retrival 
"""
#############################################
"""
Create way to get the Playerboxscore
"""
BXS_data = dict()
for child in BXS_root:
    for x in child.iter('Player_stats'):
            BXS_data[(x)]= x.attrib
"""
Merge all information into one DF
"""
BXS_data_List = [ v for k,v in BXS_data.items()] 
BoxscoreDF = pd.DataFrame.from_dict(BXS_data_List, orient='columns')
#############################################
"""
Q1 Retrival 
"""
#############################################
Q1_data = dict()
for child in Q1_root:
    for x in child.iter('Event_pbp'):
           Q1_data[(x)]= x.attrib
"""
Merge all information into one DF
"""
Q1_data_List = [ v for k,v in Q1_data.items()] 
Q1DF = pd.DataFrame.from_dict(Q1_data_List, orient='columns')
#############################################
#############################################
Q2_data = dict()
for child in Q2_root:
    for x in child.iter('Event_pbp'):
           Q2_data[(x)]= x.attrib
"""
Merge all information into one DF
"""
Q2_data_List = [ v for k,v in Q2_data.items()] 
Q2DF = pd.DataFrame.from_dict(Q2_data_List, orient='columns')
#############################################
"""
Q3 Retrival 
"""
#############################################
Q3_data = dict()
for child in Q3_root:
    for x in child.iter('Event_pbp'):
           Q3_data[(x)]= x.attrib
"""
Merge all information into one DF
"""
Q3_data_List = [ v for k,v in Q3_data.items()] 
Q3DF = pd.DataFrame.from_dict(Q3_data_List, orient='columns')
#############################################
"""
Q4 Retrival 
"""
#############################################
Q4_data = dict()
for child in Q4_root:
    for x in child.iter('Event_pbp'):
           Q4_data[(x)]= x.attrib
"""
Merge all information into one DF
"""
Q4_data_List = [ v for k,v in Q4_data.items()] 
Q4DF = pd.DataFrame.from_dict(Q4_data_List, orient='columns')
#############################################
"""
Merge All play by Plays and start substitution logic
"""
#############################################

from datetime import datetime

FullGame = pd.concat([Q1DF,Q2DF,Q3DF,Q4DF,], ignore_index=True)
#Set Teams
FullGame['homeTeam'] = '1610612742'
FullGame['awayTeam'] = '1610612756'
FullGame['Time_left_in_period'] = FullGame['Game_clock']
#Set Starting Line up
FullGame['awayPlayer1'] = '1629059'
FullGame['awayPlayer2'] = '1628367'
FullGame['awayPlayer3'] = '1628969'
FullGame['awayPlayer4'] = '1627733'
FullGame['awayPlayer5'] = '1629034'
FullGame['homePlayer1'] = '1629029'
FullGame['homePlayer2'] = '2734'
FullGame['homePlayer3'] = '1628382'
FullGame['homePlayer4'] = '1717'
FullGame['homePlayer5'] = '203939'
#Final Table
final_Table = FullGame
#############################################
"""

## Create Flag for the substitution ##

"""
#############################################

final_Table["SUB_IS_TRUE"] = final_Table.apply(
    lambda row: ','.join ([
        sub
        for sub in ["Substitution"]
        if sub.upper() in row['Description'].upper()
        ]), axis = 1
)
final_Table["PHX_IS_TRUE"] = final_Table.apply(
    lambda row: ','.join ([
        sub
        for sub in ["PHX"]
        if sub.upper() in row['Description'].upper()
        ]), axis = 1
)
final_Table["DAL_IS_TRUE"] = final_Table.apply(
    lambda row: ','.join ([
        sub
        for sub in ["DAL"]
        if sub.upper() in row['Description'].upper()
        ]), axis = 1
)

#############################################
"""

## Build Set to use the repeat numpy function

"""
#############################################

query1="""
select 
*
,Case when SUB_IS_TRUE='Substitution' and awayPlayer1=Person_id then Person_id2 else 0 end as awayPlayer1_T
,Case when SUB_IS_TRUE='Substitution' and awayPlayer2=Person_id then Person_id2 else 0 end as awayPlayer2_T
,Case when SUB_IS_TRUE='Substitution' and awayPlayer3=Person_id then Person_id2 else 0 end as awayPlayer3_T
,Case when SUB_IS_TRUE='Substitution' and awayPlayer4=Person_id then Person_id2 else 0 end as awayPlayer4_T
,Case when SUB_IS_TRUE='Substitution' and awayPlayer5=Person_id then Person_id2 else 0 end as awayPlayer5_T
,Case when SUB_IS_TRUE='Substitution' and homePlayer1=Person_id then Person_id2 else 0 end as homePlayer1_T
,Case when SUB_IS_TRUE='Substitution' and homePlayer2=Person_id then Person_id2 else 0 end as homePlayer2_T
,Case when SUB_IS_TRUE='Substitution' and homePlayer3=Person_id then Person_id2 else 0 end as homePlayer3_T
,Case when SUB_IS_TRUE='Substitution' and homePlayer4=Person_id then Person_id2 else 0 end as homePlayer4_T
,Case when SUB_IS_TRUE='Substitution' and homePlayer5=Person_id then Person_id2 else 0 end as homePlayer5_T
From 
final_Table
"""
Final_table_1 =sqldf(query1)

condition = Final_table_1['Event_num'].astype(int)== 2
Final_table_1.loc[condition, 'awayPlayer1_T'] = '1629059'
Final_table_1.loc[condition, 'awayPlayer2_T'] = '1628367'
Final_table_1.loc[condition, 'awayPlayer3_T'] = '1628969'
Final_table_1.loc[condition, 'awayPlayer4_T'] = '1627733'
Final_table_1.loc[condition, 'awayPlayer5_T'] = '1629034'
Final_table_1.loc[condition, 'homePlayer1_T'] = '1629029'
Final_table_1.loc[condition, 'homePlayer2_T'] = '2734'
Final_table_1.loc[condition, 'homePlayer3_T'] = '1628382'
Final_table_1.loc[condition, 'homePlayer4_T'] = '1717'
Final_table_1.loc[condition, 'homePlayer5_T'] = '203939'
#############################################
"""

## apply numpy repeat set for the function above ##

"""
#############################################
Final_table_1['awayPlayer1_T'] = Final_table_1['awayPlayer1_T'].replace(0,np.nan).ffill().astype(int)
Final_table_1['awayPlayer2_T'] = Final_table_1['awayPlayer2_T'].replace(0,np.nan).ffill().astype(int)
Final_table_1['awayPlayer3_T'] = Final_table_1['awayPlayer3_T'].replace(0,np.nan).ffill().astype(int)
Final_table_1['awayPlayer4_T'] = Final_table_1['awayPlayer4_T'].replace(0,np.nan).ffill().astype(int)
Final_table_1['awayPlayer5_T'] = Final_table_1['awayPlayer5_T'].replace(0,np.nan).ffill().astype(int)
Final_table_1['homePlayer1_T'] = Final_table_1['homePlayer1_T'].replace(0,np.nan).ffill().astype(int)
Final_table_1['homePlayer2_T'] = Final_table_1['homePlayer2_T'].replace(0,np.nan).ffill().astype(int)
Final_table_1['homePlayer3_T'] = Final_table_1['homePlayer3_T'].replace(0,np.nan).ffill().astype(int)
Final_table_1['homePlayer4_T'] = Final_table_1['homePlayer4_T'].replace(0,np.nan).ffill().astype(int)
Final_table_1['homePlayer5_T'] = Final_table_1['homePlayer5_T'].replace(0,np.nan).ffill().astype(int)
#############################################
"""

## turn ints into strings ##

"""
#############################################
Final_table_1['awayPlayer1_T'] = Final_table_1['awayPlayer1_T'].astype(str)
Final_table_1['awayPlayer2_T'] = Final_table_1['awayPlayer2_T'].astype(str)
Final_table_1['awayPlayer3_T'] = Final_table_1['awayPlayer3_T'].astype(str)
Final_table_1['awayPlayer4_T'] = Final_table_1['awayPlayer4_T'].astype(str)
Final_table_1['awayPlayer5_T'] = Final_table_1['awayPlayer5_T'].astype(str)
Final_table_1['homePlayer1_T'] = Final_table_1['homePlayer1_T'].astype(str)
Final_table_1['homePlayer2_T'] = Final_table_1['homePlayer2_T'].astype(str)
Final_table_1['homePlayer3_T'] = Final_table_1['homePlayer3_T'].astype(str)
Final_table_1['homePlayer4_T'] = Final_table_1['homePlayer4_T'].astype(str)
Final_table_1['homePlayer5_T'] = Final_table_1['homePlayer5_T'].astype(str)
#############################################
"""

## Create 10 man min running total ##

"""
#############################################
##Create way to sum time ##
Final_table_1['Game_clock_min'] = Final_table_1['Game_clock'].str[0:2]
Final_table_1['Game_clock_sec'] = Final_table_1['Game_clock'].str[3:]
Final_table_1['Game_clock_min'] = Final_table_1['Game_clock_min'].astype(int)
Final_table_1['Game_clock_sec'] = Final_table_1['Game_clock_sec'].astype(float)
Final_table_1['Game_clock_min'] = Final_table_1['Game_clock_min']*60
Final_table_1['Game_clock_as_sec'] = Final_table_1['Game_clock_sec']+Final_table_1['Game_clock_min']
Final_table_1['sec_left_period'] = 720-Final_table_1['Game_clock_as_sec']
Final_table_1['sec_left_period_dif']= Final_table_1['sec_left_period'].diff()
condition = Final_table_1['sec_left_period_dif']== -720
Final_table_1.loc[condition, 'sec_left_period_dif'] =0
## aggregate by unique 10 man team ##
Final_table_1['agg_key'] = Final_table_1['awayPlayer1_T']+Final_table_1['awayPlayer2_T']+Final_table_1['awayPlayer3_T']+Final_table_1['awayPlayer4_T']+Final_table_1['awayPlayer5_T']+Final_table_1['homePlayer1_T']+Final_table_1['homePlayer2_T']+Final_table_1['homePlayer3_T']+Final_table_1['homePlayer4_T']+Final_table_1['homePlayer5_T']
## what ending total should be ##
Final_table_1['sec_per_group'] = Final_table_1.groupby(['agg_key']).sec_left_period_dif.transform(np.sum)
## now make it running ##
Final_table_1['sec_per_group_running'] = Final_table_1.groupby(['agg_key']).sec_left_period_dif.transform(np.cumsum)
## Start period with a Zero for formatting ##
condition = Final_table_1['Event_num'].astype(int)== 2
Final_table_1.loc[condition, 'sec_left_period_dif'] = 0
Final_table_1.loc[condition, 'min_per_group_running'] = 0
#############################################
"""

## Start formatting for final result ##

"""
#############################################
## Convert seconds as integers to MM:SS formatting  ##
Final_table_1['min_running'] = pd.to_datetime(Final_table_1['sec_per_group_running'], unit='s').dt.strftime("%M:%S")
Final_table_1['min_per_group'] = pd.to_datetime(Final_table_1['sec_per_group'], unit='s').dt.strftime("%M:%S")


Final_table_1.reset_index(inplace=True)
Final_table_1 = Final_table_1.rename(columns = {'index':'Record_id'})
Formatted_Final_table = Final_table_1[['Record_id','','','','','','','','','','','','','','']]







