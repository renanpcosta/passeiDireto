""" import libraries """ 
import pandas as pd
import numpy as np
from unicodedata import normalize
import re
import sqlalchemy as db

""" Path where are the necessary files """
inputPath = 'C:/Users/renan/Documents/passeiDireto/BASE A/'

"""
Load files by selecting the columns to be used
Convert column values to text
Rename columns to make the joins more easy
""" 
df_courses = pd.read_json(inputPath+'courses.json')[['Id','Name']].applymap(str).rename(columns={'Id':'CourseId', 'Name':'CourseName'})
df_sessions = pd.read_json(inputPath+'sessions.json')[['StudentId','SessionStartTime','StudentClient']].applymap(str).rename(columns={'StudentClient':'SessionStudentClient'})
df_students = pd.read_json(inputPath+'students.json')[['Id','State','City','UniversityId','CourseId','SignupSource','StudentClient']].applymap(str).rename(columns={'Id':'StudentId'})
df_subscriptions = pd.read_json(inputPath+'subscriptions.json')[['StudentId','PaymentDate','PlanType']].applymap(str)
df_universities = pd.read_json(inputPath+'universities.json')[['Id','Name']].applymap(str).rename(columns={'Id':'UniversityId', 'Name':'UniversityName'})

"""create list with dataframes for operations common to all """
listDataframe = [df_courses, df_sessions, df_students, df_subscriptions, df_universities]

"""
Removes accents and lower case the string
Removes multiple spaces
@param value String value that will be cleaned 
@return String cleaned
"""
def cleanString(value):
    value = re.sub(' +', ' ', value)
    return normalize('NFKD', value).encode('ASCII', 'ignore').decode('ASCII').upper().strip()

"""
Applies functions for normalizing columns and values that are common to all
@param dataframe Dataframe to be normalized
@return Dataframe normalized
"""
def normalizeColumnsValues(dataframe):
    for column in dataframe.columns:
        dataframe[column] = dataframe[column].apply(cleanString)
    return dataframe

""" Splits the SessionStartTime column into one with the date and one with the time """
dfAux = df_sessions['SessionStartTime'].str.split(' ', n=1, expand=True)
df_sessions['SessionStartDate'] = dfAux[0]
df_sessions['SessionStartTime'] = dfAux[1]
del dfAux

""" Create period when started the session """
df_sessions['SessionStartPeriod'] = np.select([(df_sessions['SessionStartTime'].str.split(':', n=1, expand=True)[0].isin(['06','07','08','09','10','11'])), (df_sessions['SessionStartTime'].str.split(':', n=1, expand=True)[0].isin(['12','13','14','15','16','17']))], ['MANHA','TARDE'],'NOITE') 

""" Take only the date from the PaymentDate column """
df_subscriptions['PaymentDate'] = df_subscriptions['PaymentDate'].str.split(' ', n=1, expand=True)[0]

""" Remove version and OS infos """
df_students['StudentClient'] = df_students['StudentClient'].str.split('|', n=1, expand=True)[0]

""" Apply normalizeColumnsValues and remove duplicates in all dataframes """
for dataframe in listDataframe:
    dataframe = normalizeColumnsValues(dataframe)
    dataframe.drop_duplicates(inplace=True)

""" Makes the necessary joins and deletes dataframes that will no longer be used to free memory """
dfJoined = df_students.merge(df_universities, how='left', on=['UniversityId'])
del df_students, df_universities

dfJoined = dfJoined.merge(df_courses, how='left', on=['CourseId'])
del df_courses

dfJoined = dfJoined.merge(df_subscriptions, how='left', on=['StudentId'])
del df_subscriptions

dfJoined = dfJoined.merge(df_sessions, how='left', on=['StudentId'])
del df_sessions

dfJoined.drop(['UniversityId','CourseId'], axis=1, inplace=True)

""" BD path to save files """
outputPath = 'sqlite:///C:/Users/renan/Documents/passeiDireto/pd_database.sqlite3'

""" Creates connection """
engine = db.create_engine(outputPath, echo=False)
con = engine.connect()

dfJoined.to_sql('test_first_part', engine, if_exists='replace', index=False, chunksize=500)