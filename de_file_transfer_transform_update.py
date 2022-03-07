import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import date, timedelta, datetime
import psycopg2
import boto3
import simplejson

pd.set_option('display.max_rows',1000)
pd.set_option('display.max_columns',1000)
pd.set_option('display.width',1000)

ACCESS_KEY='AKIA4JI5PYRPZ4OPAGWQ'
SECRET_KEY='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'

engine=create_engine('postgresql://postgresDB:pg_database_032022@database-1.c4tzhg3p8qh6.ap-south-1.rds.amazonaws.com:5432/postgres')

def lambda_handler(event,context):
    
    try:
        print(f'event: {event}')
        bucket = event['bucket']
        file_name = event['file_name']
        print(f'The bucket is: {bucket}')
        print(f'The file_name is: {file_name}')
        # return {"status": "success", "message": None}
        
        if not 'bucket' in event and not 'file_name' in event:
        
            print(event)
            return {
                'statusCode': 500,
                'body': json.dumps('Error: Please specify all parameters (bucket and file_name).')
            }
        else:
            
            getBucket, getFileName = getDetails(event)
            # df=getFileFromS3(getBucket, getFileName)
            executor(getBucket, getFileName)


            return {
                    'statusCode': 200,
                    'bucket': bucket,
                    'file_name': file_name
                    }
     
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps('Error: Please specify all parameters (bucket and file_name).')
        }
        raise e

def getDetails(event):
    
    print("The requested bucket is: ", event['bucket'])
    print("The requested file_name is: ", event['file_name'])
    
    return(str(event['bucket']), str(event['file_name']))

def getFileFromS3(bucket, file_name):
    
    s3_client=boto3.client('s3',aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    s3 = boto3.resource('s3',aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

    AWS_s3_BUCKET = bucket
    FILE_KEY=file_name
    
    response = s3_client.get_object(Bucket=AWS_s3_BUCKET,Key=FILE_KEY)
    status = response.get("ResponseMetadata",{}).get("HTTPStatusCode")
    
    if status == 200:
        
        print(f"Successfule S3 get_object response. Status - {status}")
#         df=pd.read_csv(response.get("Body"))
        df=pd.read_json(response.get("Body"))

        obj=s3.Object(AWS_s3_BUCKET,FILE_KEY)
        data=obj.get()['Body'].read().decode('utf-8')
#         json_data=json.loads(data)
        json_data=simplejson.loads(data)

        return df, json_data
    else:
        print(f"Unsuccessfule S3 get_object response. Status - {status}")

def tranfomData(bucket, file_name):
    
    df=pd.read_json(file_name)
    
    with open(file_name,'r') as f:
        data=simplejson.loads(f.read())
    
    # df_nested_list=pd.json_normalize(data,record_path=['statusLogs'])   
    df_nested_list=pd.json_normalize(data,record_path=['statusLogs'], meta=['userId'])    
    df_nested_list
    
    dfpiv= df_nested_list.pivot(index=['userId'],columns='status',values=['updatedOn'])
    dfpiv2=dfpiv.droplevel(level=0,axis=1)
    dfpiv3=dfpiv2.reset_index()
    dfpiv4=dfpiv3.rename_axis(None, axis=1)
    dfpiv4['userId']=pd.to_numeric(dfpiv4['userId'])
    df2=df[['userId','createdAt','lastVisitedAt','status','isBlacklisted']]
    
    df3=pd.merge(df2,dfpiv4, how='left',on='userId')
    df3['createdAt']=pd.to_datetime(df['createdAt'],format="%Y-%m-%dT%H:%M:%S").dt.tz_localize(None)
    
    df3['lastVisitedAt']=pd.to_datetime(df3['lastVisitedAt'],unit='s')
    df3['INITIATED']=pd.to_datetime(df3['INITIATED'],unit='s')
    df3['PENDING_VERIFICATION']=pd.to_datetime(df3['PENDING_VERIFICATION'],unit='s')
    df3['VERIFIED']=pd.to_datetime(df3['VERIFIED'],unit='s')
    
    df3=df3.rename({'userId':'user_id',
                'createdAt':'created_at',
                'lastVisitedAt':'last_created_at',
                'isBlacklisted':'is_black_listed',
                'INITIATED':'initiated',
                'PENDING_VERIFICATION':'pending_verification',
                'VERIFIED':'verified'}, axis=1)
    
    chunk_size=int(df3.shape[0]/1)
    print(df3.shape[0], chunk_size)

    table_name=bucket
    
    for start in range(0, df3.shape[0], chunk_size):
        df_subset = df3.iloc[start:start+chunk_size]
        print(df_subset.shape[0], start)
        df_subset.to_sql(table_name,con=engine,if_exists='append',index=False)
    
    print("write completed")

def update_staging_using_procedure():
    
    print("Calling the 'update_staging_from_pre_processing_collection' procedure to move data from preprocessing to staging")
 
    procedure_call= """call update_staging_sample_details();"""
    conn = psycopg2.connect(user='postgresDB', password='pg_database_032022', host='database-1.c4tzhg3p8qh6.ap-south-1.rds.amazonaws.com', database='postgres')
    cursor = conn.cursor()
    cursor.execute(procedure_call)
    conn.commit()
    print("Update completed!")

def empty_preprocessing_table_after_staging_update():
   
    print("Cleaning the preprocessing table after the staging layer has been updated")
    truncate_table = """ TRUNCATE sample_details CASCADE; """
    conn = psycopg2.connect(user='postgresDB', password='pg_database_032022', host='database-1.c4tzhg3p8qh6.ap-south-1.rds.amazonaws.com', database='postgres')
    cursor = conn.cursor()
    cursor.execute(truncate_table)
    conn.commit()
    print("Table cleaning complete")

def executor(getBucket, getFileName):

    df_1, json_data =getFileFromS3(getBucket, getFileName)
    print("************* File Read ****************")

    tranfomData('sample_details', df_1, json_data)
    print("************* Transformation Completed Read ****************")

    update_staging_using_procedure()
    print("************* Stored Procedure Called and Updated ****************")

    empty_preprocessing_table_after_staging_update()
    print("************* Preprocessing Table Cleared ****************")
