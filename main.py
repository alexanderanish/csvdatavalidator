#import fastapi, file and upload
import datetime
from fastapi import FastAPI, File, UploadFile
#import pandas
import pandas as pd

from db import engine, SessionLocal, UploadStatus
from sqlalchemy.types import VARCHAR
from sqlalchemy import text
import sqlalchemy as sa
from sqlalchemy import or_, and_, not_

# Create an instance of FastAPI

app = FastAPI()

#create a route to upload a csv or excel file
@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile = File(...), module: str = "GL", load_id: str = None):
    
    #create a id based on the current datetime
    id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    #connver the id into an integed
    id = int(id)

    if load_id == None:
        load_id = id

    #to do encoding filtering and parsing
    if file.content_type == "text/csv":
        #use pandas to load the file into a dataframe using the chunks method
        df = pd.read_csv(file.file, chunksize=1000)
    elif file.content_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        #use pandas to load the file into a dataframe using the chunks method
        df = pd.read_excel(file.file, chunksize=1000)
    elif file.content_type == "application/vnd.ms-excel":
        #use pandas to load the file into a dataframe using the chunks method
        df = pd.read_excel(file.file, chunksize=1000)
    else:
        return {"Error": "File type must be csv or excel"}

    #number of item in df

    count=0
    table_name = file.filename.split(".")[0].replace(" ", "_")
    columns =[]
    #df = pd.read_excel(file.file, chunksize=1000)
#    for help in df:
#        print(help, "help")

    try:
        for idx, data in enumerate(df):
            #write to a mysql table using the to_sql method
            print("Start loading with file {}, itr {}, count {}".format(table_name, idx, count))
            data.to_sql(name=f"tmp_{table_name}_{id}", con=engine, if_exists="append", index=True, dtype={col_name: VARCHAR(225) for col_name in data})
            count += len(data)
            #need to optimize column getting
            columns = list(data.columns)
            print("ping iteration {} of {} with file {}".format(idx, count, table_name))
        db = SessionLocal()
        
        upload_status = UploadStatus(tablename=f"tmp_{table_name}_{id}", filename=table_name, row_count=count, status="file uploaded", status_id=1, columns=str(columns), module=module, load_id=f'tmp_{load_id}')
        db.add(upload_status)
        db.commit()
        db.refresh(upload_status)

        with engine.connect() as conn:
            query = f"ALTER TABLE tmp_{table_name}_{id} ADD error_flag INT(1) DEFAULT 0;"
            resultAddFlag = conn.execute(text(query))
            query = f"ALTER TABLE tmp_{table_name}_{id} ADD error_details longtext;"
            resultAddDetails = conn.execute(text(query))
            conn.commit()

        return {"id": upload_status.id, "filename": f"tmp_{table_name}_{id}", "count": {count}}
    except Exception as e:
        return {"Error": f"Failed to load data into the database table tmp_{table_name}_{id} at {idx} of {count}. error:{e}"}
        #return the file name


#create an api route to upload two files and load them into their own tables
@app.post("/uploadfiles/")
async def create_upload_files(file1: UploadFile = File(...), file2: UploadFile = File(...)):
    #create a id based on the current datetime
    id = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    #connver the id into an integed
    id = int(id)

    #to do need to optimize id for look up later
    try:
        file1_upload = await create_upload_file(file1,"AP", id)
    except Exception as e:
        return {"Error": f"Failed to upload file1. error:{e}"}
    
    try:
        file2_upload = await create_upload_file(file2,"AP", id)
    except Exception as e:
        return {"Error": f"Failed to upload file2. error:{e}"}
    
    return {"Status":"Success","file1": file1_upload, "file2": file2_upload}
    

#create an api route to get the status of the upload
@app.get("/uploadstatus/{id}")
async def get_upload_status(id: int):
    db = SessionLocal()
    upload_status = db.query(UploadStatus).filter(UploadStatus.id == id).first()
    if upload_status:
        return {"id": upload_status.id, "filename": upload_status.filename, "count": upload_status.row_count, "status": upload_status.status, "status_id": upload_status.status_id, "columns": upload_status.columns, "module": upload_status.module, "load_id": upload_status.load_id, "table_name": upload_status.tablename}
    else:
        return {"Error": "No record found"}

#create an api route to check from a list of input columns which ones have null or empty values and the counts
@app.post("/check_null_columns/")
#inputs should be upload id and a list of columns
async def check_null_columns(id: int, columns: list):
    #create a list to hold the results
    results = []

    #get the table name from the upload status table
    db = SessionLocal()
    upload_status = db.query(UploadStatus).filter(UploadStatus.id == id).first()
    table_name = upload_status.tablename

    #loop through the columns
    for column in columns:
        try:
            #check if the column exists in the table
            print("Start Loop for column ", column)
            with engine.connect() as conn:
                query = f"SELECT COUNT(*) FROM {table_name} WHERE `{column}` IS NULL or trim(`{column}`) = '';"
                result = conn.execute(text(query))
                #get the count number
                count = result.fetchone()[0]
                #update the table with the flag and details

                query1 = f"UPDATE {table_name} SET error_flag = 1, error_details = concat(coalesce(error_details,'') , ',' , '{column}_null') WHERE `{column}` IS NULL or trim(`{column}`) = '';"
                resultUpdate = conn.execute(text(query1))
                #print(resultUpdate)
                conn.commit()


            #count = engine.execute(query).fetchone()[0]
            #append the results to the list
                results.append({"Column_Name": column, "Null_Count": count})
            print("End Loop for column ", column)
        except Exception as e:
            return {"Error": f"Failed to get the count of null values for {column}, error: {e}"}

        

    #return the results
    return results

#create a route to check for data format based on regex checks in the db and update the table with the flag and details
@app.post("/check_data_format/")
#inputs should be upload id and a list of columns
async def check_data_format(id: int, columns: list, check_type: str):
    #create a list to hold the results
    results = []

    #get the table name from the upload status table
    db = SessionLocal()
    upload_status = db.query(UploadStatus).filter(UploadStatus.id == id).first()
    table_name = upload_status.tablename

    #list of checks - alphanumeric, numeric, date, email, phone, url
    #regex for numbers
    numberCheck = "^[0-9]+$"
    #regex for alphanumeric
    alphaNumCheck = "^[a-zA-Z0-9]+$"
    #regex for date for dd-mm-yy or dd/mm/yy or dd.mm.yy
    dateTimeCheck = "^[0-9]{2}-[0-9]{2}-[0-9]{4} [0-9]{2}:[0-9]{2}$"
    dateCheck = "^[0-9]{2}-[0-9]{2}-[0-9]{4}$"
    timeCheck = "^[0-9]{2}:[0-9]{2}$"
    #add regex for single and double deciumal float
    floatCheck = "^[0-9]+(\.[0-9]+)?$"


    if not check_type:
        return {"Error": "Check type is required"}
    
    if check_type == "number":
        check = numberCheck
    elif check_type == "alphanumeric":
        check = alphaNumCheck
    elif check_type == "date":
        check = dateCheck
    elif check_type == "datetime":
        check = dateTimeCheck
    elif check_type == "time":
        check = timeCheck
    elif check_type == "float":
        check = floatCheck
    else:
        return {"Error": f"Check type {check_type} is not supported"}

    #loop through the columns
    for column in columns:
        try:
            #check if the column exists in the table
            print("Start Loop for column ", column)
            with engine.connect() as conn:
                #write the query to get the count of rows that do not match the regex

                #to do add null exit or consider as check 

                query = f"SELECT COUNT(*) FROM {table_name} WHERE `{column}` NOT REGEXP  '{check}';"

                result = conn.execute(text(query))
                #get the count number
                count = result.fetchone()[0]
                #update the table with the flag and details)
                query1 = f"UPDATE {table_name} SET error_flag = 1, error_details = concat(coalesce(error_details,'') , ',' , '{column}_{check_type}') WHERE `{column}` NOT REGEXP  '{check}';"
                resultUpdate = conn.execute(text(query1))
                #print(resultUpdate)
                conn.commit()


            #count = engine.execute(query).fetchone()[0]
            #append the results to the list
                results.append({"Column_Name": column, "Format_Count": count})
            print("End Loop for column ", column)
        except Exception as e:
            return {"Error": f"Failed to get the count of null values for {column}, error: {e}"}

        

    #return the results
    return results


#create a route to check for conditional null checks based on two or more columns where if one column is null then the other column should not be null
@app.post("/check_conditional_null/")
#inputs should be upload id and two list of columns
async def check_conditional_null(id: int, columns: list, conditional_column: list):
    #create a list to hold the results
    results = []

    #get the table name from the upload status table
    db = SessionLocal()
    upload_status = db.query(UploadStatus).filter(UploadStatus.id == id).first()
    table_name = upload_status.tablename

    #loop through the columns
    for column in columns:
        try:
            #check if the column exists in the table
            with engine.connect() as conn:
                #write the query to get the count of rows that do not match the regex
                query = f"SELECT COUNT(*) FROM {table_name} WHERE ({column} IS NULL or trim({column}) = '') and ({conditional_column} IS NULL or trim({conditional_column}) = '');"
                result = conn.execute(text(query))
                #get the count number
                count = result.fetchone()[0]
                #update the table with the flag and details)
                query1 = f"UPDATE {table_name} SET error_flag = 1, error_details = concat(coalesce(error_details,'') , ',' , '{column}_{conditional_column}_conditional_null') WHERE ({column} IS NULL or trim({column}) = '') and ({conditional_column} IS NULL or trim({conditional_column}) = '');"
                resultUpdate = conn.execute(text(query1))
                #print(resultUpdate)
                conn.commit()


            #count = engine.execute(query).fetchone()[0]
            #append the results to the list
                results.append({"Column_Name": column, "Conditional_Null_Count": count})
        except Exception as e:
            return {"Error": f"Failed to get the count of null values for {column}, error: {e}"}

        

    #return the results
    return results

#create a route for conditional data checks when one column is a value defined in the input then the conditional columns should not be null
@app.post("/check_conditional_data/")
#inputs should be upload id and two list of columns
async def check_conditional_data(id: int, column: list, conditional_column: list, conditional_value: list):
    #create a list to hold the results
    results = []

    #get the table name from the upload status table
    db = SessionLocal()
    upload_status = db.query(UploadStatus).filter(UploadStatus.id == id).first()
    table_name = upload_status.tablename

    #loop through the columns
    for col in conditional_column:
        try:
            #check if the column exists in the table

            with engine.connect() as conn:
                #write the query to get the count of rows that do not match the regex
                query = f"SELECT COUNT(*) FROM {table_name} WHERE {column} in '{conditional_value}' and ({col} IS NULL or trim({col}) = '');"
                print(query, "query in conditional data")
                result = conn.execute(text(query))
                #get the count number
                count = result.fetchone()[0]
                #update the table with the flag and details)
                query1 = f"UPDATE {table_name} SET error_flag = 1, error_details = concat(coalesce(error_details,'') , ',' , '{column}_{col}_conditional_data') WHERE {column} = '{conditional_value}' and ({col} IS NULL or trim({col}) = '');"
                resultUpdate = conn.execute(text(query1))
                #print(resultUpdate)
                conn.commit()


            #count = engine.execute(query).fetchone()[0]
            #append the results to the list
                results.append({"Column_Name": column, "Conditional_Data_Count": count})
        except Exception as e:
            return {"Error": f"Failed to get the count of null values for {column}, error: {e}"}

        

    #return the results
    return results

#create a route that will allow the user to map source columns to target columns
@app.post("/map_columns/")
#inputs should be upload id and two list of columns
async def map_columns(id: int, source_columns: list, target_columns: list):
    #create a list to hold the results
    results = []

    #get the table name from the upload status table
    db = SessionLocal()
    upload_status = db.query(UploadStatus).filter(UploadStatus.id == id).first()
    table_name = upload_status.tablename

    #loop through the columns
    for source_column, target_column in zip(source_columns, target_columns):
        try:
            #check if the column exists in the table
            with engine.connect() as conn:
                #write the query to get the count of rows that do not match the regex
                query = f"UPDATE {table_name} SET {target_column} = {source_column};"
                print(query, "query in map columns")
                result = conn.execute(text(query))
                #get the count number
                #count = result.fetchone()[0]
                #update the table with the flag and details)
                #query1 = f"UPDATE {table_name} SET error_flag = 1, error_details = concat(coalesce(error_details,'') , ',' , '{column}_{col}_conditional_data') WHERE {column} = '{conditional_value}' and ({col} IS NULL or trim({col}) = '');"
                #resultUpdate = conn.execute(text(query1))
                #print(resultUpdate)
                conn.commit()


            #count = engine.execute(query).fetchone()[0]
            #append the results to the list
                results.append({"Source_Column": source_column, "Target_Column": target_column})
        except Exception as e:
            return {"Error": f"Failed to get the count of null values for {column}, error: {e}"}

        

    #return the results
    return results

#create a route to add a new temp table of the columns mapped by the user from source to target
@app.post("/create_staging_table/")
#inputs should be upload id and two list of columns
async def create_staging_table(id: int, source_columns: list, target_columns: list):
    #create a list to hold the results
    results = []

    #get the table name from the upload status table
    db = SessionLocal()
    upload_status = db.query(UploadStatus).filter(UploadStatus.id == id).first()
    table_name = upload_status.tablename

    #loop through the columns
    for source_column, target_column in zip(source_columns, target_columns):
        try:
            #check if the column exists in the table
            with engine.connect() as conn:
                #write the query that creates the staging table naming the columns as target columns and selecting from table as srouce columns
                query = f"CREATE TABLE {table_name}_staging AS SELECT {source_columns} FROM {table_name};"
                result = conn.execute(text(query))

                conn.commit()


            #count = engine.execute(query).fetchone()[0]
            #append the results to the list
                results.append({"Source_Column": source_column, "Target_Column": target_column})
        except Exception as e:
            return {"Error": f"Failed to get the count of null values for {column}, error: {e}"}

        

    #return the results
    return results

#run the app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)