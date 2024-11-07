import pandas as pd
from sqlalchemy import text

def execute_sql(help_id, db_session, table_name, schema_name='cdbdw_prod'):
    try:
        # Create the SQL query with schema name
        sql_query = text(f"SELECT * FROM {schema_name}.{table_name} WHERE HELP_ID = :help_id")

        # Execute the query
        result = db_session.execute(sql_query, {'help_id': help_id})

        # Fetch all the rows
        data = result.fetchall()

        # Get the column names
        column_names = result.keys()

        # Create a DataFrame from the fetched data
        dataframe = pd.DataFrame(data, columns=column_names)
        
        return dataframe
    except Exception as e:
        print(f"Error executing SQL: {str(e)}")
        return None
    

def update_crib_status(help_id, db_session, schema_name='cdbdw_prod'):
    try:
        # Create the SQL query with schema name
        sql_query = text(f"UPDATE {schema_name}.cdb_cdpu_crib_status SET BOT_STATUS = 'COMPLETED' WHERE HELP_ID = :help_id")

        # Execute the query
        db_session.execute(sql_query, {'help_id': help_id})

        # Commit the transaction
        db_session.commit()

        return "Success", 200
    
    except Exception as e:
        return str(e), 400
