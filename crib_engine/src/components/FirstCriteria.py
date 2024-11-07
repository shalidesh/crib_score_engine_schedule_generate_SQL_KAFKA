
import sys
from src.logger import logging
from src.exception import CustomException
import pandas as pd
import numpy as np
import warnings
from src.components.sql.execute_sql import execute_sql

# Ignore FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)


class FirstCriteria:
    def __init__(self,help_id:str,mysql):

        self.output_data=[]
        self.cf_stat = ['ACTV', 'LGAL', 'TRMN', 'RSTR', 'R&SL', 'WRTN', 'CLSD','MGCP']
        self.own_shp = ['OWN', 'JNT', '001','1',1,2,'002','2']
        self.cf_type = ['OVDR']

        self.bins = [0, 30, 60, 90, 120, float('inf')]
        self.labels = ['0 - 30', '31 - 60', '61 - 90', '91 - 120', 'Above 121']

        self.months_list = ['MONTH1','MONTH2', 'MONTH3', 'MONTH4', 'MONTH5', 'MONTH6']

        self.weightages = {'0 - 30': 1, '31 - 60': 0.6, '61 - 90': -1, '91 - 120': -2, 'Above 121': -2.5}
       
        self.table_name1='cdb_cdpu_crib_status'
        self.table_name2='crib_24m_credit_fac'
        self.table_name3='crib_credit_fac_dtl'

        self.crib_status = execute_sql(help_id,mysql,self.table_name1)
        self.credit_facility = execute_sql(help_id,mysql,self.table_name2)
        self.facility_detail = execute_sql(help_id,mysql,self.table_name3)

        self.facility_detail['Amount_Granted_Limit']=self.facility_detail['Amount_Granted_Limit'].astype(float)


    def get_assigned_values(self,row):
        cf_status = row['CF_Stat']
        own_shp_status = row['Own_Shp']
        cf_type_value = row['CF_Type']
        id = row['newID']

        if cf_status in self.cf_stat and own_shp_status in self.own_shp:
            if cf_type_value in self.cf_type:
                # If CF_Type is OVDR, get the value from the Current_Balance column
                self.output_data.append({'newID': id, 'Amount_Granted_Limit': row['Current_Balance']})
            else:
                # If CF_Type is not OVDR, get the value from the Amount_Granted_Limit column
                self.output_data.append({'newID': id, 'Amount_Granted_Limit': row['Amount_Granted_Limit']})
                
        else:
            self.output_data.append({'newID':"999", 'Amount_Granted_Limit':0})

    # Function to count values in each bin for a row
    def count_in_bins(self,row):
       
        # Convert strings with value "OK" to integer 1 and others to integers
        row_numeric = [-1 if val is None else val for val in row]
        row_numeric = [1 if val == 'OK' else val for val in row_numeric]
        row_numeric = [1 if val == 'Cls' else val for val in row_numeric]
        row_numeric = [x for x in row_numeric]

        counts = pd.cut(row_numeric, bins=self.bins, labels=self.labels).value_counts()
        return counts.reindex(self.labels, fill_value=0)


    # Function to check if all values in count_columns are null for a row
    def check_null_values(self,row):
        return row[self.months_list].isna().all()


    def calculate_first_score(self):


        logging.info("Entered the calculate first criteria score calculation")
        try:
            credit_facility=self.credit_facility.copy()
            facility_detail=self.facility_detail.copy()

            #create Primery key for identify the each facillity of the specific customer(credit_facility dataset)
            credit_facility['newID'] = credit_facility['RUNNING_NIC'].astype(str) + credit_facility['NO'].astype(str)
            credit_facility = credit_facility[['newID'] + [col for col in credit_facility.columns if col != 'newID']]
            
            #create Primery key for identify the each facillity of the specific customer (facility_detail dataset)
            facility_detail['newID'] = facility_detail['Running_NIC'].astype(str) + facility_detail['No'].astype(str)
            facility_detail = facility_detail[['newID'] + [col for col in facility_detail.columns if col != 'newID']]

            #select only 6 months data
            selected_columns = ['newID', 'NO', 'HELP_ID', 'REQ_NIC', 'RUNNING_NIC',
                    'MONTH1', 'MONTH2', 'MONTH3', 'MONTH4', 'MONTH5', 'MONTH6',
                    ]

            credit_facility = credit_facility[selected_columns]

            #select facillities that fullfill the requirements
            facility_detail.apply(self.get_assigned_values, axis=1)

            output_df = pd.DataFrame(self.output_data)
        
            filtered_df2 = credit_facility[credit_facility['newID'].isin(output_df['newID'])].copy()

            if not filtered_df2.empty:
                
                month6_dt = filtered_df2[self.months_list]

                # If not empty, execute the code
                filtered_df2.loc[:, 'count_1'] = month6_dt.count(axis=1)
                filtered_df2.loc[:, 'count_2'] = filtered_df2.apply(lambda row: row.str.count('OK').sum(), axis=1)
            
                ## Removing the null columns
                df_1=filtered_df2[self.months_list]
            
                #put each facillities in to appropriatr bins
                basket_counts = df_1.apply(self.count_in_bins, axis=1)
                filtered_df2.loc[:, self.labels] = basket_counts

                count_1 = filtered_df2['count_1']
                filtered_df2.loc[:, self.labels] = filtered_df2[self.labels].div(count_1, axis=0)

                #apply weightage for each facillity
                for label, weightage in self.weightages.items():
                    filtered_df2.loc[:, label] *= weightage

                #merge the datasets
                merged_df = pd.merge(filtered_df2, output_df, on='newID', how='left')

                merged_df["Amount_Granted_Limit"] = np.where(merged_df.apply(self.check_null_values, axis=1), 0, merged_df["Amount_Granted_Limit"])

                for label in self.labels:
                    merged_df.loc[:, label] = merged_df[label] * merged_df['Amount_Granted_Limit']

                merged_df['Total'] = merged_df[self.labels].sum(axis=1)

                Tot = merged_df.Total.sum()
                
                tot_Amount_Granted = merged_df.Amount_Granted_Limit.sum()
                
                if tot_Amount_Granted ==0:
                    score=None

                else:
                    p1_scr = (Tot/tot_Amount_Granted) * 100
                    score=p1_scr*0.25

            else:
                score = 0*100
            print("1st score",score)
            return(
                score
            )
        except Exception as e:
            raise CustomException(e,sys)

        
