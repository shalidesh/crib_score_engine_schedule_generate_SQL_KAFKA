import sys
from src.logger import logging
from src.exception import CustomException
import pandas as pd
import numpy as np
from src.components.sql.execute_sql import execute_sql

class ThirdCriteria:
    def __init__(self,help_id:str,mysql):

        self.cf_stat = ['ACTV', 'LGAL', 'TRMN', 'RSTR', 'R&SL', 'WRTN', 'MGCP' ,'CLD']
        self.own_shp = ['GRT']
        self.cf_type = ['OVDR']
        self.output_data = []

        self.months6 = ['MONTH1', 'MONTH2', 'MONTH3', 'MONTH4', 'MONTH5',
                'MONTH6']
        
        self.bins = [0, 30, 60, 90, 120, float('inf')]
        self.labels = ['0 - 30', '31 - 60', '61 - 90', '91 - 120', 'Above 121']

        # Define weightages
        self.weightages = {'0 - 30': 1, '31 - 60': 0.75, '61 - 90': 0, '91 - 120': -0.5, 'Above 121': -1}

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

        df=(self.credit_facility
            .dropna(subset=self.months6, how='all'))
     
        No_List= (df['RUNNING_NIC'].astype(str) + df['NO'].astype(str)).tolist()
        if (cf_status in self.cf_stat and own_shp_status in self.own_shp) & (str(id) in No_List) :
        
            if (cf_type_value in self.cf_type):
                # If CF_Type is OVDR, get the value from the Current_Balance column
                self.output_data.append({'newID': id, 'Amount_Granted_Limit': row['Current_Balance']})
            else:
                # If CF_Type is not OVDR, get the value from the Amount_Granted_Limit column
                self.output_data.append({'newID': id, 'Amount_Granted_Limit': row['Amount_Granted_Limit']})
                     
        else:
            self.output_data.append({'newID':"999", 'Amount_Granted_Limit': 0})


    # Function to count values in each bin for a row
    def count_in_bins(self,row):
        # Replace "OK" with a value that falls into the '0-30' bin
        row_numeric = [-1 if val is None else val for val in row]
        row_numeric = [1 if val == 'OK' else val for val in row_numeric]
        row_numeric = [1 if val == 'Cls' else val for val in row_numeric]
        row_numeric = [x for x in row_numeric]
        counts = pd.cut(row_numeric, bins=self.bins, labels=self.labels).value_counts()
        return counts.reindex(self.labels, fill_value=0)
    
    def check_null_values(self,row):
        return row[self.months6].isna().all()

    def calculate_third_score(self):

        logging.info("Entered the calculate third criteria score calculation")
        
        try:
            if not (self.credit_facility.empty and self.facility_detail.empty):
                self.credit_facility['newID'] = self.credit_facility['RUNNING_NIC'].astype(str) + self.credit_facility['NO'].astype(str)
                self.credit_facility = self.credit_facility[['newID'] + [col for col in self.credit_facility.columns if col != 'newID']]

                self.facility_detail['newID'] = self.facility_detail['Running_NIC'].astype(str) + self.facility_detail['No'].astype(str)
                self.facility_detail = self.facility_detail[['newID'] + [col for col in self.facility_detail.columns if col != 'newID']]

                selected_columns = ['newID', 'NO', 'HELP_ID', 'REQ_NIC', 'RUNNING_NIC',
                        'MONTH1', 'MONTH2', 'MONTH3', 'MONTH4', 'MONTH5', 'MONTH6',
                        ]
                
                self.credit_facility = self.credit_facility[selected_columns]

                # Example usage with the sample DataFrame
                self.facility_detail.apply(self.get_assigned_values, axis=1)

                # Convert the list of dictionaries to a DataFrame
                output_df = pd.DataFrame(self.output_data)

                filtered_df2 = (
                    self.credit_facility[self.credit_facility['newID']
                                        .isin(output_df['newID'])].copy()
                                        .dropna(subset=self.months6, how='all')
                    )
                
                if not filtered_df2.empty:

                    month6_dt = filtered_df2[self.months6]

                    filtered_df2['count_1'] = month6_dt.count(axis=1)

                    filtered_df2['count_2'] = filtered_df2.apply(lambda row: row.str.count('OK').sum(), axis=1)

                    basket_counts = filtered_df2[self.months6].apply(self.count_in_bins, axis=1)

                    # Add separate columns for each basket
                    filtered_df2[self.labels] = basket_counts

                    # Count of each basket divided by count_1
                    count_1 = filtered_df2['count_1']

                    filtered_df2[self.labels] = filtered_df2[self.labels].div(count_1, axis=0)

                    # Multiply values by weightages
                    for label, weightage in self.weightages.items():
                        filtered_df2[label] *= weightage

                    merged_df = pd.merge(filtered_df2, output_df, on='newID', how='left')

                    # Apply the condition row-wise
                    merged_df["Amount_Granted_Limit"] = np.where(merged_df.apply(self.check_null_values, axis=1), 0, merged_df["Amount_Granted_Limit"])

                    ## Multiply the result by 'Amount_Granted_Limit'
                    for label in self.labels:
                        merged_df[label] = merged_df[label] * merged_df['Amount_Granted_Limit']

                    # Calculate the total for each row
                    merged_df['Total'] = merged_df[self.labels].sum(axis=1)

                    Tot = merged_df.Total.sum()
                    
                    tot_Amount_Granted = merged_df.Amount_Granted_Limit.sum()

                    p1_scr = (Tot/tot_Amount_Granted)*100

                    score=p1_scr*0.05
                
                else:
                    score=1*0.05*100
            else:
                score = 5

            print("3rd score",score)
            return(
                score
            )
        except Exception as e:
            raise CustomException(e,sys)
        


