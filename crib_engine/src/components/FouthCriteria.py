
import sys
from src.logger import logging
from src.exception import CustomException
import pandas as pd
from src.components.sql.execute_sql import execute_sql


class FouthCriteria:
    def __init__(self,help_id:str,mysql):

        self.cf_stat = ['ACTV', 'LGAL', 'TRMN', 'RSTR', 'R&SL', 'WRTN', 'MGCP']
        self.own_shp = ['GRT','001','JNT']
        self.cf_type = ['OVDR']
        self.output_data = []

        self.table_name1='crib_credit_fac_dtl'
        self.table_name2='crib_potential_current_liabilities'

        self.credit_facility_detail = execute_sql(help_id,mysql,self.table_name1)
        self.current_liabilities = execute_sql(help_id,mysql,self.table_name2)

        self.current_liabilities['TOT_AMOUNT_GRANTED']=self.current_liabilities['TOT_AMOUNT_GRANTED'].astype(float)

    def get_assigned_values(self,row):
        cf_status = row['CF_Stat']
        own_shp_status = row['Own_Shp']
        cf_type_value = row['CF_Type']
        id = row['Running_NIC']

        if cf_status in self.cf_stat and own_shp_status in self.own_shp:
            if cf_type_value in self.cf_type:
                # If CF_Type is OVDR, get the value from the Current_Balance column
                self.output_data.append({'RUNNING_NIC': id, 'Amount_Granted_Limit': row['Current_Balance']})
            else:
                # If CF_Type is not OVDR, get the value from the Amount_Granted_Limit column
                self.output_data.append({'RUNNING_NIC': id, 'Amount_Granted_Limit': row['Amount_Granted_Limit']})

        else:
            self.output_data.append({'RUNNING_NIC':"999", 'Amount_Granted_Limit': 999})

            
    def calculate_fouth_score(self):

        logging.info("Entered the calculate fouth criteria score calculation")
        
        try:
            # Example usage with the sample DataFrame
            self.credit_facility_detail.apply(self.get_assigned_values, axis=1)

            # Convert the list of dictionaries to a DataFrame
            output_df = pd.DataFrame(self.output_data)

            # Display the resulting DataFrame
            fillterID = output_df.RUNNING_NIC.unique()

            # Filtering df2 based on output_df['No']
            filtered_df2 = self.current_liabilities[self.current_liabilities['RUNNING_NIC'].isin(fillterID)]
           
            if not filtered_df2.empty:

                result = filtered_df2.groupby(['OWNERSHIP'])['TOT_AMOUNT_GRANTED'].sum().reset_index()

                # Calculate sums for 'As Borrower' and 'As Guarantor'
                sum_as_borrower = result[result.OWNERSHIP == 'As Borrower'].TOT_AMOUNT_GRANTED.sum()
                sum_as_guarantor = result[result.OWNERSHIP == 'As Guarantor'].TOT_AMOUNT_GRANTED.sum()

                # Check for division by zero
                if sum_as_borrower == 0:
                    ratio = 100  # or handle the case appropriately
                else:
                    # Calculate the ratio
                    ratio = (sum_as_guarantor / sum_as_borrower)
                    
                if ratio <= 0.5:
                    crib04 = 1
                elif 0.5 < ratio < 0.75:
                    crib04 = 0.5
                elif 0.75 < ratio < 1:
                    crib04 = 0
                else:
                    crib04 = -0.25

                score=crib04*0.05*100
            
            else:
                score=1*0.05*100

            if (self.current_liabilities.empty) & ( self.credit_facility_detail.empty) :
                score =0

            print("4th score",score)
            
            return(
                score
            )
        except Exception as e:
            raise CustomException(e,sys)
        


  