
import sys
from src.components.sql.execute_sql import execute_sql
from src.exception import CustomException
from src.logger import logging
import pandas as pd


class NinethCriteria:
    def __init__(self,help_id:str,mysql):

        self.nine_filtered_rows=[]
        self.nine_modified_rows=[]

        self.table_name1='cdb_cdpu_crib_status'
        self.table_name2='crib_credit_fac_dtl'

        self.nine_crib_status = execute_sql(help_id,mysql,self.table_name1)
        self.nine_facility_detail = execute_sql(help_id,mysql,self.table_name2)

        self.nine_facility_detail['Arrears_Amount']=self.nine_facility_detail['Arrears_Amount'].astype(float)
      
        self.CF_Stat = ['ACTV','MGCP','LGAL','TRMN','RSTR','WRTN','R&SL']
        self.Own_Ship = ['OWN','JNT','001','1',1,'002','2',2]



    def get_assigned_values(self,row):
      
        if  (row['CF_Stat'] in self.CF_Stat) and(row['Own_Shp'] in self.Own_Ship):
            Arrears_Amount = float(row['Arrears_Amount'])
            Installment_Amount = float(row['Installment_Amount'])
            if (Arrears_Amount > 0)&(Installment_Amount>0):
                CF_Type = row['CF_Type']
                if CF_Type in ['OVDR', 'CRCD']:
                    Rental_Value = row['Arrears_Amount']
                else:
                    Rental_Value = Installment_Amount
                row['Rental_Value'] = Rental_Value
                return row[['newID', 'Help_ID', 'CF_Type', 'CF_Stat', 'Arrears_Amount', 'Installment_Amount', 'Rental_Value']]
            else:
                Rental_Value = Arrears_Amount
                row['Rental_Value'] = Rental_Value
                return row[['newID', 'Help_ID', 'CF_Type', 'CF_Stat', 'Arrears_Amount', 'Installment_Amount', 'Rental_Value']]
        else:
            Rental_Value =0
       
        
    def calculate_arease_months(self,row):
        Arrears_Amount = float(row['Arrears_Amount'])
        Rental_Value = float(row['Rental_Value'])
        
        # Calculate Arease_Months
        if Rental_Value == 0:
            Arease_Months = 0
        else:
            Arease_Months = Arrears_Amount / Rental_Value
        
        # Append the row with added 'Arease_Months' column to the list
        row['Arease_Months'] = Arease_Months
        self.nine_modified_rows.append(row)

        

    def calculate_nineth_score(self):
      
        logging.info("Entered the calculate nineth criteria score calculation")
        
        try:
            self.nine_facility_detail['newID'] = self.nine_facility_detail['Running_NIC'].astype(str) + self.nine_facility_detail['No'].astype(str)
            self.nine_facility_detail = self.nine_facility_detail[['newID'] + [col for col in self.nine_facility_detail.columns if col != 'newID']]

            for _, row in self.nine_facility_detail.iterrows():
                nine_filtered_row = self.get_assigned_values(row)
                if nine_filtered_row is not None:
                    self.nine_filtered_rows.append(nine_filtered_row)

            filtered_df9 = pd.DataFrame(self.nine_filtered_rows, columns=['newID', 'HELP_ID', 'CF_Type', 'CF_Stat', 'Arrears_Amount', 'Installment_Amount', 'Rental_Value'])
            if not filtered_df9.empty:

                filtered_df9.reset_index(drop=True, inplace=True)

                for index, row in filtered_df9.iterrows():  
                    self.calculate_arease_months(row)

                filtered_df9 = pd.DataFrame(self.nine_modified_rows)

                filtered_df9.reset_index(drop=True, inplace=True)

                Tot_Arrears_Amount = filtered_df9.Arrears_Amount.sum()
                Tot_Rental_Value = filtered_df9.Rental_Value.sum()

                if(Tot_Arrears_Amount==0)&(Tot_Rental_Value==0):
                    score_value = 0
                else:
                    score_value = (Tot_Arrears_Amount/Tot_Rental_Value)

                if 0 <= score_value <= 1:
                    crib_sc = 1
                elif 1 < score_value <= 2:
                    crib_sc = 0.5
                elif 2 < score_value <= 3:
                    crib_sc = -0.5
                elif 3 < score_value <= 6:
                    crib_sc = -1
                else:
                    crib_sc = -2

                score=crib_sc*0.1*100
            
            else:
                score=1*0.1*100

            print("9th score",score)

            return(
                score
            )
        except Exception as e:
            raise CustomException(e,sys)
        

