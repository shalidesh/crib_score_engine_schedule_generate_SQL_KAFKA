from dataclasses import dataclass
from src.components.sql.execute_sql import execute_sql
from src.exception import CustomException
from src.logger import logging
import sys

class TenthCriteria:
    def __init__(self,help_id:str,mysql):
    
        self.cf_stat_C10 = ['LGAL', 'R&SL']
        self.wtrn= ['WRTN']
        self.ownership = ['001','JNT','1',1,2,'2','002']
        self.tot_written_off = 0
        self.score_value_2 = 0
        self.score_value_3 = 0
        self.table_name1='cdb_cdpu_crib_status'
        self.table_name2='crib_credit_fac_dtl'

        self.crib_status = execute_sql(help_id,mysql,self.table_name1)
        self.facility_details = execute_sql(help_id,mysql,self.table_name2)

        

    def calculate_tenth_score(self):
    
        logging.info("Entered the calculate tenth criteria score calculation")
        
        try:
            if not (self.facility_details.empty or self.crib_status.empty):
                # Iterate over each row of the dataframe
                count =0
                for _, row in self.facility_details.iterrows():
                    if row['CF_Stat'] in self.cf_stat_C10 and row['Own_Shp'] in self.ownership:
                        self.score_value_2 = -1

                    elif row['CF_Stat'] in self.wtrn and row['Own_Shp'] in self.ownership:
                        self.tot_written_off += float(row['Amount_Written_Off'])
                        facility_amount = (self.crib_status['FACILITY_AMOUNT'])
                        facility_amount = float(facility_amount.iloc[0])
                        score_value = self.tot_written_off/(facility_amount)
                        count = count + 1                         
                    else:
                        self.score_value_3 = -2

                facility_amount = (self.crib_status['FACILITY_AMOUNT'])
                facility_amount = float(facility_amount.iloc[0])

                if (self.score_value_2 == -1) & (self.tot_written_off==0) & (count ==0):
                    score_value = -1
                elif (self.score_value_3 == -2) & (self.tot_written_off==0)& (count ==0):
                    score_value = -2
            
                else:
                    score_value = self.tot_written_off/(facility_amount)

                if 0 <= score_value <= 0.2:
                    crib_sc = -0.25
                elif 0.2 <score_value <= 0.5:
                    crib_sc = -1
                elif score_value > 0.5:
                    crib_sc = -2
                elif (score_value == -2):
                    crib_sc = 1
                else:
                    crib_sc = -1  # Assign a default value when none of the conditions are met
                    
           
                score=crib_sc * 0.1*100

            else:
                score=1* 0.1*100
            print("10th score",score)
            return(
                    score
            )
        except Exception as e:
            raise CustomException(e,sys)
        

        
