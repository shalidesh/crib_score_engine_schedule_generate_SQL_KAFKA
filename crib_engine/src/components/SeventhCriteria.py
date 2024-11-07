import sys
from src.logger import logging
from src.exception import CustomException
from src.components.sql.execute_sql import execute_sql

class SeventhCriteria:
    def __init__(self,help_id:str,mysql):


        self.table_name1='cdb_cdpu_crib_status'
        self.table_name2='crib_settled_credit_facilities'

        self.crib_status = execute_sql(help_id,mysql,self.table_name1)
        self.credit_facillities = execute_sql(help_id,mysql,self.table_name2)

        self.crib_status['FACILITY_AMOUNT']=self.crib_status['FACILITY_AMOUNT'].astype(float)
        self.credit_facillities['YEAR_1_TOT_AMOUNT_GRANTED']=self.credit_facillities['YEAR_1_TOT_AMOUNT_GRANTED'].astype(float)
        self.credit_facillities['YEAR_2_TOT_AMOUNT_GRANTED']=self.credit_facillities['YEAR_2_TOT_AMOUNT_GRANTED'].astype(float)
       


    def calculate_seventh_score(self):

        logging.info("Entered the calculate seventh criteria score calculation")
        
        try:
            filtered = self.credit_facillities[self.credit_facillities['OWNERSHIP'] == 'As Borrower']

            if not filtered.empty:

                total_granted_amount_y1 = filtered['YEAR_1_TOT_AMOUNT_GRANTED'].sum()
                total_granted_amount_y2 = filtered['YEAR_2_TOT_AMOUNT_GRANTED'].sum()

                facility_amount = self.crib_status['FACILITY_AMOUNT']
                total_granted_amount =total_granted_amount_y1+total_granted_amount_y2
                score = (total_granted_amount/facility_amount)*100

                # Extract the scalar value from the Series
                score_value = score.values[0]
              
                if score_value >= 100:
                    crib_sc = 1
                elif 50 <= score_value < 100:
                    crib_sc = 0.5
                elif 0 < score_value <= 50:
                    crib_sc = 0.1
                else:
                    crib_sc = 0.1

                score=crib_sc*0.05*100
            else:
                score=1*0.05*100
                
            print("7th score",score)
            return(
                score
            )
        except Exception as e:
            raise CustomException(e,sys)
        

