import sys
from src.logger import logging
from src.exception import CustomException
from src.components.sql.execute_sql import execute_sql

class SixthCriteria:
    def __init__(self,help_id:str,mysql):

        self.table_name1='cdb_cdpu_crib_status'
        self.table_name2='crib_potential_current_liabilities'

        self.crib_status = execute_sql(help_id,mysql,self.table_name1)
        self.current_liabillities = execute_sql(help_id,mysql,self.table_name2)

        self.current_liabillities['TOT_AMOUNT_GRANTED']=self.current_liabillities['TOT_AMOUNT_GRANTED'].astype(float)
        self.crib_status['FACILITY_AMOUNT']=self.crib_status['FACILITY_AMOUNT'].astype(float)

        
    def calculate_sixth_score(self):

        logging.info("Entered the calculate sixth criteria score calculation")
        
        try:
            
            filtered = self.current_liabillities[self.current_liabillities['OWNERSHIP'] == 'As Borrower']

            if not filtered.empty:

                total_granted_amount = filtered['TOT_AMOUNT_GRANTED'].sum()

                facility_amount = self.crib_status['FACILITY_AMOUNT']
                score = facility_amount.div(total_granted_amount) * 100

                score_value = score.values[0]

                # Assuming 'score' is a column in your DataFrame
                if score_value < 100:
                    crib_sc = 1
                elif 100 <= score_value < 150:
                    crib_sc = 0.75
                elif 150 <= score_value <= 200:
                    crib_sc = 0.5
                elif 200 < score_value < 250:
                    crib_sc = 0.0
                else:
                    crib_sc = -0.5

                score=crib_sc*0.1*100

            else:
                score=1*0.1*100

            print("6th score",score)
            return(
                score
            )
        except Exception as e:
            raise CustomException(e,sys)
        
