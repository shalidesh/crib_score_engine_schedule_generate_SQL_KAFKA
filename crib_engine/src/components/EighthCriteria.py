import sys
from src.logger import logging
from src.exception import CustomException
import pandas as pd
from datetime import datetime, timedelta
from src.components.sql.execute_sql import execute_sql


class EighthCriteria:


    def __init__(self,help_id:str,mysql):

        self.table_name='crib_dishonour_chq_dtls'
        self.crib_dishouner8 = execute_sql(help_id,mysql,self.table_name)
        self.crib_dishouner8['Date_Dishonoured'] = pd.to_datetime(self.crib_dishouner8['Date_Dishonoured'])
  
    def calculate_eighth_score(self):

        logging.info("Entered the calculate eighth criteria score calculation")
        
        try:
            crib_sc=1
            current_date = datetime.now()
            six_month = current_date - timedelta(days=180)

            if not self.crib_dishouner8.empty:
                self.crib_dishouner9 = self.crib_dishouner8[self.crib_dishouner8['Date_Dishonoured'] >= six_month]
                count_dishonor_cheque = self.crib_dishouner9.Cheque_Number.count()
                if 1 <= count_dishonor_cheque < 10:
                    crib_sc = 0.5
                elif 10 <= count_dishonor_cheque < 20:
                    crib_sc = -0.5
                elif 20 <= count_dishonor_cheque < 50:
                    crib_sc = -0.75
                elif 50 <= count_dishonor_cheque:
                    crib_sc = -1.5

                score = crib_sc*0.1*100

            else:
                score = crib_sc*0.1*100
            
            print("8th score",score)
            
            return(
                score
            )
        except Exception as e:
            raise CustomException(e,sys)
        

