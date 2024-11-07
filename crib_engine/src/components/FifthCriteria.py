
import sys
from src.logger import logging
from src.exception import CustomException
import pandas as pd
from datetime import datetime, timedelta
from src.components.sql.execute_sql import execute_sql
import pandas as pd
from datetime import datetime, timedelta
import pytz


class FifthCriteria:
    def __init__(self,help_id:str,mysql):

        self.table_name1='crib_credit_fac_dtl'
        self.table_name2='crib_inq_made_6m_lending_ins'

        self.credit_facility_detail = execute_sql(help_id,mysql,self.table_name1)
        self.lending_inf = execute_sql(help_id,mysql,self.table_name2)

        self.lending_inf['Inquiry_Date'] = pd.to_datetime(self.lending_inf['Inquiry_Date'])
        self.credit_facility_detail['First_Disburse_Date'] = pd.to_datetime(self.credit_facility_detail['First_Disburse_Date'])

        
    def calculate_fifth_score(self):
        logging.info("Entered the calculate fifth criteria score calculation")

        if not self.lending_inf.empty:
            try:
            
                # Get the timezone
                tz = pytz.timezone('Asia/Kolkata')  # Use your specific timezone

                # Calculate the date for six months ago
                six_months_ago = datetime.now(tz) - timedelta(days=6*30)

                # Filter the DataFrame
                df5 = self.lending_inf[self.lending_inf['Inquiry_Date'] >= six_months_ago]
            
                
                if not df5.empty:

                    df5['Reason_lower'] = df5['Reason'].str.lower()
                    
                    # Sort the DataFrame
                    df5 = df5.sort_values(by='Inquiry_Date', ascending=False)
                

                    # Get the current date
                    current_date = datetime.now(tz)

                    # Calculate the difference in days
                    df5['Days_Since_Inquiry'] = (current_date - df5['Inquiry_Date']).dt.days
            

                    # Add a new column with the date after one month
                    df5['One_month_After'] = df5['Inquiry_Date'] + timedelta(days=30)

                    # Extract the date part and repeat it for each row
                    self.credit_facility_detail['Disburse_Date_Only'] = self.credit_facility_detail['First_Disburse_Date']
                    
                    self.credit_facility_detail['Disburse_Date_Only'] =pd.to_datetime(self.credit_facility_detail['Disburse_Date_Only'])
                    self.credit_facility_detail = self.credit_facility_detail.sort_values(by='Disburse_Date_Only', ascending=False)
    
                    # df5['One_month_After'] = pd.to_datetime(df5['One_month_After']).dt.strftime('%Y-%m-%d')
                
                    df5 = df5.sort_values(by='One_month_After', ascending=False)
                    # df5['Inquiry_Date']=df5['Inquiry_Date'].dt.strftime('%Y-%m-%d')

                    # Create a new column in df5 and initialize it with 0
                    df5['flag1'] = 0

                    #Iterate through each row in df5
                    for index, row in df5.iterrows():
                        # Check if 'Disburse_Date_Only' is between 'Inquiry_Date' and 'One_Week_After'
                        if any((self.credit_facility_detail['Disburse_Date_Only'] >= row['Inquiry_Date']) & 
                            (self.credit_facility_detail['Disburse_Date_Only']<= row['One_month_After'])&(row['Reason_lower'] == 'evaluating of a borrower for a new credit facility')):
                            df5.at[index, 'flag1'] = 1
                            
                        df5['Mark'] = df5.apply(lambda row: 1 if row['flag1'] == 1 and row['Reason_lower'] == 'evaluating of a borrower for a new credit facility' else 0, axis=1)

                    # Check if there is any value of -0.5 in the 'Mark' column
                    if (0 in df5['Mark'].values) &('evaluating of a borrower for a new credit facility' in df5['Reason_lower'].values):
                        final_score =-0.5*0.05*100
                    else:
                        final_score = 1* 0.05*100

                else:
                    final_score=1* 0.05*100
                print("5th score",final_score)
                return(
                    final_score
                )
            except Exception as e:
                raise CustomException(e,sys)
            
        else:
            final_score = 1* 0.05*100
            print("5th score",final_score)
            return(
                final_score
            )



  