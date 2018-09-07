from src.general_functions import GeneralFunctions
from emails.send_emails import Email
import pandas as pd
from sys import argv

class CombineData(Email,GeneralFunctions):

    def __init__(self):
        super().__init__()

    def _update_previous_point(self,symbol_id):
        try:
            max_date = str(self.get_value('max_date_cd',symbol_id=symbol_id))
            update_query = self._format_query('queries/combined_data_update_last.sql',
            replacements={'{symbol_id}':symbol_id,'{date}':max_date,'{modified_date}':pd.Timestamp.now()})
            self.cursor.execute(update_query)
            self.conn.commit()
        except Exception as e :
            self.conn.rollback()


    def compile_data(self,symbol_id):
        '''
        Combines data from ihub message boards, and stock price history
        '''

        self._update_previous_point(symbol_id)
        query_file = 'combined_data'
        df = self.get_df(query_file,symbol_id=symbol_id)
        self.to_table(df, 'model.combined_data')

if __name__ == '__main__':
    cd = CombineData()
    # df = cd.compile_data(2)
    symbol_ids = cd.get_list('symbol_ids')
    # grp1 = [x for x in symbol_ids if not x%4]
    # grp2 = [x for x in symbol_ids if not (x+1)%4]
    # grp3 = [x for x in symbol_ids if not (x+2)%4]
    # grp4 = [x for x in symbol_ids if not (x+3)%4]
    # for symbol_id in eval(argv[1]):
    for symbol_id in symbol_ids:
        print(symbol_id)
        df = cd.compile_data(symbol_id)



    # symbol_ids = cd.get_list('symbol_ids')
    # for symbol_id in symbol_ids:
    #     print(symbol_id)
    #     cd.compile_data(symbol_id)
