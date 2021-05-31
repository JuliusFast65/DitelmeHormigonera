from database_fetcher import DatabaseFetcher
from sender import LoopSender
from time import time
from datetime import datetime,timedelta

class SignalHandler:

    def __init__(self):
        self.signal = None
        self.last_updated_date = None
        self.last_sent_orders = None

    def process(self,date_str):
        try:

            orders = DatabaseFetcher.retrieve_orders(date_str)

            # db_data,self.last_updated_date = DatabaseFetcher.retrieve_tickets(date_str,last_updated_date=self.last_updated_date)
            #
            # sender = LoopSender()
            # sender.send_data(db_data)
            #
            # now = int(time())
            #
            # if self.last_sent_orders is None or (now - self.last_sent_orders) > 10*60:
            #     print("Sending Orders")
            #     orders = DatabaseFetcher.retrieve_orders(date_str)
            #     sender.send_data(orders,method='orders')
            #
            #     self.last_sent_orders = now

        except KeyError:
            raise Exception("Invalid parameters")
        except Exception as e:
            raise e