import json
import traceback
import requests


class LoopSender:

    def __init__(self,batch_max_size=250):
        self.api_path = "https://integration.readymix.io/api/sync/"
        self.company_token = "1287879a-bdfa-4f87-914a-0d00e395"
        self.batch_max_size = batch_max_size

    def send_data(self,body:list,method='tickets'):

        body_size = len(body)
        print("Data size = ",body_size)

        headers = {
            'token': self.company_token,
            'Content-Type': 'application/json'
        }

        batch_list = list(self._get_batch(body,self.batch_max_size))

        for batch in batch_list:
            print("Sending batch = ",len(batch))
            try:
                response = requests.post(self.api_path+method,json=batch,headers=headers)
                print(response.content)
            except Exception as e:
                print("Error on Send",str(e))

        print("done")

    def _get_batch(self,original_list:list,batch_size:int) -> list:
        for i in range(0,len(original_list),batch_size):
            batch = original_list[i:i + batch_size]
            yield batch





