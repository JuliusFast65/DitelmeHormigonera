import time
import traceback
from datetime import datetime, timedelta
from processor import SignalHandler


def sync(handler:SignalHandler):
    try:

        now = datetime.now()

        date_str = now.strftime("%Y-%m-%d")
        handler.process(date_str)

        print("Sincronização feita: ", datetime.now().strftime("%d/%m/%Y %H:%M"))

    except Exception as e:
        traceback.print_exc()


if __name__ == '__main__':

    handler = SignalHandler()
    # sync(handler)

    while True:
        sync(handler)
        time.sleep(60)
