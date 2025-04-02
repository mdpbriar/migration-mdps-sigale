from datetime import datetime

class DateUtils:

    @staticmethod
    def convert_dateproeco_to_date(date_proeco:int) -> datetime|None:
        if date_proeco <= 0 or date_proeco >= 2559999:
            return None
        datestring = str(date_proeco)
        day = int(datestring[-2:])
        month = int(datestring[-4:-2])
        year = int(datestring[:-4]) + 1900
        return datetime(year, month, day)