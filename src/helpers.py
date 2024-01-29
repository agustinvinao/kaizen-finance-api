from datetime import datetime

def to_date(date_string, format="%Y-%m-%d"): 
    return datetime.strptime(date_string, format).date()