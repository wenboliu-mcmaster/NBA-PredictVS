from datetime import datetime, timedelta

yesterday_datetime = datetime.now() - timedelta(days=1)

yesterday_date = yesterday_datetime.strftime('%Y%m%d')

print(yesterday_date)