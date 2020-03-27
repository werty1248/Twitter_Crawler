from TwitterCrawler import TweeterCrawling
import datetime

print("Collecting twitter data")
str = input("from (yymmdd) : ")
start = datetime.datetime.strptime("20" + str,"%Y%m%d")
str2 = input("to(include) (yymmdd) : ")
end = datetime.datetime.strptime("20" + str2,"%Y%m%d")
end = end + datetime.timedelta(days=1)

#절대경로도 가능
tc = TweeterCrawling(query_file = 'query.csv',data_dir = 'data')
while(start < end):
    tc.getSingleDay(start)
    start = start + datetime.timedelta(days=1)
print("collecting finish!")