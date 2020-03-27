import time
import datetime
import os
import pandas as pd
import GetOldTweets3 as got
import csv

class TweeterCrawling:
    def __init__(self,query_file,data_dir):
        self.query_table = self.set_query(query_file)
        self.RAW_DATA_DIRECTORY = data_dir
        
        #데이터가 저장될 디렉토리 생성
        if(os.path.isdir(self.RAW_DATA_DIRECTORY) == False):
            os.mkdir(self.RAW_DATA_DIRECTORY)
        for q in self.query_table:
            if(os.path.isdir(os.path.join(self.RAW_DATA_DIRECTORY,q[0])) == False):
                os.mkdir(os.path.join(self.RAW_DATA_DIRECTORY,q[0]))

    #쿼리가 저장된 파일을 읽어서, 트위터 고급검색 쿼리로 바꿔주는 함수
    def set_query(self,file_name):
      query_table = []
      f = open(file_name,"r",encoding='utf-8-sig')
      data = csv.DictReader(f)
      for table in data:
        plus = table['include']
        minus = table['except']
        
        #쿼리를 and 들의 or 집합으로 가정
        #ex) A or (B and C) or (D and E)
        # -> [A],[B,C],[D,E] 형태로 변환
        inc_word = plus.split("<or>")
        expt_word = minus.split("<or>")
        inc_word_table = []
        expt_word_table = []
        for words in inc_word:
            words = words.replace("\"","")
            words = words.replace(")","")
            words = words.replace("(","")
            inc_word_table.append(words.split("<and>"))
        for words in expt_word:
            words = words.replace("\"","")
            words = words.replace(")","")
            words = words.replace("(","")
            expt_word_table.append(words.split("<and>"))
        
        #("A") OR ("B" "C") OR ("D" "E") 형태로 트위터에 보낼 쿼리 문자열 작성
        q = ""
        for words in inc_word_table:
            q = q+"("
            for word in words:
                q = q+"\"" + word + "\" "
            q = q[:-1]+") OR "
        q = q[:-3]
        query_table.append([table['keyword'],q,inc_word_table,expt_word_table])
      return query_table

    def valid_check(self,include_word,except_word,text):
        #띄워쓰기를 무시하고 검색
        text = text.replace(" ","")

        #제외단어
        for words in except_word:
            check = True
            #and 로 묶인 제외단어들 모두가 포함되어있다면
            for word in words:
                if(not word in text):
                    check = False
            #잘못 가져온 텍스트
            if(check):
                return False
        #포함단어
        for words in include_word:
            check = True
            #and로 묶인 포함단어 중 모두가 포함되어있다면
            for word in words:
                if(not word in text):
                    check = False
            #제대로 가져온 텍스트
            if(check):
                return True
        #제외단어에도 걸리지 않고, 포함단어도 하나도 없다면, 잘못 가져온 텍스트
        return False
    def send_query(self,target_date,query):
        # 트위터 고급검색 시스템이 Until값 날짜의 트윗을 포함하지 않으므로, day + 1
        next_date = target_date + datetime.timedelta(days=1)
        
        tweetCriteria = got.manager.TweetCriteria().setQuerySearch(query)\
                                                    .setSince(target_date.strftime("%Y-%m-%d"))\
                                                    .setUntil(next_date.strftime("%Y-%m-%d"))
        # 수집 with GetOldTweet3
        return got.manager.TweetManager.getTweets(tweetCriteria)

    #동시에 수 일의 데이터를 요청할 경우, 가끔 트위터에서 차단하는 경우가 있으므로 하루 단위로 끊어서 검색함
    def getSingleDay(self,target_date):
        target_next_date = target_date + datetime.timedelta(days=1)

        print("Collecting tweeter data of {}...".format(target_date.strftime("%d-%b-%Y")))
        print("  Keyword     | Response data   | {}  | {}  | invalid data | running time".format(target_date.strftime("%Y-%m-%d"),target_next_date.strftime("%Y-%m-%d")))
        print("              |                 | 09:00~23:59 | 00:00~09:00 |              |")
        #소요시간 측정
        program_start = time.time()
        
        for q in self.query_table:
            print("{:8s}   | ".format(q[0]),end="")
            try:
                tweet = self.send_query(target_date,q[1])
            except:
                #가끔 아무 이유 없이 쿼리 전송이 실패하는 경우가 있음
                print("collecting fail... try again")
                print("{:8s}   | ".format(q[0]),end="")
                tweet = self.send_query(target_date,q[1])
            times = time.time() - program_start
                
            tweet_list = []
            for index in tweet:
                if(self.valid_check(q[2],q[3],index.text)):
                    # 트위터의 검색결과는 협정세계시(UTC 0)를 알려주므로, 한국 시간(UTC +9)에 맞게 변환
                    korean_time = index.date + datetime.timedelta(hours=9)

                    # 메타데이터 목록
                    permalink = index.permalink
                    content = index.text
                    tweet_date = korean_time.strftime("%Y-%m-%d")
                    tweet_time = korean_time.strftime("%H:%M:%S")
                    tweet_list.append([tweet_date,tweet_time,permalink,content])
                
            #data\분류명\YYYY_MM_DD.csv로 저장
            tweet_df = pd.DataFrame(tweet_list, columns = ["date", "time", "permalink", "text"])
            
            #날짜에 맞게 분류
            today_tweet = tweet_df[tweet_df['date'] == target_date.strftime("%Y-%m-%d")]
            tomorrow_tweet = tweet_df[tweet_df['date'] == (target_date + datetime.timedelta(days=1)).strftime("%Y-%m-%d")]
            
            today_dir = os.path.join(self.RAW_DATA_DIRECTORY,q[0],target_date.strftime("%Y_%m_%d")) + ".csv"
            tomorrow_dir = os.path.join(self.RAW_DATA_DIRECTORY,q[0],target_next_date.strftime("%Y_%m_%d")) + ".csv"
            
            #기존 데이터와 합치기
            final_data1 = self.merge_data(today_dir,today_tweet)
            final_data2 = self.merge_data(tomorrow_dir,tomorrow_tweet)

            print(" {:5d} tweets   | {:9d}   | {:9d}   | {:10d}   | {:2d}:{:02d}:{:02d}".format(len(tweet),len(today_tweet),len(tomorrow_tweet),len(tweet) - len(today_tweet) - len(tomorrow_tweet),int(times/3600),int(times/60)%60,int(times)%60))            
            
            final_data1.to_csv(today_dir,index=False)
            final_data2.to_csv(tomorrow_dir,index=False)
            
    def merge_data(self,dir,data):
        if(os.path.isfile(dir)):
            ex_data = pd.read_csv(dir)
            diff = data.loc[~data['permalink'].isin(ex_data['permalink'])]
            if(len(diff) != 0):
                ex_data = pd.concat([ex_data,diff],axis=0)
                ex_data = ex_data.sort_values(['date','time'],ascending=[False,False])
            data = ex_data
        return data

#속성, 로그파일 등을 읽어서 map 형태로 저장하는 함수
def FileToDict(file_name):
    f = open(file_name,'r')
    dictionary = dict()
    while True:
        text = f.readline()
        if not text: break
 
        #개행문자 제거 - 운영체제 및 인코딩 방식에 따라서 개행 문자를 \r\n 을 쓰기도, \n을 쓰기도 한다고 함
        text = text.replace("\r\n","")
        text = text.replace("\n","").split(":")
        dictionary[text[0]] = text[1]
    f.close()
    return dictionary

def DictToFile(dictionary,file_name):
    f = open(file_name,'w')
    for key, value in dictionary.items():
        f.write("{}:{}\n".format(key,value))
    f.close()

if __name__ == "__main__":
    #어제 오전 9시 ~ 오늘 오전 9시까지의 트위터를 수집
    end = datetime.datetime.today() - datetime.timedelta(days=1)

    property_file = "Crawler_properties.txt"
    log_file = "Crawler_log.txt"

    #속성, 로그 불러오기
    property = FileToDict(property_file)
    log = FileToDict(log_file)
    
    if 'finished' not in log.keys():
        log['finished'] = "2020-01-05"

    start = datetime.datetime.strptime(log['finished'],"%Y-%m-%d")
    tc = TweeterCrawling(query_file = property['query'],data_dir = property['directory'])

    print("Collecting from {} to {}".format(start.strftime("%d-%b-%Y"),end.strftime("%d-%b-%Y")))

    while(start < end):
        tc.getSingleDay(start)

        #업데이트된 로그를 저장
        log['finished'] = start.strftime("%Y-%m-%d")
        DictToFile(log,log_file)

        start = start + datetime.timedelta(days=1)
