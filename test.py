import json
import datetime

from mpi4py import MPI

begin_time = datetime.datetime.now()

#predefined variables
happy_hour_dict = {}
happy_day_dict = {}
active_hour_dict = {}
active_day_dict = {}

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


#merge dicts
def merge_dicts(dict1, dict2):
    for key, value in dict2.items():
        if key in dict1:
            dict1[key] += value
        else:
            dict1[key] = value

    return dict1

#analyze the json object, extract parameters needed
def get_params(twitter: json):
    data = twitter.get('doc').get('data')
    hour = data.get('created_at')[:13]
    day = data.get('created_at')[:10]
    sentiment = data.get('sentiment',None)

    return hour, day, sentiment


with open('./twitter-50mb.json','r',encoding='utf-8') as twitter_file:
    
    #skip first line
    twitter_file.readline()

    count = 0
    while (twitter_str := twitter_file.readline()) != '{}]}\n':
        count +=1
        if count % size != rank:
            continue
    
        twitter = json.loads(twitter_str[:-2])
        hour, day, sentiment = get_params(twitter)

        if hour not in active_hour_dict:
                active_hour_dict[hour] = 0
                happy_hour_dict[hour] = 0

        if day not in active_day_dict:
                active_day_dict[day] = 0
                happy_day_dict[day] = 0
    
        active_hour_dict[hour] += 1
        active_day_dict[day] += 1    

        if sentiment and isinstance(sentiment, float):
                happy_hour_dict[hour] += sentiment
                happy_day_dict[day] += sentiment


dict_list_list = comm.gather([happy_hour_dict, happy_day_dict, active_hour_dict, active_day_dict], root=0)

#merge gathered result

happy_hour_dict = {}
happy_day_dict = {}
active_hour_dict = {}
active_day_dict = {}


if rank == 0:
    for dict_list in dict_list_list:
        happy_hour_dict = merge_dicts(happy_hour_dict, dict_list[0])
        happy_day_dict = merge_dicts(happy_day_dict, dict_list[1])
        active_hour_dict = merge_dicts(active_hour_dict, dict_list[2])            
        active_day_dict = merge_dicts(active_day_dict, dict_list[3])
         

    #format output
    happiest_hour = list(dict(sorted(happy_hour_dict.items(), key=lambda item: item[1])).items())[-1][0]
    happiest_hour_sentiment = list(dict(sorted(happy_hour_dict.items(), key=lambda item: item[1])).items())[-1][1]
    happiest_day = list(dict(sorted(happy_day_dict.items(), key=lambda item: item[1])).items())[-1][0]
    happiest_day_sentiment = list(dict(sorted(happy_day_dict.items(), key=lambda item: item[1])).items())[-1][1]
    most_active_hour = list(dict(sorted(active_hour_dict.items(), key=lambda item: item[1])).items())[-1][0]
    most_active_hour_twitter_num = list(dict(sorted(active_hour_dict.items(), key=lambda item: item[1])).items())[-1][1]
    most_active_day = list(dict(sorted(active_day_dict.items(), key=lambda item: item[1])).items())[-1][0]
    most_active_day_twitter_num = list(dict(sorted(active_day_dict.items(), key=lambda item: item[1])).items())[-1][1]

    print(f"{happiest_hour} is the happiest hour with an overall sentiment score of {happiest_hour_sentiment}")
    print(f"{happiest_day} is the happiest day  with an overall sentiment score of {happiest_day_sentiment}")
    print(f"{most_active_hour} is the most active hour with {most_active_hour_twitter_num} tweets")
    print(f"{most_active_day} is the most active day with {most_active_day_twitter_num} tweets")  


    finish_time = datetime.datetime.now()
    print(finish_time - begin_time)

print(happy_hour_dict)