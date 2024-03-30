import json
import datetime
import os
from mpi4py import MPI


# Initialize MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Define dictionaries to store aggregated data
happy_hour_dict = {}
happy_day_dict = {}
active_hour_dict = {}
active_day_dict = {}

# Merge dictionaries
def merge_dicts(dict1, dict2):
    for key, value in dict2.items():
        dict1[key] = dict1.get(key, 0) + value
    return dict1

# Extract parameters from JSON data
def get_params(twitter_data):
    hour = twitter_data.get('doc').get('data').get('created_at')[:13]
    day = twitter_data.get('doc').get('data').get('created_at')[:10]
    sentiment = twitter_data.get('doc').get('data').get('sentiment')
    return hour, day, sentiment

# Convert hour to time range
def hour_to_range(hour):
    hour_dt = datetime.datetime.strptime(hour, "%Y-%m-%dT%H")
    start_hour = hour_dt.strftime("%I%p")
    end_hour_dt = hour_dt + datetime.timedelta(hours=1)
    end_hour = end_hour_dt.strftime("%I%p")
    return f"{start_hour}-{end_hour}"

# Calculate the corresponding segment of file
total_bytes = os.path.getsize('./twitter-100gb.json')
each_bytes = total_bytes // size
begin_position = rank * each_bytes
end_position = (rank + 1) * each_bytes
current_position = begin_position



begin_time = datetime.datetime.now()

# Process JSON data
with open('./twitter-100gb.json', 'r', encoding='utf-8') as twitter_file:
        
    twitter_str = ''
    twitter_file.seek(begin_position)
    # Skip first line
    twitter_file.readline()
        
    while (twitter_str := twitter_file.readline()) != '{}]}\n':

        if current_position > end_position:
            break

        twitter = json.loads(twitter_str[:-2])
        hour, day, sentiment = get_params(twitter)

        # Update dictionaries
        active_hour_dict[hour] = active_hour_dict.get(hour, 0) + 1
        active_day_dict[day] = active_day_dict.get(day, 0) + 1

        if sentiment and isinstance(sentiment, float):
            happy_hour_dict[hour] = happy_hour_dict.get(hour, 0) + sentiment
            happy_day_dict[day] = happy_day_dict.get(day, 0) + sentiment

        # Update current position
        #current_position += len(twitter_str)
        current_position = twitter_file.tell()

# Gather data from all processes
dict_list_list = comm.gather([happy_hour_dict, happy_day_dict, active_hour_dict, active_day_dict], root=0)

# Merge gathered results
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

    # Find the happiest and most active hours and days
    happiest_hour = max(happy_hour_dict, key=happy_hour_dict.get)
    happiest_hour_sentiment = happy_hour_dict[happiest_hour]
    happiest_day = max(happy_day_dict, key=happy_day_dict.get)
    happiest_day_sentiment = happy_day_dict[happiest_day]
    most_active_hour = max(active_hour_dict, key=active_hour_dict.get)
    most_active_hour_twitter_num = active_hour_dict[most_active_hour]
    most_active_day = max(active_day_dict, key=active_day_dict.get)
    most_active_day_twitter_num = active_day_dict[most_active_day]

    # Format the output
    happiest_hour_range = hour_to_range(happiest_hour)
    most_active_hour_range = hour_to_range(most_active_hour)
        
    # Output results
    print(f"{happiest_hour_range} on {happiest_hour[:10]} is the happiest hour with an overall sentiment score of {happiest_hour_sentiment}")
    print(f"{happiest_day} is the happiest day  with an overall sentiment score of {happiest_day_sentiment}")
    print(f"{most_active_hour_range} on {most_active_hour[:10]} is the most active hour with {most_active_hour_twitter_num} tweets")
    print(f"{most_active_day} is the most active day with {most_active_day_twitter_num} tweets")

    finish_time = datetime.datetime.now()
    print(f"Time taken: {finish_time - begin_time}")


