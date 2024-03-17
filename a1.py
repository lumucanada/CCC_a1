import json
from datetime import datetime



with open("twitter_1mb.json","r") as f:
    chunks = []
    data = json.load(f)



# this is the 
#print(data['rows'][0]['doc']['data']['created_at'])
#print(data['rows'][998])

#loop through all the data and find all the sentiment of the objects

my_dict = {}
for i in range(len(data['rows'])):
    try:
        
        create_at = data['rows'][i]['doc']['data']['created_at']
        sentiment = data['rows'][i]['doc']['data']['sentiment']
        my_dict[create_at] = sentiment

        #a = "%(n)s  %(s)s" % {'n': create_at, 's': sentiment}
        #print(a)
        
        #print(type(sentiment))
    except:
        continue

#print(list(my_dict.keys())[0])
#print(list(my_dict.values())[0])

#print(my_dict)
time_str = "2021-06-21 03:18:59+00:00"
parsed_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S%z")

#print(parsed_time)

parsed_time = time_str.split('+')[0]
#print(parsed_time)

### this is the happiest hour

from collections import defaultdict

def calculate_happiest_hour(dic):
    """
    Calculate the happiest hour based on total sentiment in the given dictionary.

    Args:
        dic (dict): A dictionary containing timestamps as keys and sentiment values as values.

    Returns:
        tuple: A tuple containing the happiest hour range (e.g., '3-4pm'), the date, and the overall sentiment score.
    """
    # Dictionary to store total sentiment for each hour
    total_sentiments_per_hour = defaultdict(float)

    # Loop through the dictionary
    for timestamp, data in dic.items():
        # Check if the sentiment score is present and numeric
        if isinstance(data, (float, int)):
            sentiment = data
            # Extract hour and date from timestamp
            hour = int(timestamp.split('T')[1][:2])
            date = timestamp.split('T')[0]
            # Update total sentiment for the hour
            total_sentiments_per_hour[(date, hour)] += sentiment

    # Find the hour with the highest total sentiment
    happiest_date_hour, overall_sentiment = max(total_sentiments_per_hour.items(), key=lambda x: x[1])

    # Determine the hour range
    hour_range = f"{happiest_date_hour[1]}-{happiest_date_hour[1] + 1}pm"

    return hour_range, happiest_date_hour[0], overall_sentiment


def print_happiest_hour(hour_range, date, overall_sentiment):
    """
    Print out the happiest hour information in a human-readable format.

    Args:
        hour_range (str): The hour range (e.g., '3-4pm').
        date (str): The date of the happiest hour.
        overall_sentiment (float): The overall sentiment score.
    """
    if overall_sentiment > 0:
        sentiment_text = f"with an overall sentiment score of +{overall_sentiment}"
    elif overall_sentiment < 0:
        sentiment_text = f"with an overall sentiment score of {overall_sentiment}"
    else:
        sentiment_text = "with a neutral overall sentiment"

    print(f"{hour_range} on {date} {sentiment_text}")

def test_happiest_hour(any_dic):
    # Calculate happiest hour
    hour_range, date, overall_sentiment = calculate_happiest_hour(any_dic)

    # Print happiest hour information
    print_happiest_hour(hour_range, date, overall_sentiment)
    

test_happiest_hour(my_dict)

#this is for the happiest day ever
#
#
#
#





def calculate_happiest_day(dic):
    """
    Calculate the happiest day based on total sentiment in the given dictionary.

    Args:
        dic (dict): A dictionary containing timestamps as keys and sentiment values as values.

    Returns:
        tuple: A tuple containing the happiest day (e.g., '2023-05-25') and the overall sentiment score.
    """
    from collections import defaultdict

    # Dictionary to store total sentiment for each day
    total_sentiments_per_day = defaultdict(float)

    # Loop through the dictionary
    for timestamp, data in dic.items():
        # Check if the sentiment score is present and numeric
        if isinstance(data, (float, int)):
            sentiment = data
            # Extract date from timestamp
            date = timestamp.split('T')[0]
            # Update total sentiment for the day
            total_sentiments_per_day[date] += sentiment

    # Find the day with the highest total sentiment
    happiest_day, overall_sentiment = max(total_sentiments_per_day.items(), key=lambda x: x[1])

    return happiest_day, overall_sentiment


def print_happiest_day(happiest_day, overall_sentiment):
    """
    Print out the happiest day information in a human-readable format.

    Args:
        happiest_day (str): The happiest day in the format 'YYYY-MM-DD'.
        overall_sentiment (float): The overall sentiment score.
    """
    if overall_sentiment > 0:
        sentiment_text = f"with an overall sentiment score of +{overall_sentiment}"
    elif overall_sentiment < 0:
        sentiment_text = f"with an overall sentiment score of {overall_sentiment}"
    else:
        sentiment_text = "with a neutral overall sentiment"

    print(f"{happiest_day} was the happiest day with an overall sentiment score of {sentiment_text}")


def test_happiest_day(dic):
    # Calculate happiest day
    happiest_day, overall_sentiment = calculate_happiest_day(dic)

    # Print happiest day information
    print_happiest_day(happiest_day, overall_sentiment)




# Test the functions with the  dictionary
test_happiest_day(my_dict)


my_list = []
for i in range(len(data['rows'])):
    try:
        my_list.append(data['rows'][i]['doc']['data']['created_at'])
    except:
        continue
#print(my_list)
print(len(my_list))



### this is the most active hour


def calculate_most_active_hour(timestamps):
    """
    Calculate the most active hour based on the given list of timestamps.

    Args:
        timestamps (list): A list of timestamps.

    Returns:
        tuple: A tuple containing the most active hour (e.g., '2-3pm'), the date, and the count of elements in that hour.
    """
    from collections import Counter

    # Extract hours from timestamps
    hours = [int(timestamp.split('T')[1][:2]) for timestamp in timestamps]

    # Count occurrences of each hour
    hour_counts = Counter(hours)

    # Find the hour with the maximum count
    most_active_hour = max(hour_counts, key=hour_counts.get)

    # Convert the hour to 12-hour clock format with AM/PM
    if most_active_hour < 12:
        most_active_hour_str = f"{most_active_hour % 12 or 12}-{(most_active_hour % 12) + 1}am"
    elif most_active_hour == 12:
        most_active_hour_str = "12-1pm"
    else:
        most_active_hour_str = f"{(most_active_hour % 12) or 12}-{(most_active_hour % 12) + 1}pm"

    # Count the number of elements in the most active hour
    most_active_count = hour_counts[most_active_hour]

    return most_active_hour_str, most_active_count


def print_most_active_hour(most_active_hour, most_active_count):
    """
    Print out the most active hour information in a human-readable format.

    Args:
        most_active_hour (str): The most active hour in the format '2-3pm'.
        most_active_count (int): The count of elements in the most active hour.
    """
    print(f"{most_active_hour} had the most tweets (#{most_active_count})")


def test_most_active_hour(timestamps):
    # Calculate most active hour
    most_active_hour, most_active_count = calculate_most_active_hour(timestamps)

    # Print most active hour information
    print_most_active_hour(most_active_hour, most_active_count)



# Test the functions with the sample list
test_most_active_hour(my_list)


### this is the most active day

def calculate_most_active_day(timestamps):
    """
    Calculate the most active day based on the given list of timestamps.

    Args:
        timestamps (list): A list of timestamps.

    Returns:
        tuple: A tuple containing the most active day (e.g., '3rd October'), the count of elements in that day.
    """
    from collections import Counter
    from datetime import datetime

    # Convert timestamps to dates
    dates = [datetime.strptime(timestamp.split('T')[0], '%Y-%m-%d') for timestamp in timestamps]

    # Count occurrences of each day
    day_counts = Counter(dates)

    # Find the day with the maximum count
    most_active_day = max(day_counts, key=day_counts.get)

    # Format the most active day
    most_active_day_str = most_active_day.strftime("%d %B")

    # Count the number of elements in the most active day
    most_active_count = day_counts[most_active_day]

    return most_active_day_str, most_active_count


def print_most_active_day(most_active_day, most_active_count):
    """
    Print out the most active day information in a human-readable format.

    Args:
        most_active_day (str): The most active day in the format '3rd October'.
        most_active_count (int): The count of elements in the most active day.
    """
    print(f"{most_active_day} had the most tweets (#{most_active_count})")


def test_most_active_day(timestamps):
    # Calculate most active day
    most_active_day, most_active_count = calculate_most_active_day(timestamps)

    # Print most active day information
    print_most_active_day(most_active_day, most_active_count)




# Test the functions with the sample list
test_most_active_day(my_list)

'''
#test cases
sample_list_2 = [
    '2023-05-15T14:18:59.000Z',
    '2023-05-15T12:21:05.000Z',
    '2023-08-15T14:23:48.000Z',
    '2023-08-15T15:56:40.000Z',
    '2023-08-15T14:59:17.000Z',
    '2023-08-20T03:36:59.000Z',
    '2023-10-03T01:50:19.000Z'
]
test_most_active_day(sample_list_2)
test_most_active_hour(sample_list_2)

test_case_5 = {
    '2024-07-07T03:18:59.000Z': -0.7142857142857143,
    '2024-07-07T03:21:05.000Z': 0.36363636363636365,
    '2024-07-07T10:23:48.000Z': 0,
    '2024-07-07T10:56:40.000Z': 0.25,
    '2024-07-07T11:09:17.000Z': 0,
    '2024-07-08T23:36:59.000Z': 1.09523809523809523,
    '2024-07-07T00:50:19.000Z': {'score': 0, 'comparative': 0, 'calculation': [], 'tokens': ['@awlivv', '🔥🔥🔥🔥🔥🍑😍'], 'words': [], 'positive': [], 'negative': []}
}

test_happiest_hour(test_case_5)
test_happiest_day(test_case_5)
'''