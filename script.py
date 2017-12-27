import twitter
import csv
import redis
import config
import concurrent.futures

def get_followers(api, redis_client):
    key = 'twitter_followers'
    if not redis_client.exists(key):
        followers = api.GetFollowers()
        to_cache = []
        for follower in followers:
            to_cache.append(follower.screen_name)

        for screen_name in to_cache:
            redis_client.lpush(key, screen_name)

"""
#Synchronized version to get likes from your followers.
def get_followers_likes(api, redis_client):
    key = 'twitter_followers'
    if redis_client.exists(key):
        followers_list = redis_client.lrange(key, 0, -1)
        followers_dict = dict()

        for screen_name in followers_list:
            followers_dict[screen_name] = api.GetFavorites(screen_name=screen_name, count=30)

        return followers_dict
"""

#Helper for the ThreadPoolExecutor
def get_from_url(screen_name, api):
    return screen_name, api.GetFavorites(screen_name=screen_name, count=30)

#ThreadPoolExecutor version to get likes of all your followers.
def get_followers_likes(api, redis_client):
    key = 'twitter_followers'
    if redis_client.exists(key):
        followers_list = redis_client.lrange(key, 0, -1)
        followers_dict = dict()
        futures = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            for screen_name in followers_list:
                futures.append(executor.submit(get_from_url, screen_name, api))

            for res in concurrent.futures.as_completed(futures):
                tup = res.result()
                followers_dict[tup[0]] = tup[1]

            return followers_dict

def output_csv(followers):
    if followers != None:
        followers = sorted(followers, key = lambda x : x[1], reverse=True)
        with open('follower_likes.csv', 'w') as csv_file:
            writer = csv.writer(csv_file)
            for row in followers:
                writer.writerow(row)

def find_intersection(api, followers):
    my_likes = api.GetFavorites(count=30)
    my_dict = dict()
    for like in my_likes:
        my_dict[like.id] = like.text

    output = []
    for key, val in followers.items():
        key_val = [key, 0]
        for ele in val:
            if ele.id in my_dict:
                key_val[1]=key_val[1]+1
        output.append(key_val)

    output_csv(output)

if __name__ == '__main__':
    api = twitter.Api(consumer_key=config.twitter_consumer_key,
                      consumer_secret=config.twitter_consumer_secret,
                      access_token_key=config.twitter_access,
                      access_token_secret=config.twitter_access_secret)
    redis_client = redis.Redis(host=config.redis_host, port=config.redis_port)
    get_followers(api, redis_client)
    followers = get_followers_likes(api, redis_client)
    find_intersection(api, followers)
