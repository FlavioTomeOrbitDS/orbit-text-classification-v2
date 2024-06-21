import requests
import pandas as pd
import time
import utils_conf

def get_all_recent_tweets(query, bearer_token, lang, max_results):
    tweets_list = []
    url = "https://api.twitter.com/2/tweets/search/recent"
    headers = {
        "Authorization": f"Bearer {bearer_token}"
    }
    # headers = {
    #     "Authorization": f"Bearer {bearer_token}"
    # }
    params = {
        "query": f"{query} {lang}",
        "max_results": max_results
    }
    next_token = None
    call_count = 0  # Counter to limit the number of API calls

    while True:
        if call_count > 4:
            break  # Break the loop if the call count exceeds 3 to avoid excessive API calls
        print(f"### Pagination {call_count}")
        call_count += 1
        
        if next_token:  # Add the pagination token to the parameters if it exists
            params['pagination_token'] = next_token
        
        response = requests.get(url, headers=headers, params=params)        
        time.sleep(0.5)  # Rate limiting handling

        if response.status_code == 200:
            tweets_data = response.json()
            tweets_list.extend(tweets_data['data'])  # Collect tweets from the current page
            
            next_token = tweets_data.get('meta', {}).get('next_token', None)
            if not next_token:
                break  # Exit the loop if there is no next token
        else:
            print("Failed to retrieve tweets")
            print("Status code:", response.status_code)
            print("Response:", response.text)
            return None

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(tweets_list)
    df.rename(columns={'text': 'Texto'}, inplace=True)
    return pd.DataFrame(df['Texto'])



def tweets_search(query,bearer_token,lang , max_results):
    print(f"##### Tweets Search: Buscando {query} Max results {max_results}...")    
    final_df = get_all_recent_tweets(query,bearer_token,lang, max_results)
    final_df.to_excel("outputs/tweets_search_output.xlsx")    
    
    return final_df