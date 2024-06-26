import pandas as pd
import asyncio
import aiohttp
import json
import datetime
import numpy as np

def split_dataframe(df, max_items=100):
    """
    Splits a DataFrame into multiple smaller DataFrames, each with a maximum of 'max_items' rows.
    """
    num_splits = int(np.ceil(len(df) / max_items))
    return np.array_split(df, num_splits)

def read_df():
    return pd.read_excel('outputs/tweets_search_output.xlsx')

async def make_api_call_to_gpt(prompt, api_key):
    #print(f"##### Calling API at {datetime.datetime.now()}")
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }
    async with aiohttp.ClientSession() as session:                
        payload = {
            "model": "gpt-4-turbo",
            "messages": prompt,
            "temperature": 0,
            "max_tokens": 2000,
            "top_p": 1,
            "frequency_penalty": 0,
            "presence_penalty": 0
        }

        async with session.post('https://api.openai.com/v1/chat/completions', headers=headers, data=json.dumps(payload)) as response:
            if response.status == 200:
                resp_json = await response.json()
                return resp_json['choices'][0]['message']['content']
            else:
                print(f"##### ERROR {response.status}")
                return f"Error: {response.status}"

async def get_prompt(role,text):
    prompt = [{'role': role,  'content': f"Analyze the sentiment of the following text. Return only the sentiment, without any additional information: {text}"}]
    return prompt

async def sentiment_analysis(df, api_key, max_per_call=100):
    print("sentiment analysis")
    sentiments = []
    for start in range(0, len(df), max_per_call):
        end = start + max_per_call
        tasks = [make_api_call_to_gpt(await get_prompt('system',text), api_key) for text in df['Texto'][start:end]]
        sentiments.extend(await asyncio.gather(*tasks))
    return sentiments

async def process_sentiments(df, api_key):    
    #Separa o dataframe em grupos de 100 
    df_list = split_dataframe(df)
    final_result = []
    #para cada bloco de 100, analisa os sentimentos de forma assíncrona usando a API
    for df_part in df_list:
         results = await sentiment_analysis(df_part, api_key)
         final_result.append(results)        
                
    flat_list = [item for sublist in final_result for item in sublist]
    
    df['Sentimento'] = flat_list    
    return df
    
if __name__ == "__main__":
    asyncio.run(process_sentiments())
