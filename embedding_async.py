
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

async def get_embedding(session, text, api_key):
    #print(f"##### Calling API at {datetime.datetime.now()}")
    url = "https://api.openai.com/v1/embeddings"
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }
    data = json.dumps({
        "input": text,
        "model": "text-embedding-3-small"
    })
    async with session.post(url, headers=headers, data=data) as response:
        if response.status == 200:
            result = await response.json()
            return result['data'][0]['embedding']
        else:
            response.raise_for_status()  # This will raise an exception for non-200 responses

async def start_embedding(df, api_key, max_per_call):   
    async with aiohttp.ClientSession() as session:
        print("embedding...")
        embed_list = []
        for start in range(0, len(df), max_per_call):
            end = start + max_per_call
            tasks = [get_embedding(session, text, api_key) for text in df['Texto'][start:end]]
            embeddings = await asyncio.gather(*tasks)
            embed_list.extend(embeddings)
        return embed_list

async def process_embedding(df, api_key):
    # Separa o dataframe em grupos de 100
    df_list = split_dataframe(df)
    final_result = []
    # Para cada bloco de 100, analisa os sentimentos de forma assíncrona usando a API
    for df_part in df_list:
        results = await start_embedding(df_part, api_key,100)
        final_result.extend(results)
    
    df['embedding'] = final_result    
    return df

if __name__ == "__main__":
    df = read_df()
    asyncio.run(process_embedding(df))
