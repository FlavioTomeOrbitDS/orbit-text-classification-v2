from openai import OpenAI
import openai
import pandas as pd
import re
import string
from typing import List, Optional
import numpy as np
from ast import literal_eval
from sklearn.cluster import KMeans
import datetime
import utils_conf
import json
#************************** MAIN FUNCTIONS **********************************
TEMA_PRINCIPAL = "Chat GPT"
QTD_TAGS = 5
QTD_CLASSIFIC = 2


#Embedding params
embedding_model = "text-embedding-3-small"
embedding_encoding = "cl100k_base"
max_tokens = 8000  # the maximum for text-



def get_current_datetime():
  """Returns the current datetime as a string."""
  return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def limpar_texto(texto):
    # Lista de palavras a serem removidas
    palavras_a_remover = [' q ', ' t ', ' comr ', ' s ', ' pq ', ' rao ', ' j ', ' at ', ' p ', ' pro ',
                          ' to ', ' sade ', ' l ', ' ento ', ' kkkkk ', ' sei ', ' vocs ', ' cm ', ' saudveis ', ' salrio ',
                          ' n ', ' td ', ' mo ', ' krl ', ' gua ', ' sa ', ' mt ', ' tambm ', ' at ', ' raes ', ' moa ',
                          ' nenm ']

    texto = texto.lower()
    texto = re.sub(r'https?://\S+|www\.\S+', '', texto)  # Remove URLs
    texto = re.sub(r'\d+', '', texto)  # Remove números
    texto = re.sub(r'\[.*?\]', '', texto)  # Remove texto entre colchetes
    texto = re.sub(r'[\w\.-]+@[\w\.-]+', '', texto)  # Remove emails
    texto = re.sub(r'(?:(?:31(\/|-|\.)(?:0?[13578]|1[02]))\1|(?:(?:29|30)(\/|-|\.)(?:0?[1,3-9]|1[0-2])\2))(?:(?:1[6-9]|[2-9]\d)?\d{2})$|^(?:29(\/|-|\.)0?2\3(?:(?:(?:1[6-9]|[2-9]\d)?(?:0[48]|[2468][048]|[13579][26])|(?:(?:16|[2468][048]|[3579][26])00))))$|^(?:0?[1-9]|1\d|2[0-8])(\/|-|\.)(?:(?:0?[1-9])|(?:1[0-2]))\4(?:(?:1[6-9]|[2-9]\d)?\d{2})', '', texto)  # Remove datas
    texto = re.sub(r'[^A-Za-záéíóúâêîôûñç \t]', '', texto)  # Remove caracteres especiais e emojis
    texto = re.sub(r'\s+', ' ', texto)  # Remove múltiplos espaços

    for palavra in palavras_a_remover:
        texto = texto.replace(palavra, " ")

    # Remove pontuações após a limpeza
    texto = ''.join([char for char in texto if char not in string.punctuation])
    return texto

def gen_cluster_description(client,n_clusters, cluster_column, text_column, max_length, sentimento, df):
    samples_per_cluster = 5
    cluster_descriptions = []
    
    for i in range(n_clusters):
        #print(f"Cluster {i}: ")
        cluster_data = df[df[cluster_column] == i].drop_duplicates(subset=[text_column])  # Remove duplicatas baseado na coluna de texto
        sample_size = min(samples_per_cluster, len(cluster_data))  # Define o tamanho da amostra com base na quantidade de dados únicos
        sampled_texts = cluster_data[text_column].sample(n=sample_size, random_state=42).values  # Seleciona textos aleatoriamente
        numbered_texts = "\n".join(
            f"{idx + 1}. {text}" for idx, text in enumerate(sampled_texts)  # Adiciona numeração a cada texto
        )

        tweets_list = numbered_texts
        contexto = "O contexto do estudo é chatgpt"

        #PROMPT 1
        # prompt_user = f"""Você é um analista de dados avançado, especializado em processamento de linguagem natural. Seu papel hoje é analisar um conjunto de tweets fornecidos, com o tema {TEMA_PRINCIPAL}. Após uma análise detalhada, seu objetivo é sintetizar as informações e gerar uma única descrição concisa que capture a essência dos temas abordados nos tweets.
        # Faça as análises considerando o seguinte contexto:
        # {contexto}
        # Especificações:
        # 1. Leia e processe cada tweet, identificando temas-chave.
        # 2. Use técnicas de NLP para detectar padrões e tendências nos dados.
        # 3. Gere uma descrição clara e objetiva para o grupo de tweets. A descrição deve ser precisa, refletir os temas principais.
        # 4. Mantenha a descrição entre 3 e {max_length} palavras para garantir concisão e foco.
        # 5. Retorne somente uma única descrição criada que consiga sintetizar todas as outras, sem nenhuma outra informação adicional
        # 6. Evite usar palavras do tipo 'Positivo', 'Negativo' e 'Neutro' na tag criada.
        # 7. Evite usar palavras {TEMA_PRINCIPAL}
        # 8. O sentimento geral dos tweets é {sentimento}

        # Lista de tweets:
        # {tweets_list}
        # """

        #PROMPT 2
        # prompt_user = f"""Por favor, gere uma categoria para o grupo de comentários acima. Me retorne somente a categoria gerada, sem nenhuma outra informação:
        # {contexto}
        # Lista de comentários:
        # {tweets_list}
        # """

        prompt_user =f"""
Considerando os seguintes comentários de redes sociais conforme o seguinte contexto {contexto}:
{tweets_list}
Por favor, analise e sintetize uma categoria única que capture a essência geral destes comentários, refletindo temas ou preocupações principais expressos. Em caso de opinioes, discussões, criticas, preocupaçoes ou outras situações do tipo, cite as palavras que estão causando esse efeito nos comentários
Evite usar a palavra "{TEMA_PRINCIPAL}"
Retorne somente a categoria gerada, sem nenhuma outra informação.
"""
        messages = [
            {"role": "system", "content": prompt_user}
        ]

        response = client.chat.completions.create(
            model="gpt-4",
            messages=messages,
            temperature=0.5,
            max_tokens=100,
            top_p=1,
            frequency_penalty=0.1,
            presence_penalty=0.1
        )

        response_description = response.choices[0].message.content
        cluster_descriptions.append(response_description)
        #print(f"##### PROMPT: {prompt_user}")
        #print(f"Lista de tweets: {tweets_list}")
        #print(f"##### RESPONSE: {response_description}")

    return cluster_descriptions

def get_embedding(client,text: str, model="text-embedding-3-small", **kwargs) -> List[float]:
    # replace newlines, which can negatively affect performance.
    text = text.replace("\n", " ")

    response = client.embeddings.create(input=[text], model=model, **kwargs)

    return response.data[0].embedding

def get_clusters(n_clusters,embedding_column,cluster_column,df):
  sentimentos = ['positivo','neutro','negativo']
  df_final_clusters = pd.DataFrame()

  # para cada sentimento, gera uma matriz e depois agrega no numero de clusters
  for sentimento in sentimentos:
    df_aux = df[df['Sentimento'] == sentimento]
    matrix = np.vstack(df_aux[embedding_column].values)

    kmeans = KMeans(n_clusters=n_clusters, init="k-means++", random_state=42)
    kmeans.fit(matrix)
    labels = kmeans.labels_
    df_aux[cluster_column] = labels

    df_final_clusters = pd.concat([df_final_clusters,df_aux])

  return df_final_clusters

def sentiment_analysis(client,text):
    # Initialize the OpenAI API client with your API key
    openai.api_key = "your_openai_api_key"
    
    prompt = f"Analise o sentimento do texto a seguir. Retorne somente o sentimento, sem nenumha outra informação: {text}"
    messages = [
        {"role": "system", "content": prompt}
    ]
    # response = openai.ChatCompletion.create(
    #     model="gpt-4",
    #     messages=messages,
    #     temperature=0.1,
    #     max_tokens=100,
    #     top_p=1,
    #     frequency_penalty=0.1,
    #     presence_penalty=0.1
    # )
    
    response = client.chat.completions.create(
            model="gpt-4",
            messages=messages,
            temperature=0.5,
            max_tokens=100,
            top_p=1,
            frequency_penalty=0.1,
            presence_penalty=0.1
        )

    
    response_content = response.choices[0].message.content
    # Extract the sentiment from the API response
    response_content = response_content.strip().lower()
    
    # Map the sentiment to a more concise label
    if "positivo" in response_content:
        sentiment = "positivo"
    elif "negativo" in response_content:
        sentiment = "negativo"
    elif "neutro" in response_content:
        sentiment = "neutro"
    else:
        sentiment = "indeterminado"  # Handle unexpected responses
    
    return sentiment

def le_dataframe(file_path = 'outputs/tweets_search_output.xlsx'):
    try:
        df = pd.read_excel(file_path)    
        print(f"###### Tamanho do arquivo: {len(df)}")
        return df
    except Exception as e:
        print(f'Erro ao ler o dataframe: {e}')
        return None

def adicionar_tags_embedding(df, dicionario):
# Criando um mapeamento dos clusters para embeddings usando o dicionário
    map_desc_to_embedding = dict(zip(dicionario['desc'], dicionario['embedding']))

    # Aplicando o mapeamento para criar a nova coluna 'tags_embedding'
    df['tags_embedding'] = df['Cluster_Description'].map(map_desc_to_embedding)

    return df

def processar_incidencias(file_path):
    # Carregar o arquivo Excel
    data = pd.read_excel(file_path)

    # Verificar se as colunas necessárias estão presentes
    required_columns = ['Categoria', 'Tag', 'Sentimento']
    if not all(col in data.columns for col in required_columns):
        return "As colunas necessárias não estão todas presentes no arquivo."

    # Agrupar os dados e calcular a incidência
    grouped_data = data.groupby(required_columns).size().reset_index(name='Incidência')

    # Calcular a incidência total por categoria
    total_por_categoria = data.groupby('Categoria').size().reset_index(name='Total Categoria')

    # Calcular a incidência total geral
    total_geral = data.shape[0]

    # Unir os dados para calcular as porcentagens
    final_data = pd.merge(grouped_data, total_por_categoria, on='Categoria')
    final_data['% na Categoria'] = (final_data['Incidência'] / final_data['Total Categoria']) * 100
    final_data['% no Arquivo Geral'] = (final_data['Incidência'] / total_geral) * 100

    # Criar uma nova aba no arquivo Excel
    with pd.ExcelWriter(file_path, mode='a', engine='openpyxl') as writer:
        final_data.to_excel(writer, sheet_name='Incidência', index=False)

    return "Processamento concluído com sucesso!"

def generate_js_dictionary(file_path = 'outputs/text_classification_output.xlsx', sheet_name= 1):
    # Carregar a aba especificada do arquivo Excel
    df = pd.read_excel(file_path, sheet_name=sheet_name)

    # Agrupar os dados por Sentimento e Categoria, e incluir as Tags e Incidências
    grouped = df.groupby(['Sentimento', 'Categoria']).apply(
        lambda x: x[['Tag', 'Incidência']].to_dict('records')
    ).reset_index().groupby('Sentimento').apply(
        lambda x: x[['Categoria', 0]].set_index('Categoria').to_dict(orient='index')
    ).to_dict()

    # Construir a estrutura do dicionário conforme o modelo solicitado
    data_dict = {
        "name": "Tema Principal",
        "children": []
    }

    for sentiment, categories in grouped.items():
        sentiment_dict = {
            "name": f"sentimento {sentiment}",
            "children": []
        }
        for category, details in categories.items():
            category_dict = {
                "name": category,
                "children": [
                    {"name": tag['Tag'], "value": tag['Incidência']} for tag in details[0]
                ]
            }
            sentiment_dict['children'].append(category_dict)
        data_dict['children'].append(sentiment_dict)

    # Converter o dicionário para uma string formatada em JSON para ser usada em JavaScript
    # Usando ensure_ascii=False para manter caracteres acentuados corretamente
    #js_string = 'const data = ' + json.dumps(data_dict, indent=2, ensure_ascii=False) + ';'
    
    return json.dumps(data_dict, indent=2, ensure_ascii=False)



 #************************** MAIN FUNCTIONS############################### 
def text_classification():
    #OpenAI API Key
    client = OpenAI(api_key = utils_conf.get_api_key('OPENAI_KEY'))
    
    print(f"#### Lendo dataframe...{get_current_datetime()}")
    df = le_dataframe()
    
    #Formatação dos textos
    print(f"##### Formatando os textos...{get_current_datetime()}")
    texto_limpo = []
    df.Texto.apply(lambda x : texto_limpo.append(limpar_texto(x)))
    df.Texto = texto_limpo
    df = df.drop_duplicates(subset=['Texto'])
        
    #Análise de sentimentos
    print(f"##### Fazendo a analise de sentimentos...{get_current_datetime()}")
    sentimentos = []
    df.Texto.apply(lambda x : sentimentos.append(sentiment_analysis(client,x)))
    df = df.dropna(subset=['Texto'])
    df['Sentimento'] = sentimentos
    df['Sentimento'] = df['Sentimento'].replace("'negativo'", "negativo")
    df['Sentimento'] = df['Sentimento'].replace("'positivo'", "positivo")
    df['Sentimento'] = df['Sentimento'].replace("'neutro'", "neutro")
    
    # df = pd.read_excel("testes/ChatGPT_sentimentos.xlsx")
    # df = df.head(500)
    
    #Embedding
    print(f"####Embedding...{get_current_datetime()}")
    df["embedding"] = df.Texto.apply(lambda x: get_embedding(client,x, model=embedding_model))
    df.to_csv("text_embeddings.csv")

    #Gerando os Clusters
    print(f"#####Gerando CLusters...{get_current_datetime()}")
    datafile_path = "text_embeddings.csv"
    df = pd.read_csv(datafile_path)
    df["embedding"] = df.embedding.apply(literal_eval).apply(np.array)  # convert string to numpy array        
    df_final_clusters = get_clusters(QTD_TAGS,'embedding', 'Cluster',df)
    
    #Gerando as descrições dos clusters
    print(f"##### Gerando as descrições das tags...{get_current_datetime()}")
    sentimentos = ['positivo', 'neutro','negativo']
    final_df = pd.DataFrame()
    for sentimento in sentimentos:
        print(f"Processando sentimento {sentimento}")
        cluster_descriptions = []
        temp_df = df_final_clusters[df_final_clusters['Sentimento'] == sentimento]
        aux = gen_cluster_description(client,QTD_TAGS, 'Cluster', 'Texto', 8,sentimento,temp_df)
        cluster_descriptions.append(aux)
        temp_df['Cluster_Description'] = temp_df['Cluster'].apply(lambda x: cluster_descriptions[0][x])
        final_df = pd.concat([final_df, temp_df])

    #Gerando as classificações
    #Agrupa as tags iguais para gerar o embedding
    print(f"##### Gerando as descrições das Classificações...{get_current_datetime()}")
    lista_cluster_desc_embed = { 'desc': [], 'embedding':[]}

    for i in final_df['Cluster_Description'].unique().tolist():
        lista_cluster_desc_embed['desc'].append(i)
        lista_cluster_desc_embed['embedding'].append(get_embedding(client,i, model=embedding_model))

    final_df = adicionar_tags_embedding(final_df, lista_cluster_desc_embed)
    
    #Gerando os clusters
    df_final_clusters = get_clusters(QTD_CLASSIFIC, 'tags_embedding', 'Tags_cluster',final_df)
    #Gerando as descrições das categorias
    sentimentos = ['positivo', 'neutro','negativo']
    final_df = pd.DataFrame()
    for sentimento in sentimentos:
        print(f"Processando sentimento {sentimento}")
        cluster_descriptions = []
        temp_df = df_final_clusters[df_final_clusters['Sentimento'] == sentimento]
        aux = gen_cluster_description(client,QTD_CLASSIFIC,'Tags_cluster', 'Cluster_Description',4,sentimento,temp_df)
        cluster_descriptions.append(aux)
        temp_df['Categoria'] = temp_df['Tags_cluster'].apply(lambda x: cluster_descriptions[0][x])
        final_df = pd.concat([final_df, temp_df])

    #Formatação do arquivo final
    print(f"##### Formatando arquivo final...{get_current_datetime()}")
    final_df = final_df.drop(['Unnamed: 0','embedding', 'Cluster', 'tags_embedding','Tags_cluster'], axis=1)
    final_df = final_df.rename(columns={'Cluster_Description': 'Tag'})

    final_df.to_excel(f"outputs/text_classification_output.xlsx")
    
    print(f"##### Gerando aba de incidências...!{get_current_datetime()}")
    processar_incidencias("outputs/text_classification_output.xlsx")
    
    print(f"##### Processo finalizado!{get_current_datetime()}")
            
    return final_df

def main():    
    

    text_classification(client)


if __name__ == "__main__":
    main()
    
