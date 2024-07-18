import aiohttp
import asyncio
import difflib
import pandas as pd
import streamlit as st

async def openAiApiCall(messages):
    api_key = ""
    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
    data = {"model": "gpt-4", "messages": messages, "temperature": 0.1}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=data) as response:
            if response.status != 200:
                return f"Error: {response.status}, {await response.text()}"
            json_response = await response.json()            
            return json_response['choices'][0]['message']['content']


def geraMensagens_sentimento(lista_de_comentários):        
    messages = [{"role": "system", "content": f"""Tarefa: Análise de Sentimentos
                 Formato de saida: "texto : sentimento (negativo, positivo ou neutro)"
                 
                 Analise o sentimento de cada texto na lista a seguit: {lista_de_comentários}
                 """}]
    return messages

def geraMensagens_tags(lista_de_comentarios, tema_principal, contexto):
    messages = [
        {"role": "system", "content": f"""Tema Principal: {tema_principal}
Contexto: {contexto}
Objetivo: Desenvolver categorias para agrupar os comentários fornecidos. As categorias devem ser concisas e não devem começar com a frase "Comentários sobre". Em vez disso, as categorias devem descrever diretamente o conteúdo dos comentários, como "Elogios", "Críticas", ou "Sugestões".
Comentários:
{lista_de_comentarios}
Instruções: Por favor, crie 10 categorias baseadas nos comentários listados acima, usando descrições diretas e concisas para cada uma delas. As categorias devem refletir os principais temas e sentimentos expressos nos comentários. Caso não seja possível inferir alguma categoria a partir dos comentários, classificar como "Outros". As categorias devem ter no máximo 4 palavras
"""}
    ]
    return messages

def geraMensagens_categorias(lista_de_tags, tema_principal):
    messages = [
        {"role": "system", "content": f"""Tema principal: {tema_principal}
Objetivo: Gerar categorias para o conjunto de textos informados pelo usuário.
Metodologia: Leia a lista de textos e classifique cada texto conforme as categorias criadas.
Regras: 1. As categorias devem ter no máximo 3 palavras. 
2. Evite nomear uma categoria com o tema principal.
3. Não crie mais de 10 categorias.
4. Retorne somente os textos classificados
5. caso sejam criadas mais de 10 categorias, agrupe algumas para que essa regra não seja quebrada
Formato de saída :  
exemplo de texto 1 : exemplo de categoria 1
exemplo de texto 2 : exemplo de categoria 2
Lista de textos: {lista_de_tags}
"""}
    ]
    return messages

def geraMensagens_classificação(lista_de_comentarios,lista_de_categorias):
    messages = [
        {"role": "system", "content": f"""Objetivo da tarefa: Classificar os comentários.

        Classifique a lista de comentário de acordo com a categoria que melhor enfatizar o sentido do texto.
        Caso um comantério não possa ser classificado com as categorias existentes, classifique- o como : Indefinido.

        Lista de Categorias: {lista_de_categorias}
         
        Lista de Comentários: {lista_de_comentarios}

        Formato de saída: 
        exemplo de comentário 1 : exemplo de categoria 1        
        exemplo de comentário 2 : exemplo de categoria 2        
"""}
    ]
    return messages

def geraMensagens_classificação_refinamento(texto):
    messages = [
        {"role": "system", "content": f"""Leia a string abaixo que está configurada na forma " Tag : Categoria".
1. Caso existam mais de 10 categorias, substitua algumas de forma que existam no máximo 10. 
3. Se necessário gere novas categorias de forma que consiga agrupar outras. 
4. Substitua as categorias no tageamento e retorne uma string no mesmo formato.
Formato de saída: 
exemplo de tag 1 : exemplo de categoria 1
exemplo de tag 2 : exemplo de categoria 2
Observação: retorne somente o novo tageamento, sem nenhuma outra informação
Texto para analise: {texto}
"""}
    ]
    return messages

def dividir_lista(lista, tamanho_sublista):
    # Usando list comprehension para criar sub-listas com o tamanho especificado
    return [lista[i:i + tamanho_sublista] for i in range(0, len(lista), tamanho_sublista)]

def normalize_text(text):
    """ Normaliza o texto removendo espaços extras, convertendo para minúsculas e removendo pontuações comuns. """
    import string
    text = text.lower().strip()
    text = text.translate(str.maketrans('', '', string.punctuation))
    return text

def find_unique_tags(tags):
    """ Encontra categorias únicas em uma lista, considerando semelhanças de texto simples. """
    unique_categories = []
    seen_categories = set()

    for tag in tags:
        norm_category = normalize_text(tag)
        
        # Verificar se a categoria normalizada já foi vista ou é muito similar a uma existente
        if not any(difflib.SequenceMatcher(None, norm_category, normalize_text(existing)).ratio() > 0.8 for existing in seen_categories):
            unique_categories.append(tag)
            seen_categories.add(norm_category)

    return unique_categories

def flatten_tags_list(tags_groups):
    # Lista para armazenar todas as categorias unificadas
    all_tags = []
    
    # Itera sobre cada grupo de categorias na lista fornecida
    for group in tags_groups:
        # Divide o grupo em linhas individuais (categorias)
        tags = group.split('\n')
        # Remove a numeração de cada categoria e adiciona à lista unificada
        for tag in tags:
            # Extrai a parte da categoria após o número e o ponto
            cleaned_tag = tag.split('. ', 1)[1] if '. ' in tag else tag
            all_tags.append(cleaned_tag.strip())
    
    return all_tags

import re
def clean_text(text): 
  #text = text.lower()  
  #remove a palavra sentimento
  text = text.replace("sentimento", "").strip()
  # Remove "Comentário" and its variations
  text = re.sub(r'^Comentário\s*\d*:\s*', '', text)
  # Remove leading numerals
  text = re.sub(r'^\d+\.\s*', '', text)
  # Remove as aspas simples
  text = text.replace("'", "")
  text = text.replace('"', "")
  # Remove leading punctuation
  text = text.lstrip(".'\"")
  # Remove URLs
  text = re.sub(r'https?://\S+|www\.\S+', '', text)
  # Finalmente, remove espaços no início e no final do texto  
  text = text.strip()

  return text

def formata_sentimentos(lista):
    comentario = []
    sentimento = []

    # Separa os blocos
    for bloco in lista:
        # Separa as linhas de cada bloco
        linhas = bloco.split("\n")
        for row in linhas:
            splited_row = row.split(":")

            # Processa o texto do comentário
            cleaned_text = clean_text(splited_row[0])            
            comentario.append(cleaned_text)

            # Processa o texto do sentimento
            if len(splited_row) > 1:
                cleaned_text = clean_text(splited_row[1])                
                sentimento.append(cleaned_text)

    # Garantindo que ambas as listas tenham o mesmo tamanho
    min_length = min(len(comentario), len(sentimento))
    comentario = comentario[:min_length]
    sentimento = sentimento[:min_length]

    # Criando DataFrame
    dados = {'Texto': comentario, 'Sentimento': sentimento}
    df_sentimentos = pd.DataFrame(data=dados)
    return df_sentimentos
    

def formata_classificacao(lista_classificados):
    comentario = []
    classificacao = []
    for bloco in lista_classificados:
        row = bloco.split("\n")
        for i in row:
            try:
                cleaned_text = clean_text(i)
                classificacao.append(cleaned_text.split(":")[1])
                comentario.append(cleaned_text.split(":")[0])      
            except:                
                None
                


    dados = {'Texto': comentario, 'Tag': classificacao}
    df_classificado = pd.DataFrame(data=dados)
    return df_classificado

async def refina_categorias(texto):
    print("Refinando as categorias")
    mensagem = geraMensagens_classificação_refinamento(texto)
    
    resposta = await openAiApiCall(mensagem)
    print(f"#### CAtegorias: {resposta}")
    
    df = formata_categorias(resposta)
    
    return df
    

#***************************  SENTIMENTOS *************************************************************
async def sentiment_analysis(lista_de_comentarios):    
    #Divide a lista principal de comentários em uma lista menor para fazer as chamadas a API
    aux_list = dividir_lista(lista_de_comentarios, 10)
    
    #Gera as mensagens para a api para cada item das listas menores
    lista_mensagens = []
    for i in aux_list:
        lista_mensagens.append(geraMensagens_sentimento(i))
         
    #GEra a lista de tarefas para fazer as chamadas assíncronas para a api   
    tasks = []
    for mensagem in lista_mensagens:
        tasks.append(openAiApiCall(mensagem))
    
    # Inicializa a barra de progresso no Streamlit
    progress_bar = st.progress(0)
    
    # Lista para armazenar as respostas
    respostas = []
    
    # Executando as tarefas e atualizando a barra de progresso
    for i, task in enumerate(asyncio.as_completed(tasks), start=1):
        resposta = await task
        respostas.append(resposta)
        progress_bar.progress(i / len(tasks))
        
    df = formata_sentimentos(respostas)
    
    progress_bar.empty()    
    #respostas = await asyncio.gather(*tasks)
    return df

#******************************* TAGS ********************************************************
async def tags_analysis(lista_de_comentarios, tema_principal, contexto):
    print("### Tag analisys")
    # Dividir a lista principal de comentários em uma lista menor para fazer as chamadas à API
    aux_list = dividir_lista(lista_de_comentarios, 100)
    
    # Gera as mensagens para a API para cada item das listas menores
    lista_mensagens = []
    for i in aux_list:
        lista_mensagens.append(geraMensagens_tags(i, tema_principal, contexto))
    
    # Gera a lista de tarefas para fazer as chamadas assíncronas para a API
    tasks = []
    for mensagem in lista_mensagens:
        tasks.append(openAiApiCall(mensagem))
    
    # Inicializa a barra de progresso no Streamlit
    progress_bar = st.progress(0)
    
    # Lista para armazenar as respostas
    respostas = []
    
    # Executando as tarefas e atualizando a barra de progresso
    for i, task in enumerate(asyncio.as_completed(tasks), start=1):
        resposta = await task
        respostas.append(resposta)
        progress_bar.progress(i / len(tasks))
    
    # Remove as categorias duplicadas
    flattened_tags = flatten_tags_list(respostas)
    unique_tags = find_unique_tags(flattened_tags)
    df_tags = pd.DataFrame(data=(unique_tags), columns=['Tag'])
    
    progress_bar.progress(100)
    # Resetar a barra de progresso (opcional)
    progress_bar.empty()
    
    return df_tags

#******************************* CATEGORIAS ********************************************************
async def category_analysis(lista_de_tags, tema_principal):        
    mensagem = geraMensagens_categorias(lista_de_tags, tema_principal)    
    resposta = await openAiApiCall(mensagem)                
    
    #df = formata_categorias(resposta)

    df = await refina_categorias(resposta)
    
    return df



def formata_categorias(data_string):
    # print(data_string)
    # # Primeiro, removemos os espaços extras e aspas
    # #cleaned_string = data_string.replace("'", "")
    # cleaned_string = clean_text(data_string)
    
    # # Separar as entradas por vírgulas
    # entries = cleaned_string.split(',')
    
    # # Dividir cada entrada em 'Tag' e 'Categoria' usando ':'
    # tags = []
    # categories = []
    # for entry in entries:
    #     try:
    #         tag, category = entry.split(':')
    #         tags.append(tag)
    #         categories.append(category) 
    #     except:
    #         None
    
    # # Criar um DataFrame com as listas de tags e categorias
    # df = pd.DataFrame({
    #     'Tag': tags,
    #     'Categoria': categories
    # })
    
    # print(f"df de categorias: {df}")
    # return df

    cleaned_string = data_string.replace("'", "")
    cleaned_string = clean_text(data_string)
    
    # Transformar a string em uma lista de linhas
    lines = cleaned_string.split('\n')

    # Separar as linhas em duas colunas
    rows = [line.split(' : ') for line in lines]

    # Criar o DataFrame
    df = pd.DataFrame(rows, columns=['Tag', 'Categoria'])

    # Exibir o DataFrame
    print(f"dataframe:  {df}")
    return df

    
#******************************* CLASSIFICAÇÃO ********************************************************
async def text_classification(lista_de_comentarios,lista_de_categorias):
    # Dividir a lista principal de comentários em uma lista menor para fazer as chamadas à API
    aux_list = dividir_lista(lista_de_comentarios, 10)
    
    # Gera as mensagens para a API para cada item das listas menores
    lista_mensagens = []
    for i in aux_list:
        lista_mensagens.append(geraMensagens_classificação(i, lista_de_categorias))
    
    # Gera a lista de tarefas para fazer as chamadas assíncronas para a API
    tasks = []
    for mensagem in lista_mensagens:
        tasks.append(openAiApiCall(mensagem))
    
    # Inicializa a barra de progresso no Streamlit
    progress_bar = st.progress(0)
    
    # Lista para armazenar as respostas
    respostas = []
    
    # Executando as tarefas e atualizando a barra de progresso
    for i, task in enumerate(asyncio.as_completed(tasks), start=1):
        resposta = await task
        respostas.append(resposta)
        progress_bar.progress(i / len(tasks))        
        
    df = formata_classificacao(respostas)
    
    progress_bar.empty()
    
    return df

import json

def generate_js_dictionary(file_path='outputs/text_classification_output.xlsx', sheet_name=1):
    # Carregar a aba especificada do arquivo Excel
    df = pd.read_excel(file_path, sheet_name=sheet_name)

    # Agrupar os dados por Categoria, construindo a estrutura necessária
    grouped = df.groupby('Categoria').apply(lambda x: x[['Tag', 'Incidência', '% na Categoria', '% no Arquivo Geral']].to_dict('records')).to_dict()

    # Construir a estrutura do dicionário conforme o modelo solicitado
    data_dict = {
        "name": "Tema Principal",
        "children": []
    }

    for category, details in grouped.items():
        category_dict = {
            "name": category,
            "children": [
                {"name": tag['Tag'], "value": tag['Incidência'],"percent_categoria": round(tag['% na Categoria'], 1),
                        "percent_geral": round(tag['% no Arquivo Geral'], 1)} for tag in details
            ]
        }
        data_dict['children'].append(category_dict)

    # Converter o dicionário para uma string formatada em JSON para ser usada em JavaScript    
    return json.dumps(data_dict, indent=2, ensure_ascii=False)



    
    


    


