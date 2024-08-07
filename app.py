from flask import Flask, request, jsonify,send_from_directory # type: ignore
from flask_cors import CORS # type: ignore
from tweets_search import tweets_search
import pandas as pd
import utils_conf
import re
from scripts_async import *

app = Flask(__name__)
CORS(app)

@app.route('/')
def home():    
    
    return 'Text Classification Backend!'

def processar_incidencias(file_path):
    # Carregar o arquivo Excel
    data = pd.read_excel(file_path)

    # Verificar se as colunas necessárias estão presentes
    required_columns = ['Categoria', 'Tag']
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

    return final_data

def remove_mentions_hashtags(text):
    text = re.sub(r'@\w+', '', text)
    text = re.sub(r'#\w+', '', text)
    return text

# def remove_stopwords(text):
#     stop_words = set(stopwords.words('portuguese'))
#     word_tokens = nltk.word_tokenize(text)
#     filtered_text = [word for word in word_tokens if word.lower() not in stop_words]
#     return ' '.join(filtered_text)

def clean_text(text):
    if not isinstance(text, str):
        text = str(text)
    text = remove_mentions_hashtags(text)
    #text = remove_special_characters(text)
    #text = remove_stopwords(text)
    return text.strip()

def gera_df_final(df_classific,df_categorias):
    # Primeiro, garanta que a coluna 'Tag' no DataFrame df_classific não tenha espaços extras
    df_classific['Tag'] = df_classific['Tag'].str.strip()
    df_categorias['Tag'] = df_categorias['Tag'].str.strip()

    # Realizar uma junção (merge) para adicionar a coluna 'Categoria' baseada na correspondência de 'Tag'
    df_classific = pd.merge(df_classific, df_categorias, on='Tag', how='left')

    #df_classific = df_classific.drop(columns=['Unnamed: 0_x', 'Unnamed: 0_y'])

    # O DataFrame df_classific agora possui uma coluna 'Categoria' correspondente
    return df_classific

async def text_class(lista_de_comentários, tema, context):
    ERROR_CONT = 0
                
    #Gera as Tags
    try:
        print("##### Gerando tags")
        df_tags = ( await tags_analysis(lista_de_comentários, tema, context))            
        print(f"##### TAGS {df_tags}")
    except Exception as e:
        print(f"##### Erro ao gerar Tag: {e}")
    #Gera as Categorias
    try:
        print("##### Gerando categorias")
        df_categorias = await category_analysis(df_tags['Tag'].to_list(), tema)       
    except:
        print(f"##### Erro ao as Categorias: {e}")
    
    #Classificação dos textos
    try:
        print("##### Gerando classificação")
        df_classificado = ( await text_classification(lista_de_comentários, df_tags['Tag'].to_list()))            
    except:
        print(f"##### Erro ao Classificar: {e}")
    
    #GEra arquuivo final 
    try:
        df_final = gera_df_final(df_classificado,df_categorias)                
        df_final.to_excel("outputs/text_classification_output.xlsx")                
    except:
        print(f"##### Erro ao gerar o arquivo final: {e}")
        
    try:
        print("Calculando totais...")                
        df_final = processar_incidencias("outputs/text_classification_output.xlsx")                
    except:
        print(f"##### Erro ao calcular as incidências: {e}")
    
    json_data = df_final.to_json(orient='records')   
    
    return json_data         

#--------------------------------------------------------------------------------------
@app.route('/api/tweetssearch', methods=['POST'])
def tweetssearch():
    if request.is_json:
        # Get the JSON data
        data = request.get_json()    
        query = data.get('query', None)        
        #b_token = utils_conf.get_api_key('bearer_token')
        #max_results = utils_conf.get_api_key('max_tweets')
        b_token = utils_conf.get_config_value('bearer_token')        
        max_results = utils_conf.get_config_value('max_tweets')
        df = tweets_search(query,b_token,"lang:pt" ,max_results)    
        json_data = df.to_json(orient='records')        
                
        return jsonify(json_data=json_data)            
        
    
    return None

#--------------------------------------------------------------------------------------
@app.route('/api/getclassifications', methods=['POST'])
async def getclassifications():
    if request.is_json:    
        data = request.get_json()    
        context = data.get('context', None)
        tema = data.get('tema', None)        
        # # Get the JSON data       
        df = pd.read_excel('outputs/tweets_search_output.xlsx')    
        #result = text_classification(df, context, tema)           
        if 'Texto' in df.columns:
            textos = df['Texto']
            df = pd.DataFrame(textos)                      
            df['texto_limpo']  = df['Texto'].apply(clean_text)
            lista_de_comentários = df['texto_limpo'].to_list()                                        
            
            json_data = await text_class(lista_de_comentários, tema, context)
                                
            return jsonify(json_data=json_data), 200
        
        
    return jsonify({"error": "Erro"}), 400      
    

#--------------------------------------------------------------------------------------
@app.route('/api/getclassificationsbyfilenew', methods=['POST'])
async def getclassificationsbyfilenew():        
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']
    tema = request.form.get('tema')
    context = request.form.get('context')    
    
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    if file:        
            df = pd.read_excel(file)                        
            if 'Texto' in df.columns:
                textos = df['Texto']
                df = pd.DataFrame(textos)
                df = df.head(1000)                
                df['texto_limpo']  = df['Texto'].apply(clean_text)
                lista_de_comentários = df['texto_limpo'].to_list()                                        
                
                json_data = await text_class(lista_de_comentários, tema, context)
                                     
                return jsonify(json_data=json_data), 200
            else:
                return jsonify({"error": "Column 'Texto' not found"}), 400
       
#--------------------------------------------------------------------------------------
@app.route('/api/getchartdata', methods=['GET'])
def getchartdata():        
    json_data = generate_js_dictionary()
    return jsonify(json_data=json_data)            

#--------------------------------------------------------------------------------------
@app.route('/api/getkeys', methods=['GET'])
def getkeys():
    try:
        keys = []
        keys.append(utils_conf.get_config_value('bearer_token'))
        keys.append(utils_conf.get_config_value('OPENAI_KEY'))
        keys.append(utils_conf.get_config_value('max_tweets'))
        keys.append(utils_conf.get_config_value('max_len_tags'))
        keys.append(utils_conf.get_config_value('max_len_class'))
        keys.append(utils_conf.get_config_value('tagQTD'))
        keys.append(utils_conf.get_config_value('classificQTD'))
        
        print(f"#### Data sent {keys}")
    except:
        return jsonify("Erro ao carregar as APIs")            
        
    return jsonify(keys)          

#--------------------------------------------------------------------------------------
@app.route('/api/setkeys', methods=['POST'])
def setkeys():
    if request.is_json:
        # Get the JSON data
        data = request.get_json()    
        print(f"##### Config received: {data}")
        twitter_key = data.get('twitter_key', None)
        openai_key = data.get('openai_key', None)
        max_tweets = data.get('max_tweets', None)
        max_len_tags = data.get('max_len_tags', None)
        max_len_class = data.get('max_len_class', None)
        tagQTD = data.get('tagQTD', None)
        classificQTD = data.get('classificQTD', None)
        
        utils_conf.update_config_file('bearer_token', twitter_key)
        utils_conf.update_config_file('OPENAI_KEY', openai_key)
        utils_conf.update_config_file('max_tweets', str(max_tweets))
        utils_conf.update_config_file('max_len_tags', str(max_len_tags))
        utils_conf.update_config_file('max_len_class', str(max_len_class))
        utils_conf.update_config_file('tagQTD', str(tagQTD))
        utils_conf.update_config_file('classificQTD', str(classificQTD))                
        
        print("##### Config Updated!")
        return jsonify('200')            
        
    
    return None

#--------------------------------------------------------------------------------------
@app.route('/api/downloadsearch')
def downloadsearch(filename='tweets_search_output.xlsx'):
    return send_from_directory('outputs', filename, as_attachment=True)

#--------------------------------------------------------------------------------------
@app.route('/api/downloadclassification')
def downloadclassification(filename='text_classification_output.xlsx'):
    return send_from_directory('outputs', filename, as_attachment=True)
  
#--------------------------------------------------------------------------------------    
if __name__ == '__main__':
    app.run(debug=True, port=8080)
