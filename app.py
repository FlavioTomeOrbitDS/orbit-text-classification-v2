from flask import Flask, request, jsonify,send_from_directory
from flask_cors import CORS
from tweets_search import tweets_search
from text_classification import text_classification, generate_js_dictionary
import pandas as pd
import utils_conf

app = Flask(__name__)
CORS(app)

@app.route('/')
def home():
    return 'Text Classification Backend!'

@app.route('/api/tweetssearch', methods=['POST'])
def tweetssearch():
    if request.is_json:
        # Get the JSON data
        data = request.get_json()    
        query = data.get('query', None)
        print(query)
        b_token = utils_conf.get_api_key('bearer_token')
        max_results = utils_conf.get_api_key('max_tweets')
        df = tweets_search(query,b_token,"lang:pt" ,max_results)    
        json_data = df.to_json(orient='records')
        
        return jsonify(json_data=json_data)            
        
    
    return None

@app.route('/api/getclassifications', methods=['GET'])
def getclassifications():
    
    # # Get the JSON data       
    df = text_classification()    
    json_data = df.to_json(orient='records')
    
    #df = pd.read_excel("outputs/text_classification_output.xlsx", sheet_name=1)
    json_data = df.to_json(orient='records')
    
    return jsonify(json_data=json_data)            

@app.route('/api/getchartdata', methods=['GET'])
def getchartdata():
    
    #df = pd.read_excel("outputs/text_classification_output.xlsx", sheet_name=1)    
    #json_data = df.to_json(orient='records')
    
    json_data = generate_js_dictionary()
    return jsonify(json_data=json_data)            

@app.route('/api/getkeys', methods=['GET'])
def getkeys():
    try:
        keys = []
        keys.append(utils_conf.get_api_key('bearer_token'))
        keys.append(utils_conf.get_api_key('OPENAI_KEY'))
        keys.append(utils_conf.get_api_key('max_tweets'))
        
        #print(f"#### Data sent {keys}")
    except:
        return jsonify("Erro ao carregar as APIs")            
        
    return jsonify(keys)          
@app.route('/api/setkeys', methods=['POST'])
def setkeys():
    if request.is_json:
        # Get the JSON data
        data = request.get_json()    
        print(f"##### Config received: {data}")
        twitter_key = data.get('twitter_key', None)
        openai_key = data.get('openai_key', None)
        max_tweets = data.get('max_tweets', None)
        
        utils_conf.saveApiKey('bearer_token', twitter_key)
        utils_conf.saveApiKey('OPENAI_KEY', openai_key)
        utils_conf.saveApiKey('max_tweets', str(max_tweets))
        
        print("##### Config Updated!")
        return jsonify('200')            
        
    
    return None

@app.route('/api/downloadsearch')
def downloadsearch(filename='tweets_search_output.xlsx'):
    return send_from_directory('outputs', filename, as_attachment=True)
  
    
if __name__ == '__main__':
    app.run(debug=True, port=8080)
