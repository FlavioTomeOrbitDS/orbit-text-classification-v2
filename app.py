from flask import Flask, request, jsonify,send_from_directory # type: ignore
from flask_cors import CORS # type: ignore
from tweets_search import tweets_search
from text_classification import text_classification, generate_js_dictionary
import pandas as pd
import utils_conf

app = Flask(__name__)
CORS(app)

@app.route('/')
def home():    
    
    return 'Text Classification Backend!'
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
def getclassifications():
    if request.is_json:    
        data = request.get_json()    
        context = data.get('context', None)
        tema = data.get('tema', None)        
        # # Get the JSON data       
        df = pd.read_excel('outputs/tweets_search_output.xlsx')    
        result = text_classification(df, context, tema)   
        if result["status"] == "error":
            return jsonify({"status": "error", "message": result["message"]}), 500 
        
        df = pd.read_excel("outputs/text_classification_output.xlsx", sheet_name=1)                        
        json_data = df.to_json(orient='records')
                    
    return jsonify(json_data=json_data)            
#--------------------------------------------------------------------------------------
@app.route('/api/getclassificationsbyfile', methods=['POST'])
def getclassificationsbyfile():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    if file:
        try:
            df = pd.read_excel(file)            
            #context = data.get('context', None)
            #tema = data.get('tema', None)      
            context = ""
            tema = ""
            if 'Texto' in df.columns:
                textos = df['Texto']
                df = pd.DataFrame(textos)
                df = df.head(2000)
                text_classification(df, context, tema)   
                df = pd.read_excel("outputs/text_classification_output.xlsx", sheet_name=1)
                json_data = df.to_json(orient='records') 
                print("sendind json data!")
                return jsonify(json_data=json_data), 200
            else:
                return jsonify({"error": "Column 'Texto' not found"}), 400
        except Exception as e:
            return jsonify({"error": str(e)}), 500
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
