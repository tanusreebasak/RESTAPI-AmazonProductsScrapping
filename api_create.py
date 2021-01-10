from flask import Flask,jsonify,request
from flask_restful import Api,Resource
import csv,os
from bs4 import BeautifulSoup
import requests
import unicodedata
import pymongo
import urllib,json
from api_constants import mongodb_password
from bson import ObjectId
from bson.json_util import dumps
import datetime


app = Flask(__name__)


################# Building the mongodb connection #####
database_name = "AmazonScrappedDatabase"
DB_URI="mongodb+srv://{}:{}@amazon-products-data.zjtih.mongodb.net/{}?retryWrites=true&w=majority".format(urllib.parse.quote('tanusree'),urllib.parse.quote(mongodb_password),database_name)

client = pymongo.MongoClient(DB_URI)
db = client['AmazonScrappedDatabase']
collection = db['AmazonScrappedCollection']
########################################################

@app.route("/api/scrap",methods=['POST'])  # #Adding endpoint for scrapping data
  
def scrap():
    url = request.args['url']         
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64: x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    resp = requests.get(url,headers=headers)
    s=BeautifulSoup(resp.content)
    '''Product name'''
    product_title = s.select("#productTitle")[0].get_text().strip()
    product_title = unicodedata.normalize("NFKD",product_title)
    '''Product Price'''
    product_price = s.select("#priceblock_ourprice")[0].get_text().strip()
    product_price = unicodedata.normalize("NFKD",product_price)    
    '''Rating'''
    rating = s.select("#acrCustomerReviewText")[0].get_text()
    '''Description'''
    description = s.select("#feature-bullets")[0].get_text().strip()
    description = description.replace('\n','')
    '''Timestamp'''
    ct = datetime.datetime.now() 
    scraped_data = {'url':url,'product_name':product_title,'price': product_price,'rating': rating,'description':description,'timestamp': ct}  
    return(scraped_data)


@app.route("/api/scrap/add",methods=['POST'])  # Adding endpoint for adding to database
def addToDB():
    url = request.args['url']
    payload = scrap()    
    ct = datetime.datetime.now()     
    payload['timestamp'] =  ct      
    collection.insert_one(payload)
    return('Suucessfully added the payload to the MongoDB database')
    

@app.route("/api/get",methods=['GET'])  # Adding endpoint for fetching all the document
def get():
    documents = collection.find()
    response = []
    for document in documents:
        document['_id'] = str(document['_id'])
        document['timestamp'] = str(document['timestamp'])
        response.append(document)
    return jsonify(response)

@app.route("/api/get/url",methods=['GET'])  # Adding endpoint to filter out the payload based on url
def get_document_by_url():
    url = request.args['url']         
    documents = collection.find({'url': url})
    response = []
    for document in documents:
        document['_id'] = str(document['_id'])
        document['timestamp'] = str(document['timestamp'])
        response.append(document)
    return jsonify(response)

@app.route("/api/update/price",methods=['PUT'])  # Adding endpoint to update price
def put_price():
    url = request.args['url']
    data = scrap()    
    price = data['price']
    current_price={'$set':{'price':price}}        
    documents = collection.find_one({'url': url},{'price':1})
    price_document = documents['price']       
    document_price ={'price': price_document}
    if(price == price_document):
        return('Price of the product in the database ={} and current price from scrapped data is also {}. No update required'.format(price,price_document))
    else:
        collection.update_one(document_price,current_price)
        return('Scraped price is {} and price of the product in database is {} . Successfully updated the price with recent scraped data'.format(price,price_document))

@app.route("/api/update/rating",methods=['PUT'])  # Adding endpoint to update rating
def put_rating():
    url = request.args['url']
    data = scrap()    
    rating = data['rating']
    current_rating={'$set': {'rating':rating}}        
    documents = collection.find_one({'url': url},{'rating':1})
    rating_document = documents['rating']
    print('Scraped rating',rating)
    print('Database rating',rating_document)      
    document_rating ={'rating': rating_document}
    if(rating == rating_document):
        return('Rating of the product in the database = {} and current rating from scrapped data is also {}. No update required'.format(rating,rating_document))
    else:
        collection.update_one(document_rating,current_rating)
        return('Scraped rating is {} and rating of the product in database is {} . Successfully updated the rating with recent scraped data'.format(rating,rating_document))

if __name__=="__main__":
    #app.run(debug=True)
    app.run(host="0.0.0.0",port=5000)
    
