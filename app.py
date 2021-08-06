
import pandas as pd
import numpy as np
import ast
import nltk
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from sklearn.metrics.pairwise import pairwise_distances
import sys

def get_key_value(attribute, key):

    if attribute == None: 
        return {}

    if key in attribute:
        return ast.literal_eval(attribute.pop(key)) 
    else:
        return {}
def getDummiesForRestaurants(restaurants):
  
  restaurants['BusinessParking'] = restaurants.apply(
      lambda x: get_key_value(x['attributes'], 'BusinessParking'), axis=1)

  restaurants['Ambience'] = restaurants.apply(
      lambda x: get_key_value(x['attributes'], 'Ambience'), axis=1)

  restaurants['GoodForMeal'] = restaurants.apply(
      lambda x: get_key_value(x['attributes'], 'GoodForMeal'), axis=1)

  restaurants['DietaryRestrictions'] = restaurants.apply(
      lambda x: get_key_value(x['attributes'], 'DietaryRestrictions'), axis=1)

  restaurants['Music'] = restaurants.apply(
      lambda x: get_key_value(x['attributes'], 'Music'), axis=1)

  restaurants['BestNights'] = restaurants.apply(
      lambda x: get_key_value(x['attributes'], 'BestNights'), axis=1)


  restaurants = restaurants.dropna(subset=['attributes'])

  attributesDF = pd.concat([restaurants['attributes'].apply(pd.Series), 
                          restaurants['BusinessParking'].apply(pd.Series),
                          restaurants['Ambience'].apply(pd.Series),
                          restaurants['GoodForMeal'].apply(pd.Series), 
                          restaurants['DietaryRestrictions'].apply(pd.Series),
                          restaurants["Music"].apply(pd.Series),
                          restaurants['BestNights'].apply(pd.Series)], axis=1)
  
  attributesDF_dummies = pd.get_dummies(attributesDF)
  attributesDF_dummies.head()
  attributesDF_dummies = attributesDF_dummies.fillna(0)
  categoriesDF = pd.Series(restaurants['categories']).str.get_dummies(",")
  categoriesDF = categoriesDF.fillna(0)
  categoriesDF=categoriesDF*1
  attributesDF_dummies=attributesDF_dummies*1
  restaurantPropDF = restaurants[['name','stars','business_id','city']]
  final_df_restaurant = pd.concat([categoriesDF,
                                 attributesDF_dummies,restaurantPropDF], axis = 1)
  
  return final_df_restaurant

def getrating (friend_array,reviews_df,rec_basis):
  rating_df = reviews_df[reviews_df.user_id.isin(friend_array)]
  rating_df = rating_df[rating_df.business_id.isin(rec_basis['business_id'].to_list())]
  rating_df = rating_df[['business_id','stars']]
  rating_df = rating_df.groupby('business_id').mean().reset_index()
  ratingList = []
  for biz_id in rec_basis['business_id']:
    ratingList.append(rating_df[rating_df['business_id']== biz_id]['stars'].item())
  return ratingList

def getAverageSentimentForBusiness(business_id,reviews_df):
  # gc.collect()
  reviewsForBusinessDF = pd.DataFrame(reviews_df[reviews_df['business_id'] == str(business_id)])
  sentiment = SentimentIntensityAnalyzer()
  reviewsForBusinessDF["sentimentScore"] = reviewsForBusinessDF['text'].apply(lambda x: (sentiment.polarity_scores(x)['compound'] + 1) *(5/2))
  reviewsForBusinessDF['sentimentStars'] = reviewsForBusinessDF.apply(lambda x: x['stars'] + x['sentimentScore'],axis=1)

  return sum(reviewsForBusinessDF['sentimentStars'].tolist()) / reviewsForBusinessDF.shape[0]


def getSentimentRating(rec_basis,reviews_df):
  rec_basis['sentimentScore'] = rec_basis['business_id'].apply(lambda x: getAverageSentimentForBusiness(str(x),reviews_df))
  rec_basis['totalScore'] = rec_basis.apply(lambda x: x['stars'] + x['sentimentScore'],axis =1)

from pyathena import connect
import boto3
from pyathena.pandas.util import as_pandas

# To connect to AWS Athena
cursor = connect(aws_access_key_id='AKIAYHZDVL25C3GBT2VP',
                 aws_secret_access_key='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',
                 s3_staging_dir='s3://athena-query-self/',
                 region_name='ap-southeast-1').cursor()

id = 'ZJ6sj1IjdwmPPL_ZxmRKgw'
city = 'Airdrie'
num_rec = 20
num_rect = input("Number of recommendations you want (1-20): ")
while num_rec >20 or num_rec < 1:
  num_rect = input("Invalid input. Enter the number of recommendations you want: ")
  
id = input("Please enter a user ID: ")
print("Checking for user record.")
str_id="'"+id+"'"
str1 = ' SELECT friends FROM "sampledb"."userdata_yelp" WHERE user_id = '+str_id
cursor.execute(str1)
user_data = as_pandas(cursor)

if len(user_data.index) == 0:
  print("No record found.")
  sys.exit()

friend_array = list(user_data['friends'][0].split(','))

friend_array.append(id)
friend_id = str(tuple(friend_array))
str2 = ' SELECT * FROM "sampledb"."reviewdata_yelp" WHERE user_id IN '+friend_id

cursor.execute(str2)
review_data = as_pandas(cursor)

review_data_cp = review_data.copy()
review_data_cp = review_data_cp.drop_duplicates(subset='business_id')

biz_list = review_data_cp['business_id'].to_list()
biz_list_str = str(tuple(biz_list))
str3 = ' SELECT * FROM "sampledb"."businessdata_yelp" WHERE business_id IN '+biz_list_str
cursor.execute(str3)
biz_data = as_pandas(cursor)

biz_data = biz_data[biz_data['categories'].str.contains('Restaurant.*')==True].reset_index()
biz_data=biz_data.drop(columns = 'index')

reviews_basis = review_data.merge(biz_data[['business_id']], how='inner', on='business_id')
reviews_basis  = reviews_basis .drop_duplicates(subset=['review_id'])

rec_basis_crude = biz_data.merge(review_data[['business_id']], how='inner', on='business_id')
rec_basis_crude = rec_basis_crude.drop_duplicates(subset = ['business_id'])

reviews_basis = reviews_basis.merge(reviews_basis[['business_id']], how='inner', on='business_id')
reviews_basis = reviews_basis.drop_duplicates(subset=['review_id'])
print("User found.")
city = input("Enter city name: ")
city="'"+city+"'"
str4 = ' SELECT * FROM "sampledb"."businessdata_yelp" WHERE city= '+city

cursor.execute(str4)

biz_city = as_pandas(cursor)

if len(biz_city.index) == 0:
    print("No record found.")
    sys.exit()
print("City found. Gathering data.")
biz_city = biz_city[biz_city['categories'].str.contains('Restaurant.*')==True].reset_index()
biz_city=biz_city.drop(columns = 'index')
biz_list2 = biz_city['business_id'].to_list()
biz_list2_str = str(tuple(biz_list2))
str5 = ' SELECT * FROM "sampledb"."reviewdata_yelp" WHERE business_id IN '+biz_list2_str

cursor.execute(str5)

city_review = as_pandas(cursor)

city_review = city_review.merge(biz_city[['business_id']], how='inner', on='business_id')
city_review = city_review.drop_duplicates(subset=['review_id'])

biz_city = biz_city.merge(city_review[['business_id']], how='inner', on='business_id')
biz_city = biz_city.drop_duplicates(subset=['business_id'])

city_review = city_review.merge(biz_city[['business_id']], how='inner', on='business_id')
city_review = city_review.drop_duplicates(subset=['review_id'])

reviews_df = pd.concat([city_review,reviews_basis])

restaurants_df = pd.concat([biz_city,rec_basis_crude])

restaurants_df = getDummiesForRestaurants(restaurants_df)

print("Evaluating reviews... This may take some time.")

getSentimentRating(restaurants_df,reviews_df)

print("Review Evaluation Complete.")

restaurants_cp=restaurants_df.copy()

rec_basis = restaurants_cp.merge(rec_basis_crude[['business_id']], how='inner', on='business_id')
rec_basis = rec_basis.drop_duplicates(subset = ['business_id'])

rec_pool = restaurants_cp.merge(biz_city[['business_id']], how='inner', on='business_id')
rec_pool = rec_pool.drop_duplicates(subset = ['business_id'])

restaurants_df.iloc[:,:len(restaurants_df.columns)-6]
cos_sim = pairwise_distances (rec_pool.iloc[:,:len(restaurants_df.columns)-6],rec_basis.iloc[:,:len(restaurants_df.columns)-6],metric = "cosine")
cos_sim = pd.DataFrame(cos_sim, index=rec_pool.T.columns, columns=rec_basis.T.columns)
cos_sim = abs(abs(cos_sim)-1)

difference = np.subtract(getrating(friend_array,reviews_df,rec_basis),rec_basis['stars'].to_list())

cos_sim['sumproduct'] = cos_sim.copy().dot(difference)
cos_sim['sum'] = cos_sim.copy().drop(columns='sumproduct').sum(axis=1)
cos_sim['score']=cos_sim['sumproduct']/cos_sim['sum']
score = np.add(cos_sim['score'].to_list(),rec_pool['stars'].to_list())
  
rec_pool['score']=score

rec_pool_cp = rec_pool.copy()
cond = rec_pool['business_id'].isin(rec_basis['business_id'])
rec_pool_cp.drop(rec_pool_cp[cond].index, inplace = True)

  
recommendationList_name = rec_pool_cp.nlargest(num_rec,columns='score')['name'].to_list()
recommendationList_business_id = rec_pool_cp.nlargest(num_rec,columns='score')['business_id'].to_list()

print("Recommendation generated.")

print("Name: ", recommendationList_name)
print("ID: ", recommendationList_business_id)
