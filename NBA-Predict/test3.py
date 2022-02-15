from sklearn import datasets 
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn import metrics 

 

source, target =datasets.load_iris(return_X_y=True)
print(source.shape)
train_s, test_s, train_t, test_t = train_test_split(source, target)

knn=KNeighborsClassifier(n_neighbors=3)
knn.fit(train_s,train_t)
test_prediction=knn.predict(test_s)

z=metrics.accuracy_score(test_t,test_prediction)

z=['wenbo','ming']
z.sort()
print(z)
from newsapi import NewsApiClient

# Init
newsapi = NewsApiClient(api_key='API_KEY')

# /v2/top-headlines
top_headlines = newsapi.get_top_headlines(q='bitcoin',sources='bbc-news,the-verge',category='business',language='en',country='us')



# /v2/everything
all_articles = newsapi.get_everything(q='bitcoin',
                                      sources='bbc-news,the-verge',
                                      domains='bbc.co.uk,techcrunch.com',
                                      from_param='2017-12-01',
                                      to='2017-12-12',
                                      language='en',
                                      sort_by='relevancy',
                                      page=2)

# /v2/top-headlines/sources
sources = newsapi.get_sources()