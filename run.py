from pymongo import MongoClient
from pymongo import UpdateOne
import pandas as pd


class MongoDao:
    def __init__(self):
        self._client = MongoClient(
            "mongodb+srv://admin:admin@incubytecluster.t51av.mongodb.net/incubyteDb?retryWrites=true&w=majority")
        self._db = self._client['incubyteDb']

    def __del__(self):
        self._client.close()

    def get_count(self, collection_name):
        return self._db['table_' + collection_name].count()

    def drop_collection(self, name):
        self._db['table_' + name].drop()

    def insert_country_values(self, country_name, documents):
        batch_doc = [UpdateOne({'_id': elem['_id']}, {'$set': elem['data']}, upsert=True) for elem in
                     documents]
        self._db['table_' + country_name].bulk_write(batch_doc, ordered=False)


class Main:
    def __init__(self):
        self.dao = MongoDao()

    def __del__(self):
        self.dao.__del__()

    def read_file(self, filename):
        df = pd.read_csv(filename, sep='|', index_col='customer_id')
        # dropping the first column H, which is an indicator for header row vs data row
        df = df.drop(['H'], axis=1)

        # getting unique country names
        unique_countries = df['country'].unique()
        print(unique_countries)

        for country in unique_countries:
            self.dao.drop_collection(country)
            print("creating collection for :" + str(country))
            df_country = df.loc[df['country'] == country]
            docs = [{'_id': k, 'data': v} for k, v in df_country.to_dict('index').items()]
            self.dao.insert_country_values(country, docs)
            print("records inserted for {0} : {1}".format(country, self.dao.get_count(country)))
            print("-----")


ob = Main()
ob.read_file('raw_data.csv')
