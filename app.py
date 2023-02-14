from flask import Flask, jsonify
from flask_restful import Resource, Api
from sqlalchemy import create_engine
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
import json

todayx = datetime.today()
emailsendingtime = todayx.strftime("%d/%m/%Y")
emailtime1 = todayx.strftime("%d/%m/%Y (05:00:pm)")
data2 = datetime.today() - timedelta(hours=24, minutes=0)
emailtime2 = data2.strftime("%d/%m/%Y (05:00:pm)")
sqltoday = todayx.strftime("%Y-%m-%d 16:59:59.999999")
sqlbackto24h = data2.strftime("%Y-%m-%d 16:59:59.999999")
db_connection_str = 'mysql+pymysql://root:root@localhost:3306/touchfree'
db_connection = create_engine(db_connection_str)
Live_db_connection_str = 'mysql+pymysql://kapraadmin:802710062V@139.162.50.185:3306/tf_iot'
db_connection = create_engine(Live_db_connection_str)
print('starttt')
df = pd.read_sql('SELECT * FROM 01_history', con=Live_db_connection_str)
df2 = pd.read_sql('SELECT * FROM 01_history_data', con=Live_db_connection_str)
df3 = pd.read_sql('SELECT * FROM 01_history_settings', con=Live_db_connection_str)
df2.rename(columns={'id': 'history_data'}, inplace=True)
df3.rename(columns={'id': 'history_settings'}, inplace=True)
mergedf = pd.merge(df, df2, how='left', on='history_data')
newmergedf = pd.merge(df3, mergedf, how='right', on='history_settings')
print('done')
finalDF = newmergedf[(newmergedf["created_on"] > sqlbackto24h)
                     & (newmergedf["created_on"] < sqltoday)]
print(finalDF)

mydb = mysql.connector.connect(
    host="localhost", user="root", password="root", database="touchfree")
mycursor = mydb.cursor()
mycursor.execute("SELECT * FROM  test")
myresult = mycursor.fetchall()
# print('new', myresult)
# print('done')
# for row in myresult:
#     print(row)

app = Flask(__name__)
api = Api(app)

fakeDatabase = {
    1: {'name': 'Clean car'},
    2: {'name': 'Write blog'},
    3: {'name': 'Start stream'},
}


class Items(Resource):
    def get(self):
        return myresult


class Item(Resource):
    def get(self, pk):
        return fakeDatabase[pk]


# class Ids(Resource):
#     def get(self, ids):
#         print('test', ids)
#         print('kooooyakooooooooooooooooooooooooooooooooooo')
#         mycursor = mydb.cursor()
#         # mycursor.execute("SELECT * FROM  test")
#         mycursor.execute("SELECT * FROM test WHERE id =%s", [ids])
#         myresult = mycursor.fetchall()
#         return myresult

class Ids(Resource):
    def get(self, ids):
        print('test', ids)

        print('kooooyakooooooooooooooooooooooooooooooooooo')
       
        finalDF = newmergedf[(newmergedf["created_on"] > sqlbackto24h) & (
            newmergedf["created_on"] < sqltoday)]
        

        value = '4C:75:25:09:E4:28'
        # value = ids
        selected_rows = finalDF[finalDF['access_key'] == value]
        orderdf = selected_rows.reset_index()
        lengthx = len(orderdf)

        orderCount = 0
        orderx = []

        while orderCount < lengthx:
            orderx.append(orderCount)
            orderCount = orderCount +1

        ser = pd.Series(orderx)
        primaryKeyDF = pd.DataFrame(ser)

        primaryKeyDF = pd.DataFrame(orderx)
        orderdf['nprimarykey'] = primaryKeyDF
        orderdf.to_sql('orderdf', db_connection_str, index=False, if_exists='replace')
        mergDFforFunction = pd.read_sql("SELECT t.* FROM touchfree.orderdf AS t LEFT JOIN touchfree.orderdf AS tsub ON t.nprimarykey = tsub.nprimarykey + 1  WHERE !(t.`function` <=> tsub.`function`) or !(t.`alarm` <=> tsub.`alarm`) or !(t.`status` <=> tsub.`status`) ORDER BY t.id;",con=db_connection_str)
        outdf = jsonify(mergDFforFunction.to_dict())
        return outdf
        


api.add_resource(Items, '/')
# api.add_resource(Item, '/<int:pk>')
api.add_resource(Ids, '/<int:ids>')
if __name__ == '__main__':
    app.run(debug=True)
