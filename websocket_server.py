import asyncio
import datetime
import json
import websockets
from mysql_tool import MysqlTool
import tushare as ts

mysql_tool = MysqlTool()
ts.set_token('5393c1a13564d31252ac2e6cea28a440a4e52549396ec810d7a02e7c')
pro = ts.pro_api()
# last_time = datetime.datetime.now()
# last_time = datetime.datetime(2023, 10, 31, 0, 5, 1)
delta_minute = 5
fmt = '%Y-%m-%d %H:%M:%S'


def getNews():
    #查询当前所有正常上市交易的股票列表
    src = 'sina'
    last_time = mysql_tool.getLastTime() + datetime.timedelta(seconds=1)
    start_date= datetime.datetime.strftime(last_time, fmt)
    end_date = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(minutes=delta_minute), fmt)
    print(start_date, end_date) 
    df = pro.news(src=src, start_date=start_date, end_date=end_date, fields='datetime,content,channels')
    print(len(df))
    # df.to_csv(f'{start_date}_{end_date}_{src}.csv')
    # last_time = datetime.datetime.strptime(end_date, fmt)
    return df

def deal(df):
    send_data = list()
    for index,row in df.iterrows():
        print(row['content'])
        left = row['content'].find('【')
        right = row['content'].find('】')
        print(left)

        if left != -1:
            title = row['content'][left+1:right]
            content = row['content'][right+1:]
        else:
            title = None
            content = row['content']
        args = (title, content, row['datetime'], 'sina', str(row['channels']))
        print(args)
        mysql_tool.insert(args)
        id = mysql_tool.getId()
        useful_data = {
            "title" : title,
            "id" : id,
        }
        send_data.append(json.dumps(useful_data, ensure_ascii=False))
    return '&'.join(send_data)

 
async def time(websocket, path):
    while True:
        print('This time to get news now.')
        df = getNews()
        send_data = deal(df)
        # print(send_data)
        if len(send_data)>0:
            await websocket.send(send_data)
        await asyncio.sleep(delta_minute*60)

start_server = websockets.serve(time, "127.0.0.1", 5678)
print(' ========= websocket running =========')
asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()
