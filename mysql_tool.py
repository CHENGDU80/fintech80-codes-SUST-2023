import pymysql


class MysqlTool:
    def __init__(self):
        """mysql 连接初始化"""
        self.host = 'localhost'
        self.port = 3306
        self.user = 'root'
        self.password = '12345678'
        self.db = 'FINTECH80'
        self.charset = 'utf8'
        self.mysql_conn = None
        self.__enter__()

    def __enter__(self):
        """打开数据库连接"""
        try:
            self.mysql_conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                passwd=self.password,
                db=self.db,
                charset=self.charset
            )
            print('数据库连接成功')
        except:
            print('数据库连接失败')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """关闭数据库连接"""
        if self.mysql_conn:
            self.mysql_conn.close()
            self.mysql_conn = None

    def insert(self, args):
        sql = "INSERT INTO news (title, content, released_time, src, channel) VALUES (%s, %s, %s, %s, %s)"
        self.execute(sql, args, commit=True)

    def getId(self):
        sql = "SELECT max(id) from news;"
        return self.execute(sql, commit=False)[0][0]

    def getLastTime(self):
        sql = "SELECT max(released_time) from news;"
        return self.execute(sql, commit=False)[0][0]


    def execute(self, sql: str, args: tuple = None, commit: bool = False) -> any:
        """执行 SQL 语句"""
        try:
            with self.mysql_conn.cursor() as cursor:
                cursor.execute(sql, args)
                if commit:
                    self.mysql_conn.commit()
                    print(f"执行 SQL 语句：{sql}，参数：{args}，数据提交成功")
                else:
                    result = cursor.fetchall()
                    print(f"执行 SQL 语句：{sql}，参数：{args}，查询到的数据为：{result}")
                    return result
        except Exception as e:
            print(f"执行 SQL 语句出错：{e}")
            self.mysql_conn.rollback()
            raise e



# if __name__ == '__main__':
#     db = MysqlTool()
#     # 查询所有数据
#     # title = '贵州遵义：认房不认贷，鼓励实力较强的央企、国企到遵拿地'
#     # content = '贵州遵义：认房不认贷，鼓励实力较强的央企、国企到遵拿地贵州遵义：认房不认贷，鼓励实力较强的央企、国企到遵拿地贵州遵义：认房不认贷，鼓励实力较强的央企、国企到遵拿地'
#     # src = 'sina'
#     # channel = '[{"id":"1","name": "宏观"}]'
#     # released_time = '2023-10-14 13:57:32'
#     # args = (title, content, released_time, src, channel)
#     # db.insert(args)
#     print(db.getId())
        