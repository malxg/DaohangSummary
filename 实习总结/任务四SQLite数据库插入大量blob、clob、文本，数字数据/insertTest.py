# summon 汇总
# ***********************************************************************************
import sqlite3, os
import datetime
import time  # 加入其中sleep()延迟函数，防止数据库未关闭，就运行修改后缀名函数造成报错
import codecs


# ************************************************************************************************************************
# 数据库插入大量数据blob,slob，文本，数字（百万级）***********************************************************************************************************************

# 创建tablename表,和其statname字段,用以储存该stattype数据
def buildtable(sqliteConnection, cursor, tablename, statname, stattype):
    try:
        # 创建tiles表
        sql = 'CREATE TABLE ' + tablename + ' (id integer,' + statname + ' ' + stattype + ', uptime text)'
        cursor.execute(sql)
        sqliteConnection.commit()
        print(tablename + ' table build')
    except sqlite3.Error as error:
        print(tablename + ' table exsit ', error)
    finally:
        pass


# 转文件为二进制格式
def convertToBinaryData(filename):
    # Convert digital data to binary format
    with open(filename, 'rb') as file:
        blobData = file.read()
    return blobData


# 将clob文本数据,由繁体转化为简体
def hant_2_hans(hant_str: str):
    '''
    Function: 将 hant_str 由繁体转化为简体
    '''
    return zhconv.convert(hant_str, 'zh-hans')


# 1、插入blob数据：将blob数据如图片文件插入到生成的./InsertTest.db数据库文件中blobTest表,填充blobTest表
def insertblobTest(sqliteConnection, cursor, filepath):
    buildtable(sqliteConnection, cursor, 'blobTest', 'blobStats', 'blob')  # 创建blobTest表
    # 开始从文件填充表
    try:
        filesname = os.listdir(filepath)  # 得到文件夹下的所有文件名称
        # print(filesname)
        try:
            for i in range(0, len(filesname)):
                # 开始填入数据到表
                sqlite_insert_blob_query = """ INSERT INTO blobTest
                                          (id, blobStats,uptime) VALUES (?, ?, ?)"""
                empPhoto = convertToBinaryData(filepath + '/' + filesname[i])  # 转blob文件为二进制格式
                # Convert data into tuple format
                data_tuple = (i + 1, empPhoto, datetime.datetime.now())
                cursor.execute(sqlite_insert_blob_query, data_tuple)
                sqliteConnection.commit()
        except sqlite3.Error as error:
            print('error:', error)
    except sqlite3.Error as error:
        print('error:', error)


# 2、插入clob数据：
def insertclobTest(sqliteConnection, cursor, filepath):
    buildtable(sqliteConnection, cursor, 'clobTest', 'clobStats', 'clob')  # 创建clobTest表
    # 开始从文件下文本文件.txt填充表
    try:
        filesname = os.listdir(filepath)  # 得到文件夹下的所有文件名称
        # print(filesname)
        count = 1
        try:
            for x in filesname:
                # 打开文件地址下的这些.txt文件，并以clob格式读入数据库
                fp = filepath + "/" + x
                f = codecs.open(fp, 'r', encoding="utf8")
                line_num = 1
                line = f.readline()
                while line:
                    # 开始填入数据到表
                    sqlite_insert_blob_query = """ INSERT INTO clobTest
                                              (id, clobStats,uptime) VALUES (?, ?, ?)"""
                    # Convert data into tuple format
                    data_tuple = (count, line, datetime.datetime.now())
                    cursor.execute(sqlite_insert_blob_query, data_tuple)
                    sqliteConnection.commit()

                    line_num = line_num + 1;
                    count += 1
                    line = f.readline()
        except sqlite3.Error as error:
            print('error:', error)
    except sqlite3.Error as error:
        print('error:', error)


# 3、插入text数据：
def inserttextTest(sqliteConnection, cursor, filepath):
    buildtable(sqliteConnection, cursor, 'textTest', 'textStats', 'text')  # 创建textTest表
    # 开始从文件下文本文件.txt填充表
    try:
        filesname = os.listdir(filepath)  # 得到文件夹下的所有文件名称
        # print(filesname)
        count = 1
        try:
            for x in filesname:
                # 打开文件地址下的这些.txt文件，并以clob格式读入数据库
                fp = filepath + "/" + x
                f = codecs.open(fp, 'r', encoding="utf8")
                line_num = 1
                line = f.readline()
                while line:
                    # 开始填入数据到表
                    sqlite_insert_blob_query = """ INSERT INTO textTest
                                              (id, textStats,uptime) VALUES (?, ?, ?)"""
                    # Convert data into tuple format
                    data_tuple = (count, line, datetime.datetime.now())
                    cursor.execute(sqlite_insert_blob_query, data_tuple)
                    sqliteConnection.commit()

                    line_num = line_num + 1;
                    count += 1
                    line = f.readline()
        except sqlite3.Error as error:
            print('error:', error)
    except sqlite3.Error as error:
        print('error:', error)


# 4、插入integer(number)数据：
def insertintTest(sqliteConnection, cursor, filepath):
    buildtable(sqliteConnection, cursor, 'intTest', 'intStats', 'integer')  # 创建integerTest表
    # 开始从文件下文本文件.txt填充表
    try:
        filesname = os.listdir(filepath)  # 得到文件夹下的所有文件名称
        # print(filesname)
        count = 1
        try:
            for x in filesname:
                # 打开文件地址下的这些.txt文件，并以clob格式读入数据库
                fp = filepath + "/" + x
                f = codecs.open(fp, 'r', encoding="utf8")
                line_num = 1
                line = f.readline()
                while line:
                    # 开始填入数据到表
                    sqlite_insert_blob_query = """ INSERT INTO intTest
                                              (id, intStats,uptime) VALUES (?, ?, ?)"""
                    # Convert data into tuple format
                    data_tuple = (count, line, datetime.datetime.now())
                    cursor.execute(sqlite_insert_blob_query, data_tuple)
                    sqliteConnection.commit()

                    line_num = line_num + 1;
                    count += 1
                    line = f.readline()
        except sqlite3.Error as error:
            print('error:', error)
    except sqlite3.Error as error:
        print('error:', error)

if __name__ == '__main__':
    # 记录运行时间
    starttime = datetime.datetime.now()

    try:
        DatabasePath = r'.\InsertreadTest.db'#这里创建在当前目录，也可指定如D:\中导航\Convertmbtiles\InsertreadTest.db
        # 连接数据库，若没有则自动创建
        sqliteConnection = sqlite3.connect(DatabasePath)
        cursor = sqliteConnection.cursor()

        flag = -1
        flag = int(input("请输入要实现的目标\n" + \
                         "1 insert blob数据\n" + \
                         "2 insert clob数据\n" + \
                         "3 insert text数据\n" + \
                         "4 insert integer数据\n"))
        if flag == 1:  # 1、insert blob数据
            filepath = r'.\blobTestfile'#这里创建在当前目录，也可指定如：D:\中导航\Convertmbtiles\blobTestfile
            # 1、insert blob数据：将blob数据如图片文件插入到生成的./InsertTest.db数据库文件中blobTest表
            insertblobTest(sqliteConnection, cursor, filepath)  # 创建，并填充blobTest表
        elif flag == 2:
            filepath = r'.\clobTestfile'#这里创建在当前目录，也可指定如D:\中导航\Convertmbtiles\clobTestfile
            # 2、insert clob数据：
            insertclobTest(sqliteConnection, cursor, filepath)  # 创建，并填充clobTest表
        elif flag == 3:
            filepath = r'.\textTestfile'#这里创建在当前目录，也可指定如D:\中导航\Convertmbtiles\textTestfile
            # 3、insert text数据：
            inserttextTest(sqliteConnection, cursor, filepath)  # 创建，并填充textTest表
        elif flag == 4:
            filepath = r'.\intTestfile'#这里创建在当前目录，也可指定如D:\中导航\Convertmbtiles\intTestfile
            # 4、insert integer(number)数据：
            insertintTest(sqliteConnection, cursor, filepath)  # 创建，并填充intTest表
        else:
            print("order is error!")

    except sqlite3.Error as error:
        print("Failed:", error)
    finally:
        if sqliteConnection:
            time.sleep(1)  # 加入其中sleep()延迟函数，防止数据库未插入结束，就关闭数据库造成报错
            sqliteConnection.close()  # 关闭数据库
            print("the sqlite connection is closed")
    print('end')

    endtime = datetime.datetime.now()
    print(endtime - starttime)