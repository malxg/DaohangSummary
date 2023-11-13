import sqlite3
import datetime

def writeTofile(data, filename):
    # Convert binary data to proper format and write it on Hard Disk
    with open(filename, 'wb') as file:
        file.write(data)
    print("Stored blob data into: ", filename, "\n")


# 读取各个格式的数据,如果数据为blob格式的图片,即IsBLOBpic==True则默认读取并保存该图片在当前目录下面
def readData(sqliteConnection, cursor, tablename, empId, IsBLOBpic=False, photoPath='.'):
    try:
        sql_fetch_blob_query = "SELECT * from " + tablename + " where id = ?"
        cursor.execute(sql_fetch_blob_query, (empId,))
        record = cursor.fetchall()
        inquireTime = datetime.datetime.now()
        print(record, '\n查询时间：', inquireTime)
        if IsBLOBpic == True:
            for row in record:
                idd = row[0]
                photo = row[1]
                t = photo
                formatt = ''.join(list(str(t[1:5]))[2:5]).lower()
                # print("Storing employee image on disk \n")
                photoPath = photoPath + '/' + str(idd) + '.' + formatt
                # print(photoPath)
                writeTofile(photo, photoPath)
        cursor.close()

    except sqlite3.Error as error:
        print("Failed to read " + tablename + " data from sqlite table", error)


# 在单表查询基础上四个表的合并表的联立查询
def multreadData(sqliteConnection, cursor, empId, IsBLOBpic=False, photoPath='.'):
    try:
        sql_fetch_blob_query = "SELECT * FROM((blobTest INNER JOIN clobTest  ON blobTest.id = clobTest.id)" + \
                               "INNER JOIN textTest  ON blobTest.id = textTest.id)" + \
                               "INNER JOIN intTest  ON blobTest.id = intTest.id where blobTest.id=?"
        cursor.execute(sql_fetch_blob_query, (empId,))
        record = cursor.fetchall()
        inquireTime = datetime.datetime.now()
        print(record, '\n查询时间：', inquireTime)
        if IsBLOBpic == True:
            for row in record:
                idd = row[0]
                photo = row[1]
                t = photo
                formatt = ''.join(list(str(t[1:5]))[2:5]).lower()
                # print("Storing employee image on disk \n")
                photoPath = photoPath + '/' + str(idd) + '.' + formatt
                # print(photoPath)
                writeTofile(photo, photoPath)
        cursor.close()

    except sqlite3.Error as error:
        print("Failed to read  data from sqlite table", error)

if __name__ == '__main__':
    DatabasePath=r'.\InsertreadTest.db'#这里是在当前目录，也可指定如要读取的数据库地址，样例：r'D:\中导航\Convertmbtiles\InsertTest.db'
    sqliteConnection = sqlite3.connect(DatabasePath)
    cursor = sqliteConnection.cursor()

    flag = -1
    flag = int(input("请输入要实现的目标\n"+\
                 "1 read blob数据\n"+\
                 "2 read clob数据\n"+\
                 "3 read text数据\n"+\
                 "4 read integer数据\n"+\
                 "5 mult read Data数据\n"))
    empId = int(input('输入读取该数据的多少行'))
    if flag == 1:
        #1、读取id为empId的那一行的blob数据，如果是图片数据，则保存该图片在photoPath路径下面,若不是则IsBLOBpic，photoPath不用填写，保存默认
        photoPath=r'.'#这里保存在当前目录，也可指定如blob图片保存地址样例：r'D:\中导航\Convertmbtiles'
        readData(sqliteConnection,cursor,'blobTest',empId=empId,IsBLOBpic=True,photoPath=photoPath)
    elif flag == 2:
        #2、读取id为empId的那一行的clob数据
        readData(sqliteConnection,cursor,'clobTest',empId=empId)
    elif flag == 3:
        #3、读取id为empId的那一行的text数据
        readData(sqliteConnection,cursor,'textTest',empId=empId)
    elif flag == 4:
        #4、读取id为empId的那一行的int数据
        readData(sqliteConnection,cursor,'intTest',empId=empId)
    elif flag == 5:
        photoPath=r'.'#这里保存在当前目录，也可指定如blob图片保存地址样例：r'D:\中导航\Convertmbtiles'
        #5、读取id为empId的那一行的四个表的合并表
        multreadData(sqliteConnection,cursor,empId=empId,IsBLOBpic=True,photoPath=photoPath)
    else:
        print("Serial number error!")

    #关闭数据库
    if sqliteConnection:
        sqliteConnection.close()
        print("sqlite connection is closed")