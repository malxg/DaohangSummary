# summon 汇总
# ***********************************************************************************
import sqlite3, os
import datetime
import json
import re
import datetime
import time  # 加入其中sleep()延迟函数，防止数据库未关闭，就运行修改后缀名函数造成报错


# 创建metadata表
def buildmetadata(sqliteConnection, cursor):
    ##构造metadata,files表
    try:
        # 创建metadata表
        sql = '''CREATE TABLE metadata (name text, value text)'''
        cursor.execute(sql)
        sqliteConnection.commit()
        print('metadata build')
    except sqlite3.Error as error:
        print('metadata exsit ', error)


# 创建tiles表
def buildtiles(sqliteConnection, cursor):
    try:
        # 创建tiles表
        sql = '''CREATE TABLE tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob,uptime text)'''
        cursor.execute(sql)
        sqliteConnection.commit()
        print('tiles build')
    except sqlite3.Error as error:
        print('tiles exsit ', error)
    finally:
        pass


# ************************************************************************************************************************
# 一、从文件夹->实现创建->直接创建（地址）输出.db数据库(NEED)***********************************************************************************************************************
# 默认以左下角为原点,将瓦片图片文件形成./OrderTest.db文件
def fileTodb(filepath, DatabasePath='./OrderTest.db', bounds='-180.0,-90.0,180.0,90.0', \
             typee='baselayer', axis_origin='-180.0,-90.0', axis_positive_direction='RightUp'):
    # 转化图片为二进制格式
    def convertToBinaryData(filename):
        # Convert digital data to binary format
        with open(filename, 'rb') as file:
            blobData = file.read()
        return blobData

    # 从base.json中获取metadata表属性
    def forbasejson(jsonPath):
        f = open(jsonPath, 'r')
        content = f.read()
        a = json.loads(content)
        f.close()
        formatt = a["tileInfo"]['format'];
        description = a['description']
        version = a["currentVersion"];
        name = a["mapName"];
        tile_width = a["tileInfo"]['rows'];
        tile_height = a["tileInfo"]['cols'];
        # maxzoom = a["tileInfo"]["lods"][-1]['level'];minzoom=a["tileInfo"]["lods"][0]['level']
        tup = (formatt, description, version, name, tile_width, tile_height)
        return tup

    # 填充metadata表
    def fillmetadata(sqliteConnection, cursor, maxzoom, minzoom):
        # 开始填入metadata表的值***************************************************************************************
        formatt, description, version, name, tile_width, tile_height = \
            forbasejson(filepath + '/' + 'base.json')

        sql = '''INSERT INTO metadata(name,value) 
        VALUES ('bounds',?),('format',?),('description',?), ('version',?),
        ('type',?),('name',?),('tile_width',?),('tile_height',?),
        ('axis_origin',?),('axis_positive_direction',?),('maxzoom',?),('minzoom',?)'''
        # 以base.json里面的文件为主
        value_tuple = (bounds, str(formatt).lower(), description, version, \
                       typee, name, tile_width, tile_height, \
                       axis_origin, axis_positive_direction, maxzoom, minzoom)
        cursor.execute(sql, value_tuple)
        sqliteConnection.commit()

    # 填充tiles表
    def filltiles(sqliteConnection, cursor, maxzoom, minzoom):
        # 开始映射tiles表****************************************************************************************
        try:
            i = j = k = 0
            formatt = ' '
            # 开始tiles表的映射
            print('开始tiles表的映射......')
            for i in range(minzoom, maxzoom + 1):
                path1 = filepath + '/' + str(i)  # 文件夹目录
                files1 = os.listdir(path1)  # 得到文件夹下的所有文件名称
                # print(files1)
                orders = pow(2, i)
                try:
                    for j in range(orders - 1, -1, -1):
                        path2 = path1 + '/' + str(j)  # 文件夹目录
                        if formatt == ' ':
                            files2 = os.listdir(path2)  # 得到文件夹下的所有文件名称
                            pic = files2[0]
                            formatt = pic[pic.index('.') + 1:len(pic)]
                            # print(formatt)
                        try:
                            for k in range(0, orders):
                                # tiles表开始填入数据
                                sqlite_insert_blob_query = """ INSERT INTO tiles
                                                          (zoom_level, tile_column, tile_row, tile_data,uptime) VALUES (?, ?, ?, ?,?)"""
                                empPhoto = convertToBinaryData(path2 + '/' + str(k) + '.' + formatt)
                                # Convert data into tuple format
                                data_tuple = (i, k, (orders - 1) - j, empPhoto, datetime.datetime.now())
                                cursor.execute(sqlite_insert_blob_query, data_tuple)
                                sqliteConnection.commit()
                        except sqlite3.Error as error:
                            print('error,映射tiles表出错,找不到文件,', k, '列下标图片是否连续？', error)
                except sqlite3.Error as error:
                    print('error,映射tiles表出错,找不到文件,', j, '行文件数是否连续？', error)
            print('tiles表映射结束。')

        except sqlite3.Error as error:
            print('error,映射tiles表出错', i, '级数文件是否连续？', error)

    # 确定实际maxzoom,minzoom并调用函数填充files表和metadata表的内容
    def fillTable(sqliteConnection, cursor):
        files = os.listdir(filepath)  # 得到文件夹下的所有文件名称
        # 获取级数的文件名称，非级数文件忽略
        temp = []
        for x in files:
            try:
                temp.append(int(x))
            except:
                continue
            finally:
                pass
        maxzoom = max(temp);
        minzoom = min(temp)
        # print(maxzoom,minzoom)

        # 填充metadata表
        fillmetadata(sqliteConnection, cursor, maxzoom, minzoom)
        # 填充tiles表
        filltiles(sqliteConnection, cursor, maxzoom, minzoom)

    # 构建数据库在DatabasePath路径和名字如：./OrderTest.db
    def buildDatabse(DatabasePath):
        try:
            # 连接数据库，若没有则自动创建
            sqliteConnection = sqlite3.connect(DatabasePath)
            cursor = sqliteConnection.cursor()

            # 创建metadata,files表
            buildmetadata(sqliteConnection, cursor)
            buildtiles(sqliteConnection, cursor)

            # 确定实际maxzoom,minzoom并调用函数填充files表和metadata表的内容
            fillTable(sqliteConnection, cursor)

        except sqlite3.Error as error:
            print("Failed to insert blob data into sqlite table", error)
        finally:
            if sqliteConnection:
                sqliteConnection.close()
                print("the sqlite connection is closed")

    buildDatabse(DatabasePath)


# ************************************************************************************************************************
# 二、从左上角.db数据库变为左下角.db数据库
# 对db文件,转化tiles_data图片原点左上角转为左下角
def leftupToleftdown(dbpath, orderTableName, \
                     zoom_level='zoom_level', tile_column='tile_column', tile_row='tile_row', tile_data='tile_data', \
                     bounds='-180.0,-90.0,180.0,90.0', description='description', version='1.3', typee='baselayer',
                     name='Layers', tile_width=256, tile_height=256, axis_origin='-180.0,-90.0',
                     axis_positive_direction='RightUp'):
    sqliteConnection = sqlite3.connect(dbpath)  # 1.打开数据库，获得连接对象
    cursor = sqliteConnection.cursor()  # 2.获得数据库的操作游标
    # 保证tiles的唯一性
    if orderTableName == 'tiles':
        # 将其改名
        sql = '''ALTER TABLE tiles RENAME TO tiles_origin'''
        cursor.execute(sql)
        sqliteConnection.commit()
        orderTableName = 'tiles_origin'
        print('由于原表叫tiles，为避重复,故已将原表改为tiles_origin')
    else:
        pass

    # 构造metadata,tiles表
    buildmetadata(sqliteConnection, cursor)
    buildtiles(sqliteConnection, cursor)

    # 复制orderTableName表内容到tiles表后面修正
    print('开始复制目标表为tiles表...')
    sql = '''INSERT INTO tiles (zoom_level, tile_column, tile_row,tile_data)
            SELECT ''' + zoom_level + ',' + tile_column + ',' + tile_row + ',' + tile_data + ' FROM ' + orderTableName
    # print(sql)
    cursor.execute(sql)
    sqliteConnection.commit()
    print('复制完成')

    # 填充metadata表*************************************************************************************************************
    # 判断metadata表是否为空，若为空则需要插入属性值，否则无需
    sql = '''SELECT name FROM metadata where Rowid=1'''
    cursor.execute(sql)
    ll = cursor.fetchall()
    if len(ll) != 0:  # 若有值则跳过
        print('metadata本身有值,这里不进行填入值')
    else:  # 若为空则填入属性值
        print('填入metadata属性值')
        # 1、获取maxzoom、minzoom
        sql = '''SELECT max(zoom_level),min(zoom_level) FROM tiles'''
        cursor.execute(sql)
        ll = cursor.fetchall()
        maxzoom = ll[0][0];
        minzoom = ll[0][1]
        # print(maxzoom,minzoom)
        # 2、获取format
        formatt = ''
        sql = '''SELECT tile_data FROM tiles where Rowid=1'''
        cursor.execute(sql)
        tile_data1 = cursor.fetchall()
        t = tile_data1[0][0]
        formatt = ''.join(list(str(t[1:5]))[2:5]).lower()
        # print(formatt)
        # 开始插入各个属性值
        sql = '''INSERT INTO metadata(name,value) 
        VALUES ('bounds',?),('format',?),('description',?), ('version',?),
        ('type',?),('name',?),('tile_width',?),('tile_height',?),
        ('axis_origin',?),('axis_positive_direction',?),('maxzoom',?),('minzoom',?)'''
        value_tuple = (bounds, formatt, description, version, \
                       typee, name, tile_width, tile_height, \
                       axis_origin, axis_positive_direction, maxzoom, minzoom)
        cursor.execute(sql, value_tuple)
        sqliteConnection.commit()
    print('填入结束')
    # 开始填充tiles表***********************************************************************************************************
    print('开始tiles表的映射......')
    sql = 'update tiles set tile_data = (select ' + tile_data + ' from ' + orderTableName + ' where ' + \
          zoom_level + '=tiles.zoom_level and ' + tile_column + '=tiles.tile_column and ' + \
          tile_row + '=(1<<tiles.zoom_level)-1-tiles.tile_row)'
    # print(sql)
    cursor.execute(sql)
    sqliteConnection.commit()
    print('tiles表映射结束。')

    # 关闭数据库
    sqliteConnection.close()
    try:
        sqliteConnection.close()
    except:
        pass


# 重命名数据库表格字段名函数,parameter:sqliteConnection,cursor,tablename,colname,newcolname
def RenameCol(sqliteConnection, cursor, tablename, colname, newcolname):
    sql = 'alter table ' + tablename + ' rename column ' + colname + ' to ' + newcolname
    cursor.execute(sql)
    sqliteConnection.commit()


# ************************************************************************************************************************
# 三、传入数据库地址,变为mbtiles文件
# 规范化db文件成要求的格式
def CanonicalDB(dbpath, orderTableName='tiles', \
                zoom_level='zoom_level', tile_column='tile_column', tile_row='tile_row', tile_data='tile_data', \
                bounds='-180.0,-90.0,180.0,90.0', description='description', version='1.3', typee='baselayer',
                name='Layers', tile_width=256, tile_height=256, axis_origin='-180.0,-90.0',
                axis_positive_direction='RightUp'):
    sqliteConnection = sqlite3.connect(dbpath)  # 1.打开数据库，获得连接对象
    cursor = sqliteConnection.cursor()  # 2.获得数据库的操作游标

    # 形成表tiles************************************************************************************************************
    try:  # 在orderTableName表的基础上，修改
        if orderTableName != 'tiles':
            # 将orderTableName表其改名为标准tiles格式名
            sql = 'ALTER TABLE ' + orderTableName + ' RENAME TO tiles'
            cursor.execute(sql)
            sqliteConnection.commit()
            orderTableName = 'tiles'
        else:
            pass
    except sqlite3.Error as error:
        print('error!!!', error)
    # 规范表的字段
    if zoom_level == 'zoom_level' and tile_column == 'tile_column' and tile_row == 'tile_row' and \
            tile_data == 'tile_data':  # 判断字段是否都规范，不规范则更改
        pass
    else:  # 不规范，则规范各字段
        # 重命名数据库表格字段名函数,parameter:sqliteConnection,cursor,tablename,colname,newcolname
        RenameCol(sqliteConnection, cursor, orderTableName, zoom_level, 'zoom_level');
        RenameCol(sqliteConnection, cursor, orderTableName, tile_column, 'tile_column');
        RenameCol(sqliteConnection, cursor, orderTableName, tile_row, 'tile_row');
        RenameCol(sqliteConnection, cursor, orderTableName, tile_data, 'tile_data');

    # 形成metadata表*************************************************************************************************************
    buildmetadata(sqliteConnection, cursor)  # 创建metadata表
    # 填充metadata表
    # 1、获取maxzoom、minzoom
    sql = '''SELECT max(zoom_level),min(zoom_level) FROM tiles'''  # 从规范的tiles中的zoom_level列，获取到maxzoom、minzoom
    cursor.execute(sql)
    ll = cursor.fetchall()
    maxzoom = ll[0][0];
    minzoom = ll[0][1]
    # print(maxzoom,minzoom)
    # 2、获取format
    formatt = ''
    sql = '''SELECT tile_data FROM tiles where Rowid=1'''  # 得到tiles表中第一行tile_data的数据，从中得到数据类型
    cursor.execute(sql)
    tile_data1 = cursor.fetchall()
    t = tile_data1[0][0]  # 得到tiles表中第一行tile_data的数据t
    formatt = ''.join(list(str(t[1:5]))[2:5]).lower()  # 从数据t中拆分出展示其数据类型的字段,如：png#print(formatt)
    # 开始填充metadata表的各个属性值
    sql = '''INSERT INTO metadata(name,value) 
    VALUES ('bounds',?),('format',?),('description',?), ('version',?),
    ('type',?),('name',?),('tile_width',?),('tile_height',?),
    ('axis_origin',?),('axis_positive_direction',?),('maxzoom',?),('minzoom',?)'''
    value_tuple = (bounds, formatt, description, version, \
                   typee, name, tile_width, tile_height, \
                   axis_origin, axis_positive_direction, maxzoom, minzoom)
    cursor.execute(sql, value_tuple)
    sqliteConnection.commit()  # 提交数据库修改

    # 关闭数据库
    sqliteConnection.close()


# 修改文件的后缀名old_ext到新的后缀名new_ext
def batch_rename(file_dir, fname, old_ext, new_ext):
    list_file = os.listdir(file_dir)  # 返回指定目录
    for file in list_file:
        ext = os.path.splitext(file)  # 返回文件名和后缀
        if fname == ext[0] and old_ext == ext[1]:  # ext[1]是.doc,ext[0]是1
            newfile = ext[0] + new_ext
            os.rename(os.path.join(file_dir, file),
                      os.path.join(file_dir, newfile))
            break


# ************************************************************************************************************************
# 主函数
if __name__ == '__main__':

    flag = -1
    flag = int(input("请输入要实现的目标\n" + \
                     "1 从文件夹->实现创建->直接创建（地址）输出.db数据库(NEED)\n" + \
                     "2 从左上角.db数据库变为左下角.db数据库\n" + \
                     "3 入数据库及以左下角为原点的表,创建metadata和tiles，成为标准的.db文件\n请输入序号:"))

    # 记录运行时间
    starttime = datetime.datetime.now()

    if flag == 1:
        # 一、********************************从文件夹读取图片->实现创建数据库、tiles和metadata表->直接创建（地址）输出.db数据库(NEED)****************************************************************************************
        dbpath = r'D:\中导航\Convertmbtiles\OrderTest.db' # 输入数据库保存地址,若没有该数据库，则会自动创建在该目录下面
        fpath = r'D:\中导航\Convertmbtiles\GoogleMapNight1-7'  # 输入级数文件夹地址,用来作数据库的映射

        print('开始第1个功能：实现瓦片文件夹转化为.mbtiles文件')
        fileTodb(filepath=fpath, DatabasePath=dbpath)  # 默认以左下角为原点,将瓦片图片文件形成名为OrderTest.db路径文件。parameters: filepath

    elif flag == 2:
        # 二、********************************从左上角.db数据库变为左下角.db数据库****************************************************************************************
        dbpath = r'D:\中导航\Convertmbtiles\ArcGisStreetWorld10.db'  # 输入文件的绝对路径如D:\中导航\Convertmbtiles\ArcGisStreetWorld10.db
        orderTableName = 'tiles'  # tiles TilePic#目标表名（用来形成tiles表）

        print('开始第2个功能：从左上角.db数据库变为左下角.db数据库')
        # 若字段为默认的则可以不用传入,否则要对应各参数,若目标表为tiles表则会改名为tiles_origin避免与新建tiles表重复,否则目标表名不会改
        leftupToleftdown(dbpath, orderTableName)  # 从左上角.db数据库变为左下角.db数据库
        # leftupToleftdown(dbpath,orderTableName,zoom_level='lev',tile_column='mapCol',tile_row='mapRow',tile_data='pic')#若字段为默认的则可以不用传入,否则需要对应各参数
        # leftupToleftdown()可选参数
        # 1、指明目标表字段含义:如zoom_level='lev',tile_column='mapCol',tile_row='mapRow',tile_data='pic',uptime='uptime'
        # 2、metadata表的无法直接确定的属性值，typee可取'baselayer'or'overlay'如：
        # bounds='-180.0,-90.0,180.0,90.0',description='description',version='10.6',typee='baselayer',
        # name='Layers',tile_width=256,tile_height=256,axis_origin='-180.0,-90.0',axis_positive_direction='RightUp'

    elif flag == 3:
        # 三、*******************************传入数据库及以左下角为原点的表,创建metadata和tiles，成为标准的.db文件*****************************************************************************************
        dbpath = r'D:\中导航\Convertmbtiles\GoogleMapNight1-7.db'  # 输入文件的绝对路径
        orderTableName = 'TilePic_copy1'  # 目标表名（用来形成tiles表）

        print('开始第3个功能：传入数据库及以左下角为原点的表,创建metadata和tiles，成为标准的.db文件')
        # 会将传入的orderTableName表名改为tiles
        CanonicalDB(dbpath, orderTableName, zoom_level='lev', tile_column='mapCol', \
                    tile_row='mapRow', tile_data='pic')  # 若字段为默认的则可以不用传入,否则需要传入各参数进行对应，并修改字段名
        # 应对标准的 tiles 格式生成metadata表
        # CanonicalDB(dbpath,orderTableName)
        # CanonicalDB()可选参数
        # 1、指明目标表字段含义:如zoom_level='lev',tile_column='mapCol',tile_row='mapRow',tile_data='pic',uptime='uptime'
        # 2、metadata表的无法直接确定的属性值，如：
        # bounds='-180.0,-90.0,180.0,90.0',description='description',version='10.6',typee='baselayer',
        # name='Layers',tile_width=256,tile_height=256,axis_origin='-180.0,-90.0',axis_positive_direction='RightUp'

    else:
        print('error,序号输入错误')

    endtime = datetime.datetime.now()
    print(endtime - starttime)

    if flag in [1, 2, 3]:
        # 四、*******************************修改文件.db的后缀名，变.mbtiles文件*****************************************************************************************
        # 从dbpath中切出文件夹地址，和文件名用于修改后缀
        file_name = list(dbpath.split('\\'))[-1]  # file_name='GoogleMapNight1-7.db'#切出文件名
        file_dir = dbpath.replace('\\' + file_name, '')  # 切出文件夹的地址
        # 改后缀前需要关掉该文件，否则改后缀失败！
        fname = file_name[0:file_name.index('.')]
        old_ext = file_name[file_name.index('.'):len(file_name)]  # 从file_name中切出文件名和后缀,如file_name为jn.db,则切出jn,和.db
        time.sleep(10)  # 加入其中sleep()延迟函数，防止数据库未关闭，就运行修改后缀名函数造成报错
        batch_rename(file_dir, fname, old_ext, ".mbtiles")  # 根据需要修改路径,目标文件名及后缀名
        print('转化为.mbtile文件结束')
        pass
    else:
        pass