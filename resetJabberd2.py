#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import argparse
import MySQLdb
from bsddb3 import db
inFile='workerlist.csv'
dbFile='userdb.dbm'
pathPid="/home/kvn/tmp/jabberd2/"
def searchUser(INN,inFile):
    fWork=open(inFile,'r')
    lWork=1
    bKey=0
    while lWork:
        lWork=fWork.readline()
        if (lWork.find(INN)!= -1):
            bKey=1
    fWork.close()
    return(bKey)
def clearDataBases(cur_sql):
    cur_sql.execute("show tables")
    result=cur_sql.fetchall()
    for i in result:
        cur_sql.execute("TRUNCATE"+" `"+i[0]+"`")
    con_sql.commit()  
    print ("Очистка таблиц ... сделано")
def getUser (dbFile,inFile,INN):
    '''в полях переменной aUser записаны следующие значения:'''
    '''[0] - Фамилия с инициалами - Петров П.П.'''
    '''[1] - Фамилия, имя отчество полностью - Петров Петр Петрович'''
    '''[2] - Табельный номер'''
    '''[3] - Логин - ppp'''
    '''[4] - email - ppp@g17.ivk'''
    '''[5] - Подразделение - Ремонтно-строительный участок'''
    '''[6] - Должность  - слесарь'''
    fWork=open(inFile,'r')
    aUser=[]
    lWork=1
    while lWork:
        lWork=fWork.readline()
        if(lWork.find(INN)!= -1):
            lWork=lWork.split(',')
            aUser.insert(0,unicode(lWork[0],'utf8',errors='replace'))
            aUser.insert(1,unicode(lWork[2],'utf8',errors='replace'))
            aUser.insert(2,unicode(lWork[7],'utf8',errors='replace'))
            aUser.insert(5,unicode(lWork[4],'utf8',errors='replace'))
            aUser.insert(6,unicode(lWork[5],'utf8',errors='replace'))
    userdb=db.DB()
    userdb.open(dbFile)
    userdbCursor=userdb.cursor()
    userdbRec=userdbCursor.first()
    while userdbRec:
        if (userdbRec[0]==INN):
            aUser.insert(3,userdbRec[1].split(':::')[1])
            aUser.insert(4,userdbRec[1].split(':::')[1]+'@'+userdbRec[1].split(':::')[2])
        userdbRec=userdbCursor.next()
    userdb.close()
    return(aUser)        
def addRoster(dbFile,inFile,aUser,cur_sql):
    '''Обходим все записи в userdb и добавляем ростеры и группы для пользователя aUser'''
    rUser=[]
    userdb=db.DB()
    userdb.open(dbFile)
    userdbCursor=userdb.cursor()
    userdbRec=userdbCursor.first()
    while userdbRec:
        if (searchUser(userdbRec[0],inFile)==0):
            print(userdbRec[0]+" - Данного пользователя нет в файле workerlist.csv. Он скорее всего уволился")
            userdbRec=userdbCursor.next()
            continue
        rUser=getUser(dbFile,inFile,userdbRec[0])
        '''самого себя в ростеры не добавляем'''
        if (aUser[4]!=rUser[4]): 
            rosterItems=(aUser[4],rUser[4],rUser[0],1,1,1)
            rosterGroups=(aUser[4],rUser[4],rUser[5])
            cur_sql.execute("insert into `roster-items` (`collection-owner`,`jid`,`name`,`to`,`from`,`ask`) values (%s,%s,%s,%s,%s,%s)",rosterItems)
            cur_sql.execute("insert into `roster-groups`(`collection-owner`,`jid`,`group`) values (%s,%s,%s)",rosterGroups)
        userdbRec=userdbCursor.next()
    userdb.close()
def createParser ():
  parser=argparse.ArgumentParser(
      prog='resetJabberd2.py',
      description = '''Скрипт для первичного заполнения базы jabberd2 (MySQL), также его можно использовать для \
                    сброса всех пользователей. \n Внимание при работе скрипта все таблицы из базы данных очищаются!!!''',
      epilog='''Автор: Костенков В.Н., (ↄ) МУП Ижводоканал''')
  parser.add_argument('-r','--realm',required=True , help = 'Домен где запускается скрипт ', metavar='[Домен]')
  parser.add_argument('-n','--host', help = 'Имя хоста для подключения к MySQL. Если параметр не задан,\
                      то считается, что имя совпадает с параметром realm',metavar='[Имя хоста]') 
  return parser 

'''main program'''
'''Разбираем параметры realm и host'''
parser=createParser()
listarg=parser.parse_args(sys.argv[1:])
realm=listarg.realm
if listarg.host:
    host=listarg.host
else:
    host=realm
#print("host="+host,"real="+realm)
#files=[]
#files=os.listdir(pathPid)
#print (files)
#if files!=[]:
    #print ("Сервер jabberd2 работает. На время работы скрипта его нужно отключить командой - service jabberd2 stop")
    #sys.exit(0)
''' открываем базу jabberd2 для заполнения'''
try:
  con_sql=MySQLdb.connect(host,'jabberd2','111','jabberd2')
  con_sql.set_character_set('utf8')
  cur_sql=con_sql.cursor()
  cur_sql.execute('SET NAMES `utf8`')
  cur_sql.execute('SET CHARACTER SET utf8;')
  cur_sql.execute('SET character_set_connection=utf8;')
  cur_sql.execute('SET COLLATION_CONNECTION="utf8_general_ci"')
  con_sql.commit()
except MySQLdb.Error, e:
  #print "Error %d: %s" % (e.args[0], e.args[1])
  print "Ошибка (mysql:%d) при подключения к серверу:%s. Выход"%(e.args[0],host)
  sys.exit(1)
'''очищаем таблицы базы данных'''
clearDataBases(cur_sql)
'''отрываем файл userdb и обходим все записи пренадлежащие домену - параметр realm'''
'''структура записи в userdb: ('592007613466', '0LvQsNCw:::laa:::cvz.ivk:::566:::XXXXXXXX')'''
'''                             ИНН             ????      логин   домен  табельный маска'''
'''формируем таблицы authreg,active,vcard'''

userdb=db.DB()
userdb.open(dbFile)
userdbCursor=userdb.cursor()
userdbRec=userdbCursor.first()
aUser=[]
INN=''
while userdbRec:
    uRealm=userdbRec[1].split(':::')[2]
    INN=userdbRec[0]
    uName=userdbRec[1].split(':::')[1]
    if (realm==uRealm):
        if (searchUser(INN,inFile)==1):
            '''работаем'''
            aUser=getUser(dbFile,inFile,INN)
            '''authreg'''
            authReg=(aUser[3],realm,aUser[2])
            cur_sql.execute('insert into authreg values (%s,%s,%s)',authReg)
            ''' active'''
            active=(aUser[4],'120199999')
            cur_sql.execute("insert into active (`collection-owner`,`time`) values (%s,%s)",active)
            '''vcard'''
            vCard=(aUser[4],aUser[1],aUser[4],aUser[6],aUser[6],'МУП Ижводоканал',aUser[5])
            cur_sql.execute("insert into vcard (`collection-owner`,`fn`,`email`,`title`,`role`,`org-orgname`,`org-orgunit`) values(%s,%s,%s,%s,%s,%s,%s)",vCard)
            ''' добавляем ростеры'''
            addRoster(dbFile,inFile,aUser,cur_sql)
            con_sql.commit()
        else:
            print("Пользователь: "+userdbRec[1].split(':::')[1]+" c ИНН "+userdbRec[0]+" отсутствует в файле worklist.csv. Возможно он уволился и его аккаунт нужно удалить")
    con_sql.commit()
    userdbRec=userdbCursor.next()
userdb.close()
cur_sql.close()
con_sql.close()
print("Работа скрипта завершена без ошибок")


    

    
