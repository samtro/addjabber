#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import argparse
import MySQLdb
from bsddb3 import db
inFile='workerlist.csv'
dbFile='userdb.dbm'
listHosts=['g17.ivk','inf.ivk','asu.ivk','msa.ivk','neo.ivk','ngs.ivk','upr.ivk','los.ivk','cvz.ivk']
''' функция addUserRoster - добавляет ростеры сущетсвующих на сервере пользователей для вновь созданого'''
def addUserRoster(aUser,cur_sql):
    '''Данные для ростеров берутся со всех серверов предприятия'''
    '''sqlUser заносится значение поля 
       fn - Полное имя
       cllection-owner - совпадает с адресом электронной почты
       из таблицы vcard'''
    hosts=['g17.ivk','inf.ivk','asu.ivk','msa.ivk','neo.ivk','ngs.ivk','upr.ivk','los.ivk','cvz.ivk']
    #hosts=['sa-kvn']
    for h in hosts:
        try:
            cn_sql=MySQLdb.connect(h,'jabberd2','111','jabberd2')
            cn_sql.set_character_set('utf8')
            cr_sql=cn_sql.cursor()
            cr_sql.execute('SET NAMES `utf8`')
            cr_sql.execute('SET CHARACTER SET utf8;')
            cr_sql.execute('SET character_set_connection=utf8;')
            cr_sql.execute('SET COLLATION_CONNECTION="utf8_general_ci"')
            cn_sql.commit()
        except MySQLdb.Error, e:
            #print "Error %d: %s" % (e.args[0], e.args[1])
            #print ("Из-за ошибки сооединени сервер - "+h+" будет пропущен")
            print "Ошибка (mysql:%d) при подключения к серверу:%s, функция addUserRoster(). Сервер будет пропущен. Проверьте соединение"%(e.args[0],h)
            continue
        cr_sql.execute("select `fn`,`collection-owner` from `vcard`")
        if (cr_sql.rowcount >0):
            result=cr_sql.fetchall()
        else:
            continue
        for sqlUser in result:
            ''' исключаем самого себя'''
            if(sqlUser[1]==aUser[4]):
                continue
            fio=sqlUser[0].split(' ')[0]+' '+sqlUser[0].split(' ')[1][:2]+'.'+sqlUser[0].split(' ')[2][:2]+'.'
            '''cr_sql.execute("select `group` from `roster-groups` where `jid`='"+sqlUser[1]+"' limit 1")'''
            cr_sql.execute("select `org-orgunit` from vcard where `collection-owner`='"+sqlUser[1]+"' limit 1");
            ''' откуда группа для единственного пользователя???'''
            group=cr_sql.fetchone()[0]
            rosterItems=(aUser[4],sqlUser[1],fio,1,1,1)
            rosterGroups=(aUser[4],sqlUser[1],group)
            cur_sql.execute("insert into `roster-items` (`collection-owner`,`jid`,`name`,`to`,`from`,`ask`) values (%s,%s,%s,%s,%s,%s)",rosterItems)
            cur_sql.execute("insert into `roster-groups`(`collection-owner`,`jid`,`group`) values (%s,%s,%s)",rosterGroups)           
        cr_sql.close()
        cn_sql.close()
''' функция addRoster - добавляет в ростеры пользователей вновь созданого'''
def addRoster(aUser,cur_sql):
    '''осуществляется выборка пользователей соответствующего домена'''
    '''в sqlUser заносится значение поля collection-owner (совпадает с эл. почтой) из таблицы vcard'''
    cur_sql.execute("select `collection-owner` from `vcard`")
    if (cur_sql.rowcount >0):
        result=cur_sql.fetchall()
    else:
        return 
    for sqlUser in result:
        if (sqlUser[0]==aUser[4]):
            continue
        rosterItems=(sqlUser[0],aUser[4],aUser[0],1,1,1)
        rosterGroups=(sqlUser[0],aUser[4],aUser[5])
        cur_sql.execute("insert into `roster-items` (`collection-owner`,`jid`,`name`,`to`,`from`,`ask`) values (%s,%s,%s,%s,%s,%s)", rosterItems)
        cur_sql.execute("insert into `roster-groups`(`collection-owner`,`jid`,`group`) values (%s,%s,%s)",rosterGroups)
def getUser (dbFile,inFile,INN):
    '''в полях переменной aUser записаны следующие значения:'''
    '''[0] - Фамилия с инициалами - Петров П.П.'''
    '''[1] - Фамилия, имя отчество полностью - Петров Петр Петрович'''
    '''[2] - Табельный номер - 0001'''
    '''[3] - Логин - ppp'''
    '''[4] - email - ppp@g17.ivk'''
    '''[5] - Подразделение - Ремонтно-строительный участок'''
    '''[6] - Должность  - слесарь'''
    fWork=open(inFile,'r')
    aUser=[]
    lWork=1
    lbKey=0
    while lWork:
        lWork=fWork.readline()
        if(lWork.find(INN)!= -1):
            lWork=lWork.split(',')
            aUser.insert(0,unicode(lWork[0],'utf8',errors='replace'))
            aUser.insert(1,unicode(lWork[2],'utf8',errors='replace'))
            aUser.insert(2,unicode(lWork[7],'utf8',errors='replace'))
            aUser.insert(5,unicode(lWork[4],'utf8',errors='replace'))
            aUser.insert(6,unicode(lWork[5],'utf8',errors='replace'))
            lbKey=1
    if (lbKey==0):
        print("Function getUser(): Пользователя с ИНН равным "+INN+" не существует в файле workerlist.csv. Проверьте данные. Выход")
        sys.exit(1)
    userdb=db.DB()
    userdb.open(dbFile)
    userdbCursor=userdb.cursor()
    userdbRec=userdbCursor.first()
    ubKey=0
    while userdbRec:
        if (userdbRec[0]==INN):
            aUser.insert(3,userdbRec[1].split(':::')[1])
            aUser.insert(4,userdbRec[1].split(':::')[1]+'@'+userdbRec[1].split(':::')[2])
            ubKey=1
        userdbRec=userdbCursor.next()
    if(ubKey==0):
        print("Function getUser(): Пользователя с ИНН равным "+INN+" не существует в файле userdb.dbm. Проверьте данные. Выход")
        sys.exit(1)
    userdb.close()
    return(aUser)
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
def createParser ():
  parser=argparse.ArgumentParser(
      prog='addjabber.py',
      description = '''Добавление пользователя на jabber сервер. Перед запуском скрипта пользователь должен быть обязательно добавлен в userdb.dbm''',
      epilog='''Автор: Костенков В.Н., (ↄ) МУП Ижводоканал''')
  parser.add_argument('-i','--INN',required=True , help = 'ИНН пользователя', metavar='[ИНН]')
  parser.add_argument('-n','--hosts',nargs='*', help = 'Имена серверов. Если параметр пропущен, то будет использовано значение по умолчанию -\
                                заданное в переменной listHosts (т.е. все имеющие сервера)',metavar='[Имя хоста]') 
  parser.add_argument('-s','--search',nargs=2,help= 'Поиск',metavar='[поиск пользователя]')                      
  return parser 
def searchSQL(aUser):
    sKey=0
    try:
        shost=aUser[4].split('@')[1]
        #shost='sa-kvn'
        sn_sql=MySQLdb.connect(shost,'jabberd2','111','jabberd2')
        sn_sql.set_character_set('utf8')
        sc_sql=sn_sql.cursor()
        sc_sql.execute('SET NAMES `utf8`')
        sc_sql.execute('SET CHARACTER SET utf8;')
        sc_sql.execute('SET character_set_connection=utf8;')
        sc_sql.execute('SET COLLATION_CONNECTION="utf8_general_ci"')
        sn_sql.commit()
    except MySQLdb.Error, e:
        #print "Error %d: %s" % (e.args[0], e.args[1])
        print "Ошибка (mysql:%d) при подключения к серверу:%s, в функции searchSQL. Программа завершила работу. Проверьте соединение"%(e.args[0],shost)
        sys.exit(1)
    sc_sql.execute("select `username` from authreg where `username`='"+aUser[3]+"'")
    if (sc_sql.rowcount >0):
        sKey=1
    else:
        sKey=0
    sc_sql.close()
    sn_sql.close()
    return (sKey)

"""main"""
parser=createParser()
listarg=parser.parse_args(sys.argv[1:])
INN=listarg.INN
aUser=[]
uRealm=''
if (listarg.hosts):
    listHosts=listarg.hosts
if (listarg.search):
    print"search=%s"%(listarg.search)
    sys.exit(0)
INN=listarg.INN
if (searchUser(INN,inFile)==0):
    print ("Function main: Пользователя с ИНН равным "+INN+" не существует в файле workerlist.csv. Проверьте данные. Выход")
    sys.exit(1)
aUser=getUser(dbFile,inFile,INN)
if (searchSQL(aUser)==1):
    print ("Пользователь "+aUser[4]+" с ИНН "+INN+" уже есть на jabber сервер "+aUser[4].split('@')[1]) 
    print ("Если хотите его добавить, то сначала удалите коммандой deljabber.py. Выход")
    sys.exit(0)
uRealm=aUser[4].split('@')[1]
#uRealm='sa-kvn'
for host in listHosts:
    print("\nОбработка сервера - "+host+" ....")
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
        print "Ошибка (mysql:%d) при подключения к серверу:%s.Сервер будет пропущен. Проверьте соединение"%(e.args[0],host)
        continue
    if(uRealm==host):
        '''Добавляем в таблицы authreg,active,vcard - это регистрация пользователя'''
        '''authreg'''
        authReg=(aUser[3],uRealm,aUser[2])
        cur_sql.execute('insert into authreg values (%s,%s,%s)',authReg)
        ''' active'''
        active=(aUser[4],'120199999')
        cur_sql.execute("insert into active (`collection-owner`,`time`) values (%s,%s)",active)
        '''vcard'''
        vCard=(aUser[4],aUser[1],aUser[4],aUser[6],aUser[6],'МУП Ижводоканал',aUser[5])
        cur_sql.execute("insert into vcard (`collection-owner`,`fn`,`email`,`title`,`role`,`org-orgname`,`org-orgunit`) values(%s,%s,%s,%s,%s,%s,%s)",vCard)
        '''con_sql.commit()'''
        '''Добавляем в ростеры других пользователей'''
        addRoster(aUser,cur_sql)
        '''А теперь других пользователей добавляем в ростер для вновь созданного'''
        addUserRoster(aUser,cur_sql)
        con_sql.commit()
    else:
        '''Добавляем в ростеры других пользователей'''
        addRoster(aUser,cur_sql)
        con_sql.commit()     
    cur_sql.close()
    con_sql.close()
    print('Сделано\n')
'''end for'''

    
