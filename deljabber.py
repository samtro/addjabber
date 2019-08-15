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
def createParser ():
  parser=argparse.ArgumentParser(
      prog='deljabberd.py',
      description = '''Удаления пользователя с jabber сервера''',
      epilog='''Автор: Костенков В.Н., (ↄ) МУП Ижводоканал''')
  parser.add_argument('-j','--jid',required=True , help = 'jabber id пользователя (email)', metavar='[jid]')
  parser.add_argument('-n','--hosts',nargs='*', help = 'Имена серверов. Если параметр пропущен, то будет использовано значение по умолчанию -\
                                заданное в переменной listHosts (т.е. все имеющие сервера)',metavar='[Имя хоста]') 
  return parser

"""main"""
parser=createParser()
listarg=parser.parse_args(sys.argv[1:])
jid=listarg.jid
uRealm=jid.split("@")[1]
#uRealm='sa-kvn'
if (listarg.hosts):
    listHosts=listarg.hosts

print "\nuser jid=%s"%(jid)
print "uReal=%s"%(uRealm)
print "list hosts=%s\n"%(listHosts)
''' цикл по серверам предприятия'''
for host in listHosts:
    print "Обработка сервера: %s"%(host)
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
        print "Ошибка (mysql:%d) при подключения к серверу:%s. Сервер будет пропущен. Проверьте соединение"%(e.args[0],host)
        continue
    if(uRealm==host):
        '''удаление данных авторизации таблицы authreg,active,vcard'''
        print "\tУдаление данных авторизации"
        cur_sql.execute("delete from `authreg` where `username`='%s'"%(jid.split("@")[0])) 
        print "\t\tУдалено %s записей из таблицы 'authreg'"%(cur_sql.rowcount)
        cur_sql.execute("delete from `active` where `collection-owner`='%s'"%(jid)) 
        print "\t\tУдалено %s записей из таблицы 'active'"%(cur_sql.rowcount)
        cur_sql.execute("delete from `vcard` where `collection-owner`='%s'"%(jid)) 
        print "\t\tУдалено %s записей из таблицы 'vcard'"%(cur_sql.rowcount)
        ''' удаление из ростеров других пользователей таблицы roster-items и roster-groups'''
        print "\tУдаление из ростеров пользователей"
        cur_sql.execute("delete from `roster-items` where`jid`='%s'"%(jid))
        print "\t\tУдалено %s записей из таблицы 'roster-items'"%(cur_sql.rowcount)
        cur_sql.execute("delete from `roster-groups` where`jid`='%s'"%(jid))
        print "\t\tУдалено %s записей из таблицы 'roster-groups'"%(cur_sql.rowcount)
        '''удаление собственных ростеров пользователя таблицы roster-items и roster-groups'''
        print "\tУдаление собственных ростеров"
        cur_sql.execute("delete from `roster-items` where`collection-owner`='%s'"%(jid))
        print "\t\tУдалено %s записей из таблицы 'roster-items'"%(cur_sql.rowcount)
        cur_sql.execute("delete from `roster-groups` where`collection-owner`='%s'"%(jid))
        print "\t\tУдалено %s записей из таблицы 'roster-groups'"%(cur_sql.rowcount)
        con_sql.commit()
    else:
        '''удаление собственных ростеров пользователя таблицы roster-items и roster-groups'''
        print "\tУдаление из ростеров пользователей"
        cur_sql.execute("delete from `roster-items` where`jid`='%s'"%(jid))
        print "\t\tУдалено %s записей из таблицы 'roster-items'"%(cur_sql.rowcount)
        cur_sql.execute("delete from `roster-groups` where`jid`='%s'"%(jid))
        print "\t\tУдалено %s записей из таблицы 'roster-groups'"%(cur_sql.rowcount)
        con_sql.commit()
    cur_sql.close()
    con_sql.close()
    print"cделано\n"

