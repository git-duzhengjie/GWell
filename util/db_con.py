#!/usr/bin/env python
# -*- coding: utf-8 -*-


from __future__ import division
import MySQLdb
import os
import datetime
import smtplib
import sys

reload(sys)


class Conn:
    def __init__(self, host, port, database, user, passwd):
        self.ip = host
        self.port = port
        self.user = user
        self.password = passwd
        self.database = database
        try:
            self.conn = MySQLdb.connect(host=self.ip, user=self.user, passwd=self.password, db=self.database,
                                        port=self.port)
            self.cur = self.conn.cursor()
        except MySQLdb.Error as e:
            print("Mysql Error %d: %s" % (e.args[0], e.args[1]))

    @property
    def reconnect(self):
        if not self.open:
            try:
                self.conn = MySQLdb.connect(host=self.ip, user=self.user, passwd=self.password, db=self.database,
                                            port=self.port)
                self.cur = self.conn.cursor()
                return True
            except MySQLdb.Error as e:
                print("Mysql Error %d: %s" % (e.args[0], e.args[1]))
                return False
        try:
            self.conn.ping()
        except MySQLdb.Error as e:
            try:
                self.conn = MySQLdb.connect(host=self.ip, user=self.user, passwd=self.password, db=self.database,
                                            port=self.port)
                self.cur = self.conn.cursor()
                return True
            except MySQLdb.Error as e:
                print("Mysql Error %d: %s" % (e.args[0], e.args[1]))
                return False

    @property
    def open(self):
        try:
            return self.conn.open
        except MySQLdb.Error:
            print "Connection %s:%s@%s failed" % (self.ip, self.port, self.database)
            return None

    def querynum(self, sql):
        count = self.cur.execute(sql)
        result = self.cur.fetchone()
        reval = result[0]
        if reval is None:
            return 0
        return reval

    def excute(self, sql, ps=None):
        return self.cur.execute(sql, ps)

    def fetchall(self):
        return self.cur.fetchall()

    def fetchone(self):
        return self.cur.fetchone()

    def query(self, sql):
        count = self.cur.execute(sql)
        reval = []
        if count == 0:
            return reval
        for i in range(count):
            result = self.cur.fetchone()
            reval.append(result[0])
        return reval

    def queryone(self, sql):
        count = self.cur.execute(sql)
        if count == 0:
            return ""
        result = self.cur.fetchone()
        return result

    def queryall(self, sql):
        count = self.cur.execute(sql)
        if count == 0:
            return ""
        result = self.cur.fetchall()
        return result

    def queryset(self, sql):
        ret_set = {}
        count = self.cur.execute(sql)
        for i in range(count):
            rs = self.cur.fetchone()
            ret_set[rs[0]] = 1
        return ret_set

    def querystring(self, sql):
        count = self.cur.execute(sql)
        if count == 0:
            return "\'0\'"
        reval = []
        for i in range(count):
            result = self.cur.fetchone()
            tmp = "\'%s\'" % (result[0])
            reval.append(tmp)
        return ','.join(reval)

    def querynums(self, sql):
        count = self.cur.execute(sql)
        if count == 0:
            return "0"
        reval = []
        for i in range(count):
            result = self.cur.fetchone()
            tmp = "%s" % (result[0])
            reval.append(tmp)
        return ','.join(reval)

    def querynumrows(self, sql):
        return self.cur.execute(sql)

    def querylist(self, sql):
        count = self.cur.execute(sql)
        if not count:
            return []
        reval = []
        for i in range(count):
            result = self.cur.fetchone()
            reval.append(result)
        return reval

    def querymap(self, sql):
        count = self.cur.execute(sql)
        if not count:
            return {}
        reval = {}
        for i in range(count):
            result = self.cur.fetchone()
            if isinstance(result[1], datetime.date):
                tmpk = result[1].strftime("%Y-%m-%d")
            else:
                tmpk = result[1]
            reval.setdefault(tmpk, result[0])
        return reval

    def querytuplemap(self, sql, num):
        count = self.cur.execute(sql)
        if not count:
            return {}
        reval = {}
        for i in range(count):
            result = self.cur.fetchone()
            tmpk = []
            for j in range(num):
                tmpk.append(result[1 + j])
            tmpk = tuple(tmpk)
            reval.setdefault(tmpk, result[0])
        return reval

    def querytuplemaptuple(self, sql, numkey, numvalue):
        count = self.cur.execute(sql)
        if not count:
            return {}
        reval = {}
        for i in range(count):
            result = self.cur.fetchone()
            tmpk = []
            for j in range(numkey):
                tmpk.append(result[numvalue + j])
            tmpk = tuple(tmpk)
            tmpv = []
            for j in range(numvalue):
                tmpv.append(result[0 + j])
            reval.setdefault(tmpk, tmpv)
        return reval

    def querymaplist(self, sql):
        count = self.cur.execute(sql)
        if not count:
            return {}
        reval = {}
        for i in range(count):
            result = self.cur.fetchone()
            if isinstance(result[1], datetime.date):
                tmpk = result[1].strftime("%Y-%m-%d")
            else:
                tmpk = result[1]
            reval.setdefault(tmpk, []).append(result[0])
        return reval

    def querytuplemaplist(self, sql, num):
        count = self.cur.execute(sql)
        if not count:
            return {}
        reval = {}
        for i in range(count):
            result = self.cur.fetchone()
            tmpk = []
            for j in range(num):
                tmpk.append(result[1 + j])
            tmpk = tuple(tmpk)
            reval.setdefault(tmpk, []).append(result[0])
        return reval

    def querylistmap(self, sql):
        count = self.cur.execute(sql)
        if not count:
            return {}
        reval = {}
        for i in range(count):
            result = self.cur.fetchone()
            if isinstance(result[-1], datetime.date):
                tmpk = result[-1].strftime("%Y-%m-%d")
            else:
                tmpk = result[-1]
            reval.setdefault(tmpk, result[:-1])
        return reval

    def querylistmaplist(self, sql):
        count = self.cur.execute(sql)
        if not count:
            return {}
        reval = {}
        for i in range(count):
            result = self.cur.fetchone()
            if isinstance(result[-1], datetime.date):
                tmpk = result[-1].strftime("%Y-%m-%d")
            else:
                tmpk = result[-1]
            reval.setdefault(tmpk, []).append(result[:-1])
        return reval

    def showcreatetable(self, tb):
        ret = ''
        count = self.cur.execute('show create table %s' % tb)
        if count == 0:
            return ret
        rs = self.cur.fetchone()
        return rs[1]

    def clonetable(self, src, tb):
        try:
            tb_desc = src.showcreatetable(tb)
            if tb_desc == '':
                return -1
            self.excute(tb_desc)
            self.commit()
            return 0
        except MySQLdb.Error, e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])

    def formatinsert(self, table, fds_num):
        string = 'insert into ' + table + ' values('
        for i in range(0, fds_num - 1):
            string = string + '%s,'
        string = string + '%s)'
        return string

    def deleteall(self, tb):
        self.cur.execute("delete from %s" % tb)
        self.conn.commit()

    def batchexcute(self, ary, batch_num, insert_str):
        try:
            ary_len = len(ary)
            tmp_ary = []
            for i in range(0, ary_len):
                tmp_ary.append(ary[i])
                if (i + 1) % batch_num == 0:
                    self.cur.executemany(insert_str, tmp_ary)
                    tmp_ary = []
                elif i == ary_len - 1:
                    self.cur.executemany(insert_str, tmp_ary)
                    tmp_ary = []
                self.conn.commit()
            return True
        except MySQLdb.Error, e:
            print "Mysql Error %s: %s" % (e.args[0], e.args[1])
            return False

    def batchexcute_s(self, ary, insert_str):
        try:
            ary_len = len(ary)
            print ary_len
            for i in range(0, ary_len):
                self.cur.execute(insert_str % ary[i])
            return True
        except MySQLdb.Error, e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
            return False

    def close(self):
        self.cur.close()
        self.conn.close()

    def commit(self):
        return self.conn.commit()
