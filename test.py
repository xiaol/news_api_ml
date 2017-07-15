# -*- coding: utf-8 -*-
# @bref :
from util.doc_process import get_postgredb_query

conn, cursor = get_postgredb_query()
sql = "select ni.title, ni.content, c.cname from info_news ni " \
          "inner join channellist_v2 c on ni.chid=c.id " \
          "inner join newslist_v2 nv on ni.nid=nv.nid " \
          "where ni.nid=%s and nv.state=0"
cursor.execute(sql, [22922732])
r = cursor.fetchone()
print r[0]
conn.close()