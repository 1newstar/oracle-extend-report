#!/usr/bin/python
# -*- coding: utf-8 -*-  
import sys
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
import pprint
import getopt
import time
import cx_Oracle
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from pyh import *

SQL_ORDERED_BY_ELAPSED_TIME="""
select *
  from (select sqt.sql_id,
               nvl(round((sqt.elap / 1000000),2), to_number(null)),
               nvl(round((sqt.cput / 1000000),2), to_number(null)),
               sqt.exec,
               decode(sqt.exec,0,to_number(null),round((sqt.elap / sqt.exec / 1000000),2)),               
               decode(sqt.module,
                              null,
                              null,
                              sqt.module),
               nvl(dbms_lob.substr(st.sql_text,40), ' ** SQL Text Not Available ** ')
          from (select sql_id,
                       max(module) module,
                       sum(elapsed_time_delta) elap,
                       sum(cpu_time_delta) cput,
                       sum(executions_delta) exec
                  from dba_hist_sqlstat
                 where dbid = &dbid
                   and instance_number = &inst_num
                   and parsing_schema_name = &schema_name
                   and &beg_snap < snap_id
                   and snap_id <= &end_snap
                 group by sql_id) sqt,
               dba_hist_sqltext st
         where st.sql_id(+) = sqt.sql_id
           and st.dbid(+) = &dbid
         order by nvl(sqt.elap, -1) desc, sqt.sql_id)
 where rownum <= &sql_num
"""
SQL_ORDERED_BY_CPU_TIME="""
     select *
       from (select sqt.sql_id,
                    nvl(round((sqt.elap / 1000000),2), to_number(null)),
                    nvl(round((sqt.cput / 1000000),2), to_number(null)),
                    sqt.exec,
                    decode(sqt.exec,0,to_number(null),round((sqt.elap / sqt.exec / 1000000),2)),
                    decode(sqt.module,null,null,sqt.module),
                    nvl(dbms_lob.substr(st.sql_text,40), ' ** SQL Text Not Available ** ')
               from (select sql_id,
                            max(module) module,
                            sum(cpu_time_delta) cput,
                            sum(elapsed_time_delta) elap,
                            sum(executions_delta) exec
                       from dba_hist_sqlstat
                      where dbid = &dbid
                        and instance_number = &inst_num
                        and parsing_schema_name = &schema_name
                        and &beg_snap < snap_id
                        and snap_id <= &end_snap
                      group by sql_id) sqt,
                    dba_hist_sqltext st
              where st.sql_id(+) = sqt.sql_id
                and st.dbid(+) = &dbid
              order by nvl(sqt.cput, -1) desc, sqt.sql_id)
      where rownum <= &sql_num
"""

SQL_ORDERED_BY_GETS="""
select *
  from (select sqt.sql_id,
               sqt.bget,
               sqt.exec,
               decode(sqt.exec, 0, to_number(null), (sqt.bget / sqt.exec)),
               round((100 * sqt.bget) /
               (SELECT sum(e.VALUE) - sum(b.value)
                  FROM DBA_HIST_SYSSTAT b, DBA_HIST_SYSSTAT e
                 WHERE B.SNAP_ID = &beg_snap
                   AND E.SNAP_ID = &end_snap
                   AND B.DBID = &dbid
                   AND E.DBID = &dbid
                   AND B.INSTANCE_NUMBER = &inst_num
                   AND E.INSTANCE_NUMBER = &inst_num
                   and e.STAT_NAME = 'session logical reads'
                   and b.stat_name = 'session logical reads'),2) norm_val,
               nvl(round((sqt.cput / 1000000),2), to_number(null)),
               nvl(round((sqt.elap / 1000000),2), to_number(null)),
               decode(sqt.module,null,null,sqt.module),
               nvl(dbms_lob.substr(st.sql_text,40), ' ** SQL Text Not Available ** ')
          from (select sql_id,
                       max(module) module,
                       sum(buffer_gets_delta) bget,
                       sum(executions_delta) exec,
                       sum(cpu_time_delta) cput,
                       sum(elapsed_time_delta) elap
                  from dba_hist_sqlstat
                 where dbid = &dbid
                   and instance_number = &inst_num
                   and parsing_schema_name = &schema_name
                   and &beg_snap < snap_id
                   and snap_id <= &end_snap
                 group by sql_id) sqt,
               dba_hist_sqltext st
         where st.sql_id(+) = sqt.sql_id
           and st.dbid(+) = &dbid
         order by nvl(sqt.bget, -1) desc, sqt.sql_id)
 where rownum <= &sql_num
   and (rownum <= 10 or norm_val > 1)
"""

SQL_ORDERED_BY_READS="""
 select *
     from (select sqt.sql_id,
                  sqt.dskr,
                  sqt.exec,
                  decode(sqt.exec, 0, to_number(null), (sqt.dskr / sqt.exec)),
                  round((100 * sqt.dskr) /
                  (SELECT sum(e.VALUE) - sum(b.value)
                     FROM DBA_HIST_SYSSTAT b, DBA_HIST_SYSSTAT e
                    WHERE B.SNAP_ID = &beg_snap
                      AND E.SNAP_ID = &end_snap
                      AND B.DBID = &dbid
                      AND E.DBID = &dbid
                      AND B.INSTANCE_NUMBER = &inst_num
                      AND E.INSTANCE_NUMBER = &inst_num
                      and e.STAT_NAME = 'physical reads'
                      and b.stat_name = 'physical reads'),2) norm_val,
                  nvl(round((sqt.cput / 1000000),2), to_number(null)),
                  nvl(round((sqt.elap / 1000000),2), to_number(null)),
                  decode(sqt.module, null, null, sqt.module),
                  nvl(dbms_lob.substr(st.sql_text,40), ' ** SQL Text Not Available ** ')
             from (select sql_id,
                          max(module) module,
                          sum(disk_reads_delta) dskr,
                          sum(executions_delta) exec,
                          sum(cpu_time_delta) cput,
                          sum(elapsed_time_delta) elap
                     from dba_hist_sqlstat
                    where dbid = &dbid
                      and instance_number = &inst_num
                      and parsing_schema_name = &schema_name
                      and &beg_snap < snap_id
                      and snap_id <= &end_snap
                    group by sql_id) sqt,
                  dba_hist_sqltext st
            where st.sql_id(+) = sqt.sql_id
              and st.dbid(+) = &dbid
              and (SELECT sum(e.VALUE) - sum(b.value)
                     FROM DBA_HIST_SYSSTAT b, DBA_HIST_SYSSTAT e
                    WHERE B.SNAP_ID = &beg_snap
                      AND E.SNAP_ID = &end_snap
                      AND B.DBID = &dbid
                      AND E.DBID = &dbid
                      AND B.INSTANCE_NUMBER = &inst_num
                      AND E.INSTANCE_NUMBER = &inst_num
                      and e.STAT_NAME = 'physical reads'
                      and b.stat_name = 'physical reads') > 0
            order by nvl(sqt.dskr, -1) desc, sqt.sql_id)
    where rownum <= &sql_num
        and ( rownum <= 10 or norm_val > 1)
"""

SQL_ORDERED_BY_EXECUTIONS="""
select *
  from (select sqt.sql_id,
               sqt.exec exec,
               sqt.rowp,
               round(sqt.rowp / sqt.exec,2),
               round(sqt.cput / sqt.exec / 1000000,2),
               round(sqt.elap / sqt.exec / 1000000,2),
               decode(sqt.module, null, null, sqt.module),
               nvl(dbms_lob.substr(st.sql_text,40), ' ** SQL Text Not Available ** ')
          from (select sql_id,
                       max(module) module,
                       sum(executions_delta) exec,
                       sum(rows_processed_delta) rowp,
                       sum(cpu_time_delta) cput,
                       sum(elapsed_time_delta) elap
                  from dba_hist_sqlstat
                 where dbid = &dbid
                   and instance_number = &inst_num
                   and parsing_schema_name = &schema_name
                   and &beg_snap < snap_id
                   and snap_id <= &end_snap
                 group by sql_id) sqt,
               dba_hist_sqltext st
         where st.sql_id(+) = sqt.sql_id
           and st.dbid(+) = &dbid
           and sqt.exec > 0
         order by nvl(sqt.exec, -1) desc, sqt.sql_id)
 where rownum <= &sql_num
   and (rownum <= 10 or
       (100 * exec) /
       (SELECT sum(e.VALUE) - sum(b.value)
           FROM DBA_HIST_SYSSTAT b, DBA_HIST_SYSSTAT e
          WHERE B.SNAP_ID = &beg_snap
            AND E.SNAP_ID = &end_snap
            AND B.DBID = &dbid
            AND E.DBID = &dbid
            AND B.INSTANCE_NUMBER = &inst_num
            AND E.INSTANCE_NUMBER = &inst_num
            and e.STAT_NAME = 'execute count'
            and b.stat_name = 'execute count') > 1)
"""
def query_ora_obj_size_by_num(p_dbinfo,p_schema_name,p_num):
    db = p_dbinfo
    conn=cx_Oracle.connect(db[3]+'/'+db[4]+'@'+db[0]+':'+db[1]+'/'+db[2])
    cursor = conn.cursor()
    cursor.execute("select * from (select (case s.segment_type when 'TABLE PARTITION' then partition_name else segment_name end ) object_name, s.segment_type, (case s.segment_type when 'TABLE' then s.segment_name when 'INDEX' then (select i.table_name from dba_indexes i where i.owner=s.owner and i.index_name=s.segment_name) when 'LOBSEGMENT' then (select l.table_name||':'||l.column_name from dba_lobs l where l.owner=s.owner and l.segment_name=s.segment_name) else s.segment_name end ) parent_name,round(s.bytes/1024/1024/1024,2) size_gb from dba_segments s where s.owner='"+p_schema_name+"' and s.segment_name not like 'BIN%' order by 4 desc) where rownum<="+p_num)
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return records

def query_ora_obj_by_rows(p_dbinfo,p_schema_name,p_num):
    db = p_dbinfo
    conn=cx_Oracle.connect(db[3]+'/'+db[4]+'@'+db[0]+':'+db[1]+'/'+db[2])
    cursor = conn.cursor()
    cursor.execute("select * from (select table_name,num_rows,to_char(last_analyzed,'yyyy-mm-dd hh24:mi:ss') from dba_tables where owner='"+p_schema_name+"' and num_rows is not null order by 2 desc) where rownum<="+p_num)
    records = cursor.fetchall() 
    cursor.close()
    conn.close()
    return records

def query_sql_text(p_dbinfo,p_sql_id):
    conn=cx_Oracle.connect(p_dbinfo[3]+'/'+p_dbinfo[4]+'@'+p_dbinfo[0]+':'+p_dbinfo[1]+'/'+p_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute("select dbms_lob.substr(sql_text,400) from dba_hist_sqltext where sql_id='"+p_sql_id+"'")
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return records[0][0]

def query_sql_exec(p_dbinfo,p_sql_id,p_begin_snap_id,p_end_snap_id):
    conn=cx_Oracle.connect(p_dbinfo[3]+'/'+p_dbinfo[4]+'@'+p_dbinfo[0]+':'+p_dbinfo[1]+'/'+p_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute("select a.instance_number,a.snap_id,a.plan_hash_value,b.begin_interval_time from dba_hist_sqlstat a, dba_hist_snapshot b where sql_id = '"+p_sql_id+"' and a.snap_id = b.snap_id and a.snap_id between "+p_begin_snap_id+" and "+p_end_snap_id +" order by instance_number, snap_id")
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return records

def query_sql_plan(p_dbinfo,p_sql_id,p_begin_snap_id,p_end_snap_id):
    conn=cx_Oracle.connect(p_dbinfo[3]+'/'+p_dbinfo[4]+'@'+p_dbinfo[0]+':'+p_dbinfo[1]+'/'+p_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute("select distinct a.plan_hash_value from dba_hist_sqlstat a, dba_hist_snapshot b where sql_id = '"+p_sql_id+"' and a.snap_id = b.snap_id and a.snap_id between "+p_begin_snap_id+" and "+p_end_snap_id)
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return records

def query_plan_data(p_dbinfo,p_sql_id,p_sql_plan_hash_value):
    conn=cx_Oracle.connect(p_dbinfo[3]+'/'+p_dbinfo[4]+'@'+p_dbinfo[0]+':'+p_dbinfo[1]+'/'+p_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute("select id,lpad(' ',2*depth)||operation,options,object_owner,object_name,depth,cost from dba_hist_sql_plan where sql_id = '"+p_sql_id+"' and plan_hash_value="+p_sql_plan_hash_value + " order by id")
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return records

def query_db_id(p_dbinfo):
    conn=cx_Oracle.connect(p_dbinfo[3]+'/'+p_dbinfo[4]+'@'+p_dbinfo[0]+':'+p_dbinfo[1]+'/'+p_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute("select dbid from v$database")
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return records[0][0]

def query_schema_exist(p_dbinfo,p_schema_name):
    conn=cx_Oracle.connect(p_dbinfo[3]+'/'+p_dbinfo[4]+'@'+p_dbinfo[0]+':'+p_dbinfo[1]+'/'+p_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute("select count(*) from dba_users where username='"+p_schema_name+"'")
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    if int(records[0][0])==1:
        return True
    else:
        return False

def query_inst_num(p_dbinfo):
    conn=cx_Oracle.connect(p_dbinfo[3]+'/'+p_dbinfo[4]+'@'+p_dbinfo[0]+':'+p_dbinfo[1]+'/'+p_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute("select instance_number from v$instance")
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return records[0][0]

def query_begin_snap_id(p_dbinfo,p_reportdate,p_hour):
    conn=cx_Oracle.connect(p_dbinfo[3]+'/'+p_dbinfo[4]+'@'+p_dbinfo[0]+':'+p_dbinfo[1]+'/'+p_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute("select max(snap_id) from wrm$_snapshot where begin_interval_time < to_date('"+p_reportdate+" "+p_hour+"','yyyy-mm-dd hh24') and instance_number=1")
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return records[0][0]

def query_end_snap_id(p_dbinfo,p_reportdate,p_hour):
    conn=cx_Oracle.connect(p_dbinfo[3]+'/'+p_dbinfo[4]+'@'+p_dbinfo[0]+':'+p_dbinfo[1]+'/'+p_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute("select min(snap_id) from wrm$_snapshot where end_interval_time >= to_date('"+p_reportdate+" "+p_hour+"','yyyy-mm-dd hh24') and instance_number=1")
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return records[0][0]

def query_snap_data(p_dbinfo,p_begin_snap_id,p_end_snap_id):
    conn=cx_Oracle.connect(p_dbinfo[3]+'/'+p_dbinfo[4]+'@'+p_dbinfo[0]+':'+p_dbinfo[1]+'/'+p_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute("select snap_id, to_char(begin_interval_time,'yyyy-mm-dd hh24:mi:ss'),to_char(end_interval_time,'yyyy-mm-dd hh24:mi:ss') from wrm$_snapshot where snap_id between "+p_begin_snap_id+" and "+p_end_snap_id+" order by 1")
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return records

def query_sql_data(p_dbinfo,p_sql):
    conn=cx_Oracle.connect(p_dbinfo[3]+'/'+p_dbinfo[4]+'@'+p_dbinfo[0]+':'+p_dbinfo[1]+'/'+p_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute(p_sql)
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return records


def print_html_header(p_dbinfo,p_schema_name):
    page = PyH(p_dbinfo[2]+'_'+p_schema_name+'_SQL Report')
    page << """<style type="text/css">
            body.awr {font:bold 10pt Arial,Helvetica,Geneva,sans-serif;color:black;}
            pre.awr  {font:10pt Courier;color:black; background:White;}
            h1.awr   {font:bold 20pt Arial,Helvetica,Geneva,sans-serif;color:#336699;border-bottom:1px solid #cccc99;margin-top:0pt; margin-bottom:0pt;padding:0px 0px 0px 0px;}
            h2.awr   {font:bold 18pt Arial,Helvetica,Geneva,sans-serif;color:#336699;margin-top:4pt; margin-bottom:0pt;}
            h3.awr   {font:bold 16pt Arial,Helvetica,Geneva,sans-serif;color:#336699;margin-top:4pt; margin-bottom:0pt;}
            h4.awr   {font:bold 14pt Arial,Helvetica,Geneva,sans-serif;color:#336699;margin-top:4pt; margin-bottom:0pt;}
            h5.awr   {font:bold 12pt Arial,Helvetica,Geneva,sans-serif;color:#336699;margin-top:4pt; margin-bottom:0pt;}
            h6.awr   {font:bold 10pt Arial,Helvetica,Geneva,sans-serif;color:#336699;margin-top:4pt; margin-bottom:0pt;}
            li.awr   {font: 10pt Arial,Helvetica,Geneva,sans-serif; color:black; background:White;}
            th.awrnobg  {font:bold 10pt Arial,Helvetica,Geneva,sans-serif; color:black; background:White;padding-left:4px; padding-right:4px;padding-bottom:2px}
            td.awrbg    {font:bold 10pt Arial,Helvetica,Geneva,sans-serif; color:White; background:#0066CC;padding-left:4px; padding-right:4px;padding-bottom:2px}
            td.awrnc    {font:10pt Arial,Helvetica,Geneva,sans-serif;color:black;background:White;vertical-align:top;}
            td.awrc     {font:10pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFFFCC; vertical-align:top;}
            a.awr       {font:bold 10pt Arial,Helvetica,sans-serif;color:#663300; vertical-align:top;margin-top:0pt; margin-bottom:0pt;}
            </style>"""
    page << """<SCRIPT>
            function isHidden(oDiv,oTab){
              var vDiv = document.getElementById(oDiv);
              var vTab = document.getElementById(oTab);
              vDiv.innerHTML=(vTab.style.display == 'none')?"<h5 class='awr'>-</h5>":"<h5 class='awr'>+</h5>";
              vTab.style.display = (vTab.style.display == 'none')?'table':'none';
            }
            </SCRIPT>"""
    page << """<head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
            </head>"""

    page << h3("This mail is generated automatically, please do not reply directly! If you have any questions, please contact user@mail.abc.cn!")
    page << h1(p_schema_name+' Schema Report', cl='awr')
    return page

def print_db_header(p_page,p_dbinfo,p_schema_name,p_reportdate,p_begin_hour,p_end_hour,p_begin_snap_id,p_end_snap_id):
    p_page << br()
    l_header = ['DATABASE IP','INSTANCE_NAME','SCHEMA_NAME','REPORT_DATE','BEGIN_HOUR','END_HOUR','BEGIN_SNAP_ID','END_SNAP_ID']
    l_data = [p_dbinfo[0],p_dbinfo[2],p_schema_name,p_reportdate,p_begin_hour,p_end_hour,p_begin_snap_id,p_end_snap_id]
    
    mytab = p_page << table(border='1',width=1000)
    headtr = mytab << tr()
    for i in range(0,len(l_header)):
        td_tmp = headtr << td(l_header[i])
        td_tmp.attributes['class']='awrbg'
        td_tmp.attributes['align']='center'
    
    tabtr = mytab << tr()
    for o in l_data:
        td_tmp = tabtr << td(o)
        td_tmp.attributes['class']='awrc'
        td_tmp.attributes['align']='center'

def print_html_snap_tab(p_page,p_snap_info):
    p_page << br()
    l_header = ['SNAP_ID','BEGIN_TIME','END_TIME']
    l_data = p_snap_info

    p_page << h3('ORACLE SNAPSHOT INFORMATION', cl='awr')
    mytab = p_page << table(border='1',width=400)
    headtr = mytab << tr()
    for i in range(0,len(l_header)):
        td_tmp = headtr << td(l_header[i])
        td_tmp.attributes['class']='awrbg'
        td_tmp.attributes['align']='center'
    
    for j in range(0,len(l_data)):
        tabtr = mytab << tr()
        for i in range(0,len(l_data[j])):
            td_tmp = tabtr << td(l_data[j][i])
            td_tmp.attributes['class']='awrc'
            td_tmp.attributes['align']='center'
            if j%2==0:
                td_tmp.attributes['class']='awrc'
            else:
                td_tmp.attributes['class']='awrnc'

    p_page << br()

def print_html_sql_tab(p_page,p_sqldata,p_sqltype,p_header):
    l_page = p_page
    l_data = p_sqldata
    l_header = p_header
    l_page << h3(p_sqltype, cl='awr')

    mytab = l_page << table(border='1',width=1200)
    headtr = mytab << tr()
    for i in range(0,len(l_header)):
        td_tmp = headtr << td(l_header[i])
        td_tmp.attributes['class']='awrbg'
        td_tmp.attributes['align']='center'

    for j in range(0,len(l_data)):
        tabtr = mytab << tr()
        for i in range(0,len(l_data[j])):
            
            if i==0:  #sql_id
                td_tmp = tabtr << td()
                td_tmp.attributes['class']='awrc'
                a_tmp = td_tmp<<a(l_data[j][i])
                a_tmp.attributes['class']='awr'
                a_tmp.attributes['href']='#'+l_data[j][i]
            else:
                if l_data[j][i]:
                    td_tmp = tabtr << td(l_data[j][i])
                else:
                    td_tmp = tabtr << td('0')

            if j%2==0:
                td_tmp.attributes['class']='awrc'
            else:
                td_tmp.attributes['class']='awrnc'
            if i==(len(l_data[j])-1):
                td_tmp.attributes['align']='left'
            else:
                td_tmp.attributes['align']='right'
               
    l_page << br()

def print_html_ora_obj_size_tab(p_page,p_data):
    l_page = p_page
    l_data = p_data
    l_header = ['OBJECT_NAME','OBJECT_TYPE','PARENT_NAME','OBJECT_SIZE(GB)']

    l_page << h3('ORACLE OBJECT SIZE', cl='awr')

    mytab = l_page << table(border='1',width=800)
    headtr = mytab << tr()
    for i in range(0,len(l_header)):
        td_tmp = headtr << td(l_header[i])
        td_tmp.attributes['class']='awrbg'
        td_tmp.attributes['align']='center'

    for o in l_data:
        tabtr = mytab << tr()
        for i in range(0,len(o)):
            td_tmp = tabtr << td(o[i])
            td_tmp.attributes['class']='awrc'
            if i==0 or i==1 or i==2:
                td_tmp.attributes['align']='left'
            else:
                td_tmp.attributes['align']='right'
    p_page << br()

def print_html_ora_obj_rows_tab(p_page,p_data):
    l_page = p_page
    l_data = p_data
    l_header = ['OBJECT_NAME','NUMBER_ROWS','LAST_ANALYZED']

    l_page << h3('ORACLE OBJECT NUMBER', cl='awr')

    mytab = l_page << table(border='1',width=800)
    headtr = mytab << tr()
    for i in range(0,len(l_header)):
        td_tmp = headtr << td(l_header[i])
        td_tmp.attributes['class']='awrbg'
        td_tmp.attributes['align']='center'

    for o in l_data:
        tabtr = mytab << tr()
        for i in range(0,len(o)):
            td_tmp = tabtr << td(o[i])
            td_tmp.attributes['class']='awrc'
            if i==0:
                td_tmp.attributes['align']='left'
            else:
                td_tmp.attributes['align']='right'
    p_page << br()

def print_html_sql_header(p_page,p_sql_id,p_sql_text):
    p_page << h4('sql id : '+ p_sql_id, cl='awr')
    l_header = "SQL Text"
    l_data = p_sql_text
    mytab = p_page << table(border='1',width=1200)

    headtr = mytab << tr()
    td_tmp = headtr << td(l_header)
    td_tmp.attributes['class']='awrbg'
    td_tmp.attributes['align']='center'
    
    a_tmp = td_tmp<<a()
    a_tmp.attributes['class']='awrc'
    a_tmp.attributes['name']=p_sql_id


    tabtr = mytab << tr()
    td_tmp = tabtr << td(l_data)
    td_tmp.attributes['class']='awrc'
    td_tmp.attributes['align']='left'

    p_page << br()

def print_html_sql_exec(p_page,p_sql_exec):
    l_page = p_page
    l_data = p_sql_exec
    l_header = ['INSTANCE_NUMBER','SNAP_ID','PLAN_HASH_VALUE','BEGIN_INTERVAL_TIME']
    
    mytab = l_page << table(border='1',width=800)
    headtr = mytab << tr()
    for i in range(0,len(l_header)):
        td_tmp = headtr << td(l_header[i])
        td_tmp.attributes['class']='awrbg'
        td_tmp.attributes['align']='center'

    for j in range(0,len(l_data)):
        tabtr = mytab << tr()
        for i in range(0,len(l_data[j])):
            td_tmp = tabtr << td(l_data[j][i])
            td_tmp.attributes['class']='awrc'
            td_tmp.attributes['align']='right'
            if j%2==0:
                td_tmp.attributes['class']='awrc'
            else:
                td_tmp.attributes['class']='awrnc'
    l_page << br()

def print_html_sql_plan(p_page,p_sql_id,p_sql_plan_hash_value,p_sql_plan_data,p_output_type):

    l_page = p_page
    l_data = p_sql_plan_data
    l_header = ['Id','Operation','Options','Object_Owner','Object_Name','Depth','Cost']
    
    mytab = l_page << table(border='0')
    headtr = mytab << tr()
    td_tmp = headtr << td()
    td_tmp << h5('plan hash value : '+ p_sql_plan_hash_value, cl='awr')
    td_tmp = headtr << td()
    div_tmp = td_tmp << div(id='div_'+p_sql_id+'_'+p_sql_plan_hash_value,style='cursor:hand',onclick="isHidden('"+'div_'+p_sql_id+'_'+p_sql_plan_hash_value+"','tab_"+p_sql_id+'_'+p_sql_plan_hash_value+"')")
    if p_output_type=='FILE':
        div_tmp << h5('+', cl='awr')
        mytab = l_page << table(id='tab_'+p_sql_id+'_'+p_sql_plan_hash_value,border='1',style="display:none")
    else:
        mytab = l_page << table(id='tab_'+p_sql_id+'_'+p_sql_plan_hash_value,border='1',style="display:table")
        
    headtr = mytab << tr()
    for i in range(0,len(l_header)):
        td_tmp = headtr << td(l_header[i])
        td_tmp.attributes['class']='awrbg'
        td_tmp.attributes['align']='center'

    for j in range(0,len(l_data)):
        tabtr = mytab << tr()
        for i in range(0,len(l_data[j])):
            if l_data[j][i]:
                td_tmp = tabtr << td(str(l_data[j][i]).replace(' ','&nbsp;'))
            else:
                td_tmp = tabtr << td()
            td_tmp.attributes['class']='awrc'
            td_tmp.attributes['align']='left'
            if j%2==0:
                td_tmp.attributes['class']='awrc'
            else:
                td_tmp.attributes['class']='awrnc'
    l_page << br()

def send_rpt_mail(p_page,p_rpt_emails,p_report_date,p_report_dbinfo):
    html_tmpfile='/tmp/html_tmpfile'
    html_text=''

    msgRoot = MIMEMultipart('related')
    msgRoot['Subject'] = 'Report Database Report_'+p_report_dbinfo[0]+' (' + p_report_date +')'

    p_page.printOut(file=html_tmpfile)

    fo=open(html_tmpfile)
    htmltext=fo.read()
    fo.close()

    msgText = MIMEText(htmltext,'html')
    msgRoot.attach(msgText)
    
    smtp = smtplib.SMTP()
    smtp.connect('mail.abc.cn')
    smtp.login("user@mail.abc.cn", "xxx")
    for mail_address in p_rpt_emails:
        smtp.sendmail("user@mail.abc.cn",mail_address, msgRoot.as_string())
    smtp.quit()

def print_help():
    print "Usage:"
    print "    ./monitor_schema.py -i <ip> -s <schema_name> -u <object_num>|-z <object_size> -d <yyyy-mm-dd> -b <hour> -e <hour> -n <sql_num> -f html_file|-m abc@xxx.com"
    print "    -i : database ip address"
    print "    -s : schema name"
    print "    -u : object size/number top n (by number)"
    print "    -d : report date"
    print "    -b : report begin hour (between 0 and 23)"
    print "    -e : report end hour (between 0 and 23)"
    print "    -n : query sql number"
    print "    -f : output file name"
    print "    -m : output email address"

if __name__ == "__main__":
    v_dbinfos=[]
    for o in open('/monitor/rpt_db.ini').readlines():
        v_dbinfos.append(o.replace('\n','').split('|'))

    v_report_dbinfo=[]
    v_report_dbinfos=[]
    v_instance_name=""
    v_schema_name=""
    v_report_date=""
    v_begin_hour=""
    v_end_hour=""
    v_begin_snap_id=""
    v_end_snap_id=""
    v_rpt_filename=""
    v_rpt_emails=[]
    v_snap_data=[]
    v_options=[]
    v_output_type=""   #FILE or EMAIL
    v_db_id=""
    v_inst_num=""
    v_sql_num="10"      #default value is 10
    v_sqldata=[]
    v_tuning_sql=[]     #tuning sql list
    v_sql_exec=[]      #sql execute history info
    v_sql_plan=[]
    v_obj_num=""
    v_objdata=[]
    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:s:d:b:e:f:m:n:u:")

        for o,v in opts:
            if o == "-i":
                for db in v_dbinfos:
                    if db[0]==v:
                        v_report_dbinfos.append(db)
            elif o == "-d":
                v_report_date = v.upper()
                if v_report_date == "NOW":
                    v_report_date = time.strftime('%Y-%m-%d')
            elif o == "-b":
                v_begin_hour=v
                if int(v_begin_hour)<0 or int(v_begin_hour)>23:
                    print_help()
                    exit()
            elif o == "-e":
                v_end_hour=v
                if int(v_end_hour)<0 or int(v_end_hour)>23 or int(v_end_hour)<int(v_begin_hour):
                    print_help()
                    exit()
            elif o == "-n":
                v_sql_num = v
            elif o == "-f":
                v_rpt_filename = v
            elif o == "-m":
                v_rpt_emails = v.split(',')
            elif o == "-u":
                v_obj_num = v
	    elif o == "-s":
                v_schema_name = v.upper()

        if v_instance_name=="":
            v_report_dbinfo=v_report_dbinfos[0]
        else:
            for o in v_report_dbinfos:
                if v_instance_name==o[2]:
                    v_report_dbinfo = o
                    break
        if v_obj_num=="":
            print_help()
            exit()

        if v_schema_name=="":
            print_help()
            exit()
        elif query_schema_exist(v_report_dbinfo,v_schema_name)==False:
            print "Schema: "+v_schema_name+" doesn't exist in db "+v_report_dbinfo[2]
            exit()

        if v_rpt_filename<>"" and len(v_rpt_emails)>0:
            print_help()
            exit()

        if v_rpt_filename=="" and len(v_rpt_emails)==0:
            print_help()
            exit()

        if v_rpt_filename<>"":
            v_output_type="FILE"
        else:
            v_output_type="EMAIL"

        if v_rpt_filename.upper() == 'DEFAULT':
            v_rpt_filename='rpt_db_'+v_report_dbinfo[0].replace('.','_')+'_'+v_report_dbinfo[2]+'_'+v_schema_name+'_'+v_report_date.replace('-','_')+'.html'

        if '.' not in v_rpt_filename:
            v_rpt_filename=v_rpt_filename+'.html'
    except getopt.GetoptError,msg:
        print_help()
        exit()

    v_begin_snap_id = str(query_begin_snap_id(v_report_dbinfo,v_report_date,v_begin_hour))
    if v_begin_snap_id == "None":
        print "can't find begin snap id,exit with error!"
        exit()
  
    v_end_snap_id = str(query_end_snap_id(v_report_dbinfo,v_report_date,v_end_hour))
    if v_end_snap_id == "None":
        print "can't find end snap idi,exit with error!"
        exit()

    v_page = print_html_header(v_report_dbinfo,v_schema_name)
    print_db_header(v_page,v_report_dbinfo,v_schema_name,v_report_date,v_begin_hour,v_end_hour,v_begin_snap_id,v_end_snap_id)
    
    v_page << br()
   
    v_objdata = query_ora_obj_size_by_num(v_report_dbinfo,v_schema_name,v_obj_num)
    print_html_ora_obj_size_tab(v_page,v_objdata)

    v_objdata = query_ora_obj_by_rows(v_report_dbinfo,v_schema_name,v_obj_num)
    print_html_ora_obj_rows_tab(v_page,v_objdata)


    v_db_id = str(query_db_id(v_report_dbinfo))
    v_inst_num = str(query_inst_num(v_report_dbinfo))
    # print sql ordered by elapsed time
    v_header=['SQL Id','Elapsed Time(s)','CPU TIME(s)','Executions','Elap per Exec(s)','SQL Module','SQL Text']
    v_sql = SQL_ORDERED_BY_ELAPSED_TIME.\
        replace('&schema_name',"'"+v_schema_name+"'").\
        replace('&beg_snap',v_begin_snap_id).\
        replace('&end_snap',v_end_snap_id).\
        replace('&dbid',v_db_id).\
        replace('&inst_num',v_inst_num).\
        replace('&sql_num',v_sql_num)            
    v_sqldata = query_sql_data(v_report_dbinfo,v_sql)
    print_html_sql_tab(v_page,v_sqldata,'SQL order by Elapsed Time',v_header)

    # print sql ordered by cpu time
    for o in v_sqldata:
        if o[0] not in v_tuning_sql:
            v_tuning_sql.append(o[0])
    v_header=['SQL Id','Elapsed Time(s)','CPU TIME(s)','Executions','Elap per Exec(s)','SQL Module','SQL Text']
    v_sql = SQL_ORDERED_BY_CPU_TIME.\
            replace('&schema_name',"'"+v_schema_name+"'").\
            replace('&beg_snap',v_begin_snap_id).\
            replace('&end_snap',v_end_snap_id).\
            replace('&dbid',v_db_id).\
            replace('&inst_num',v_inst_num).\
            replace('&sql_num',v_sql_num)
    v_sqldata = query_sql_data(v_report_dbinfo,v_sql)
    print_html_sql_tab(v_page,v_sqldata,'SQL order by CPU Time',v_header)

    # print sql ordered by gets
    for o in v_sqldata:
        if o[0] not in v_tuning_sql:
            v_tuning_sql.append(o[0])
    v_header=['SQL Id','Buffer Gets','Executions','Gets per Exec','% Total','CPU Time(s)','Elapsed Time(s)','SQL Module','SQL Text']
    v_sql = SQL_ORDERED_BY_GETS.\
            replace('&schema_name',"'"+v_schema_name+"'").\
            replace('&beg_snap',v_begin_snap_id).\
            replace('&end_snap',v_end_snap_id).\
            replace('&dbid',v_db_id).\
            replace('&inst_num',v_inst_num).\
            replace('&sql_num',v_sql_num)
    v_sqldata = query_sql_data(v_report_dbinfo,v_sql)
    print_html_sql_tab(v_page,v_sqldata,'SQL order by Gets',v_header)

    # print sql ordered by reads
    for o in v_sqldata:
        if o[0] not in v_tuning_sql:
            v_tuning_sql.append(o[0])
    v_header=['SQL Id','Pyhsical Reads','Executions','Reads per Exec','% Total','CPU Time(s)','Elapsed Time(s)','SQL Module','SQL Text']
    v_sql = SQL_ORDERED_BY_READS.\
            replace('&schema_name',"'"+v_schema_name+"'").\
            replace('&beg_snap',v_begin_snap_id).\
            replace('&end_snap',v_end_snap_id).\
            replace('&dbid',v_db_id).\
            replace('&inst_num',v_inst_num).\
            replace('&sql_num',v_sql_num)
    v_sqldata = query_sql_data(v_report_dbinfo,v_sql)
    print_html_sql_tab(v_page,v_sqldata,'SQL order by Reads',v_header)
            
    # print sql ordered by executions
    for o in v_sqldata:
        if o[0] not in v_tuning_sql:
            v_tuning_sql.append(o[0])
    v_header=['SQL Id','Executions','Rows Processed','Rows per Exec','CPU per Exec(s)','Elap per Exec(s)','SQL Module','SQL Text']
    v_sql = SQL_ORDERED_BY_EXECUTIONS.\
            replace('&schema_name',"'"+v_schema_name+"'").\
            replace('&beg_snap',v_begin_snap_id).\
            replace('&end_snap',v_end_snap_id).\
            replace('&dbid',v_db_id).\
            replace('&inst_num',v_inst_num).\
            replace('&sql_num',v_sql_num)
    v_sqldata = query_sql_data(v_report_dbinfo,v_sql)
    print_html_sql_tab(v_page,v_sqldata,'SQL order by Executions',v_header)
            
    # print sql detail info
    for o in v_sqldata:
        if o[0] not in v_tuning_sql:
            v_tuning_sql.append(o[0])
    v_page << br()
    v_page << h1('', cl='awr')
    for o in v_tuning_sql:
        v_sql_id = o
        v_sql_text = query_sql_text(v_report_dbinfo,v_sql_id)
        print_html_sql_header(v_page,v_sql_id,v_sql_text)
        v_sql_exec = query_sql_exec(v_report_dbinfo,v_sql_id,v_begin_snap_id,v_end_snap_id)
        print_html_sql_exec(v_page,v_sql_exec)
        v_sql_plan = query_sql_plan(v_report_dbinfo,v_sql_id,v_begin_snap_id,v_end_snap_id) #hash value
        # print sql plan
        for p in v_sql_plan:
            v_sql_plan_hash_value = str(p[0])
            v_sql_plan_data = query_plan_data(v_report_dbinfo,v_sql_id,v_sql_plan_hash_value)
            print_html_sql_plan(v_page,v_sql_id,v_sql_plan_hash_value,v_sql_plan_data,v_output_type)
        v_page << br()
        v_page << h1('', cl='awr')

    if v_output_type == "FILE":
        v_page.printOut(file=v_rpt_filename)    
        print "create report database report file ... " + v_rpt_filename
    elif v_output_type == "EMAIL":
        send_rpt_mail(v_page,v_rpt_emails,v_report_date,v_report_dbinfo)
        print "mail report database report to ... " + ",".join(list(v_rpt_emails))
