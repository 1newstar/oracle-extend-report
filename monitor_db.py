#!/usr/bin/python
# -*- coding: utf-8 -*-  
import sys
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
import pprint
import getopt
import time
import matplotlib.pyplot as plt  
import cx_Oracle
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from matplotlib.ticker import MultipleLocator, FormatStrFormatter
from pyh import *
OTS = {
	2:'PHYS_IO_MBPS (MB/s)',
	3:'PHYS_IO_READ (MB/s)',
	4:'PHYS_IO_WRITE (MB/s)',
	5:'REDO_GENERATED (MB/s)',
	6:'PHYS_IO_IOPS',
	7:'PYHS_READ_REQS',
	8:'PYHS_WRITE_REQS',
	9:'REDO_WRITES',
	10:'OS_LOAD',
	11:'DB_CPU_USAGE',
	12:'OS_CPU_UTIL(%)',
	13:'NET_TRANS (MB/S)'}

DTS = {
	2:'DB_Time (Minute)',
	3:'REDO_SIZE (MB)',
	4:'REDO_GENERATED (MB/s)',
	5:'LOGICAL_READ',
	6:'LOGICAL_READ_PER_SEC',
	7:'PHYICAL_READ',
	8:'PHYICAL_READ_PER_SEC',
	9:'EXECUTIONS',
	10:'EXECUTIONS_PER_SEC',
	11:'PARSE',
	12:'PARSE_PER_SEC',
	13:'HARD_PARSE',
    14:'HARD_PARSE_PER_SEC',
    15:'TRANSACTIONS',
    16:'TRANSACTIONS_PER_SEC'
    }

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
def query_ora_obj_size_by_num(p_dbinfo,p_num):
    db = p_dbinfo
    conn=cx_Oracle.connect(db[3]+'/'+db[4]+'@'+db[0]+':'+db[1]+'/'+db[2])
    cursor = conn.cursor()
    cursor.execute("select * from (select owner,segment_name,segment_type,round(bytes/1024/1024/1024,2) size_gb from dba_segments  where segment_name not like 'BIN%' order by 4 desc) where rownum<="+p_num)
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return records

def query_ora_obj_size_by_size(p_dbinfo,p_size):
    db = p_dbinfo
    conn=cx_Oracle.connect(db[3]+'/'+db[4]+'@'+db[0]+':'+db[1]+'/'+db[2])
    cursor = conn.cursor()
    cursor.execute("select owner,segment_name,segment_type,round(bytes/1024/1024/1024,2) size_gb from dba_segments  where segment_name not like 'BIN%' and bytes>"+p_size+"*1024*1024*1024 order by 4 desc")
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

def query_os_data(p_dbinfo,p_begin_snap_id,p_end_snap_id):
    conn=cx_Oracle.connect(p_dbinfo[3]+'/'+p_dbinfo[4]+'@'+p_dbinfo[0]+':'+p_dbinfo[1]+'/'+p_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute("""
select to_char(min(begin_time),'hh24:mi:ss'), to_char(max(end_time),'hh24:mi:ss'),
    round(sum(case metric_name when 'Physical Read Total Bytes Per Sec' then maxval end)/1024/1024,2) + 
    round(sum(case metric_name when 'Physical Write Total Bytes Per Sec' then maxval end)/1024/1024,2) + 
    round(sum(case metric_name when 'Redo Generated Per Sec' then maxval end)/1024/1024,2) Phys_IO_MBps, 
    round(sum(case metric_name when 'Physical Read Total Bytes Per Sec' then maxval end)/1024/1024,2) Phys_IO_Read,
    round(sum(case metric_name when 'Physical Write Total Bytes Per Sec' then maxval end)/1024/1024,2) Phys_IO_Write,
    round(sum(case metric_name when 'Redo Generated Per Sec' then maxval end)/1024/1024,2) Redo_Generated_Per_Sec, 
    round(sum(case metric_name when 'Physical Read Total IO Requests Per Sec' then maxval end),2) +
    round(sum(case metric_name when 'Physical Write Total IO Requests Per Sec' then maxval end),2) +
    round(sum(case metric_name when 'Redo Writes Per Sec' then maxval end),2) Phys_IO_IOPS,
    round(sum(case metric_name when 'Physical Read Total IO Requests Per Sec' then maxval end),2) Phys_read_requests,
    round(sum(case metric_name when 'Physical Write Total IO Requests Per Sec' then maxval end),2) phys_write_requests,
    round(sum(case metric_name when 'Redo Writes Per Sec' then maxval end),2) redo_writes,
    round(sum(case metric_name when 'Current OS Load' then maxval end),2) OS_LOad,
    round(sum(case metric_name when 'CPU Usage Per Sec' then maxval end),2) DB_CPU_Usage_per_sec,
    round(sum(case metric_name when 'Host CPU Utilization (%)' then maxval end),2) Host_CPU_util,
    round(sum(case metric_name when 'Network Traffic Volume Per Sec' then maxval end)/1024/1024,2) Network_MBs 
from dba_hist_sysmetric_summary 
where snap_id between """+ p_begin_snap_id +""" and """+p_end_snap_id +""" group by snap_id order by snap_id""")
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return records

def query_db_data(p_dbinfo,p_report_date,p_begin_hour,p_end_hour):
    conn=cx_Oracle.connect(p_dbinfo[3]+'/'+p_dbinfo[4]+'@'+p_dbinfo[0]+':'+p_dbinfo[1]+'/'+p_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute("""
SELECT  DECODE (s.redosize, NULL, '--shutdown or end--', s.currtime)||':00:00' "TIME",
         TO_CHAR (ROUND (s.seconds / 60, 2)) "elapse(min)",
         ROUND (t.db_time / 1000000 / 60, 2) "DB time(min)",
         ROUND(s.redosize/1024/1024,2) redo,
         ROUND (s.redosize / s.seconds/1024/1024, 2) "redo/s",
         s.logicalreads logical,
         ROUND (s.logicalreads / s.seconds, 2) "logical/s",
         physicalreads physical,
         ROUND (s.physicalreads / s.seconds, 2) "phy/s",
         s.executes execs,
         ROUND (s.executes / s.seconds, 2) "execs/s",
         s.parse,
         ROUND (s.parse / s.seconds, 2) "parse/s",
         s.hardparse,
         ROUND (s.hardparse / s.seconds, 2) "hardparse/s",
         s.transactions trans,
         ROUND (s.transactions / s.seconds, 2) "trans/s"
FROM 
(
    SELECT curr_redo - last_redo redosize,
         curr_logicalreads - last_logicalreads logicalreads,
         curr_physicalreads - last_physicalreads physicalreads,
         curr_executes - last_executes executes,
         curr_parse - last_parse parse,
         curr_hardparse - last_hardparse hardparse,
         curr_transactions - last_transactions transactions,
         ROUND ( ( (currtime + 0) - (lasttime + 0)) * 3600 * 24, 0) seconds,
         TO_CHAR (currtime, 'yyyy-mm-dd') snap_date,
         TO_CHAR (currtime, 'hh24') currtime,
         currsnap_id endsnap_id,
         TO_CHAR (startup_time, 'yyyy-mm-dd hh24:mi:ss') startup_time
    FROM 
    (  
        SELECT a.redo last_redo,
          a.logicalreads last_logicalreads,
          a.physicalreads last_physicalreads,
          a.executes last_executes,
          a.parse last_parse,
          a.hardparse last_hardparse,
          a.transactions last_transactions,
          LEAD (a.redo,1,NULL) OVER (PARTITION BY b.startup_time ORDER BY b.end_interval_time) curr_redo,
          LEAD (a.logicalreads,1,NULL) OVER (PARTITION BY b.startup_time ORDER BY b.end_interval_time) curr_logicalreads,
          LEAD (a.physicalreads,1,NULL) OVER (PARTITION BY b.startup_time ORDER BY b.end_interval_time) curr_physicalreads,
          LEAD (a.executes,1,NULL) OVER (PARTITION BY b.startup_time ORDER BY b.end_interval_time) curr_executes,
          LEAD (a.parse,1,NULL) OVER (PARTITION BY b.startup_time ORDER BY b.end_interval_time) curr_parse,
          LEAD (a.hardparse,1,NULL) OVER (PARTITION BY b.startup_time ORDER BY b.end_interval_time) curr_hardparse,
          LEAD (a.transactions,1,NULL) OVER (PARTITION BY b.startup_time ORDER BY b.end_interval_time) curr_transactions,
          b.end_interval_time lasttime,
          LEAD (b.end_interval_time,1,NULL) OVER (PARTITION BY b.startup_time ORDER BY b.end_interval_time) currtime,
          LEAD (b.snap_id,1,NULL) OVER (PARTITION BY b.startup_time ORDER BY b.end_interval_time) currsnap_id,
          b.startup_time
        FROM 
        (  
            SELECT snap_id,
                dbid,
                instance_number,
                SUM (DECODE (stat_name, 'redo size', VALUE, 0)) redo,
                SUM (DECODE (stat_name,'session logical reads', VALUE,0)) logicalreads,
                SUM (DECODE (stat_name,'physical reads', VALUE,0)) physicalreads,
                SUM (DECODE (stat_name,'execute count', VALUE,0)) executes,
                SUM (DECODE (stat_name,'parse count (total)', VALUE,0)) parse,
                SUM (DECODE (stat_name,'parse count (hard)', VALUE,0)) hardparse,
                SUM (DECODE (stat_name,'user rollbacks', VALUE,'user commits', VALUE,0)) transactions
            FROM dba_hist_sysstat
            WHERE stat_name IN('redo size','session logical reads','physical reads','execute count','user rollbacks','user commits','parse count (hard)','parse count (total)')
            GROUP BY snap_id, dbid, instance_number
        ) a,dba_hist_snapshot b
        WHERE a.snap_id = b.snap_id
            AND a.dbid = b.dbid
            AND a.instance_number = b.instance_number
        ORDER BY end_interval_time
    )
) s,
(
    SELECT LEAD (a.VALUE,1,NULL) OVER (PARTITION BY b.startup_time ORDER BY b.end_interval_time) - a.VALUE db_time,
         LEAD (b.snap_id,1,NULL) OVER (PARTITION BY b.startup_time ORDER BY b.end_interval_time) endsnap_id
    FROM dba_hist_sys_time_model a, dba_hist_snapshot b
    WHERE a.snap_id = b.snap_id
        AND a.dbid = b.dbid
        AND a.instance_number = b.instance_number
        AND a.stat_name = 'DB time'
) t
WHERE s.endsnap_id = t.endsnap_id and s.snap_date='"""+p_report_date+"""' and s.currtime between """+p_begin_hour+""" and """+p_end_hour+"""
ORDER BY s.endsnap_id""")
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return records

def print_html_header():
    page = PyH('ERP SQL Report')
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

    page << h1('ERP SQL Report', cl='awr')
    return page

def print_db_header(p_page,p_dbinfo,p_reportdate,p_begin_hour,p_end_hour,p_begin_snap_id,p_end_snap_id):
    p_page << br()
    l_header = ['DATABASE IP','DATABASE NAME','INSTANCE_NAME','REPORT_DATE','BEGIN_HOUR','END_HOUR','BEGIN_SNAP_ID','END_SNAP_ID']
    l_data = [p_dbinfo[0],p_dbinfo[5],p_dbinfo[2],p_reportdate,p_begin_hour,p_end_hour,p_begin_snap_id,p_end_snap_id]
    
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

def print_html_os_tab(p_page,p_drawdata):
    l_page = p_page
    l_data = p_drawdata
    l_header = ['BEGIN_TIME','END_TIME','PHYS_IO_MBPS\n(MB/s)','PHYS_IO_READ*\n(MB/s)','PHYS_IO_WRITE*\n(MB/s)','REDO_GENERATED*\n(MB/s)','PHYS_IO_IOPS','PHYS_READ_REQS*','PHYS_WRITE_REQS*','REDO_WRITES*','OS_LOAD','DB_CPU_USAGE','OS_CPU_UTIL(%)','NET_TRANS\n(MB/s)']
    l_page << h3('ORACLE OS INFORMATION', cl='awr')

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

    l_page <<li("* PHYS_IO_MBPS = PHYS_IO_READ + PHYS_IO_WRITE + REDO_GENERATED",cl='awr')
    l_page <<li("* PHYS_IO_IOPS = PHYS_READ_REQUESTS + PHYS_WRITE_REQUESTS + REDO_WRITES",cl='awr')
    l_page << br()

def print_html_db_tab(p_page,p_drawdata):
    l_page = p_page
    l_data = p_drawdata
    l_header = ['TIME','ELAPSED_TIME\n(minute)','DB_TIME\n(minute)','REDO_SIZE\n(MB)','REDO_SIZE\n(MB/s)','LOGICAL_READ','LOGICAL_READ\n(NUM/s)','PHYSICAL_READ','PHYSICAL_READ\n(NUM/s)','EXECUTIONS','EXECUTIONS\n(NUM/s)','PARSES','PARSE\n(NUM/s)','HARD_PARSE','HARD_PARSE\n(NUM/s)','TRANSACTIONS','TRANSACTIONS\n(NUM/s)']
    l_page << h3('ORACLE DB INFORMATION', cl='awr')

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
            #<TD class=awrc><A class=awr href="#9n7u34nfn7q01">9n7u34nfn7q01</A></TD>
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

def print_html_os_db_pic(p_page,p_pic_draw_files,p_output):
    l_page = p_page
    l_output = p_output

    mytab_p = l_page << table()
    headtr_p = mytab_p << tr()
    for o in p_pic_draw_files:
        td_p_2 = headtr_p << td()
        if l_output == "FILE":
            img1 = td_p_2 << img(src=o)
        else:
            img1 = td_p_2 << img(src='cid:'+o.split('.')[0])
        img1.attributes['align']='left'

def print_html_ora_obj_size_tab(p_page,p_data):
    l_page = p_page
    l_data = p_data
    l_header = ['OBJECT_OWNER','OBJECT_NAME','OBJECT_TYPE','OBJECT_SIZE(GB)']

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
            if i==0 or i==1:
                td_tmp.attributes['align']='left'
            else:
                td_tmp.attributes['align']='right'
    p_page << br()

def draw_line_pic(p_drawdata,p_col,p_title,p_report_dbinfo,p_report_date):
    l_drawdata = p_drawdata
    l_x = []
    l_y = []
    for o in l_drawdata:
        l_x.append(o[0].split(':')[0])
        l_y.append(o[p_col])

    fig = plt.figure(figsize=(5,2.4))
    ax = fig.add_subplot(1,1,1)
    plt.ylim(0,round(max(l_y)))
#    plt.xlim(0,23)
    plt.xlim(int(float(min(l_x))),int(float(max(l_x))))
#    plt.xlabel("Time")
#    plt.ylabel(p_title)
    plt.title(p_title)
    plt.plot(l_x,l_y,'b-')

    plt.bar(left=(1,1),height=(1,1),width = 0,align="center",color='w')   
    plt.grid(True)
    filename=p_report_dbinfo[0].replace('.','_')+'_'+p_report_dbinfo[2]+'_'+p_report_date.replace('-','_')+'_'+p_title.split('(')[0].replace(' ','').lower()+'_'+str(p_col)+'_line.png'
    #v_report_dbinfo.replace('.','_')

    plt.savefig(filename)
    return filename

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

def send_rpt_mail(p_page,p_pic_files,p_rpt_emails,p_report_date,p_report_dbinfo):
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
    
    for img in p_pic_files:
        fp = open(img, 'rb')
        msgImage = MIMEImage(fp.read())
        fp.close()
        msgImage.add_header('Content-ID', img.split('.')[0])
        msgRoot.attach(msgImage)

    smtp = smtplib.SMTP()
    smtp.connect('mail.abc.cn')
    smtp.login("user@mail.abc.cn", "xxx")
    for mail_address in p_rpt_emails:
        smtp.sendmail("user@mail.abc.cn",mail_address, msgRoot.as_string())
    smtp.quit()

def print_html_pic(p_page,p_pic_filename):
    img1 = p_page << img(src=p_pic_filename)
    img1.attributes['align']='left'

def print_help():
    print "Usage:"
    print "    ./monitor_ora_day.py -i <IP> -u <obj_num>|-z <obj_size> -d <YYYY-MM-DD> -b <hour> -e <hour> -o OBJ,OS,DB,SQL -q n1,n2... -w m1,m2... -n <sql_num> -f html_file|-m abc@xxx.com"
    print "    -i : database ip address"
    print "    -s : database instance name (may be null)"
    print "    -u : object size top n (by number)"
    print "    -z : object size top n (by size unit GB)"
    print "    -d : report date"
    print "    -b : report begin hour (between 0 and 23)"
    print "    -e : report end hour (between 0 and 23)"
    print "    -o : show information type"
    print "       ALL - show obj,os,db,sql information"
    print "       OBJ - show obj information"
    print "       OS  - show os information"
    print "       DB  - show db information"
    print "       SQL - show sql information"
    print "    -q : os information show pic"
    print "       0  - all pic show"
    print "       1  - not to specified"
    print "       2  - Physical IO Total MBPS"
    print "       3  - Physical Read Total Bytes Per Sec (MB/s)"
    print "       4  - Physical Write Total Bytes Per Sec (MB/s)"
    print "       5  - Redo Generated Per Sec (MB/s)"
    print "       6  - Physical IO TOTAL IOPS"
    print "       7  - Physical Read Total IO Requests Per Sec"
    print "       8  - Physical Write Total IO Requests Per Sec"
    print "       9  - Redo Writes Per Sec"
    print "       10 - Current OS Load"
    print "       11 - CPU Usage Per Sec"
    print "       12 - Host CPU Utilization (%)"
    print "       13 - Network Traffic Volume Per Sec (MB/s)"
    print "    -w : db information show pic"
    print "       0  - all pic show"
    print "       1  - not to specified"
    print "       2  - DB Time (minute)"
    print "       3  - Total Redo Size (MB)"
    print "       4  - Redo Generated Per Sec(MB/s)"
    print "       5  - Logical Read"
    print "       6  - Logical Read Per Sec"
    print "       7  - Phyical Read"
    print "       8  - Phyical Read Per Sec"
    print "       9  - Executions"
    print "       10 - Executions Per Sec"
    print "       11 - Parse"
    print "       12 - Parse Per Sec"
    print "       13 - Hard Parse"
    print "       14 - Hard Parse Per Sec"
    print "       15 - Transactions"
    print "       16 - Transactions Per Sec"
    print "    -n : query sql number"
    print "    -f : output file name"
    print "    -m : output email address"

if __name__ == "__main__":
    v_dbinfos=[]
    for o in open('rpt_db.ini').readlines():
        v_dbinfos.append(o.replace('\n','').split('|'))

    v_report_dbinfo=[]
    v_report_dbinfos=[]
    v_instance_name=""
    v_report_date=""
    v_begin_hour=""
    v_end_hour=""
    v_begin_snap_id=""
    v_end_snap_id=""
    v_rpt_filename=""
    v_rpt_emails=[]
    v_snap_data=[]
    v_options=[]
    v_ots=[]
    v_dts=[]
    v_output_type=""   #FILE or EMAIL
    v_pic_files=[]
    v_pic_draw_files=[]
    v_db_id=""
    v_inst_num=""
    v_sql_num="10"      #default value is 10
    v_sqldata=[]
    v_tuning_sql=[]     #tuning sql list
    v_sql_exec=[]      #sql execute history info
    v_sql_plan=[]
    v_obj_num=""
    v_obj_size=""
    v_objdata=[]
    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:d:b:e:o:f:m:q:w:n:u:z:s:")

        for o,v in opts:
            if o == "-i":
                for db in v_dbinfos:
                    if db[0]==v:
                        v_report_dbinfos.append(db)
            elif o == "-s":
                v_instance_name = v
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
            elif o == "-o":
                if v.upper() == "ALL":
                    v="OBJ,OS,DB,SQL"
                v_options=v.upper().split(',')
            elif o == "-q":  # ot
                if v=="0" or v.upper()=="ALL":
                    v="2,3,4,5,6,7,8,9,10,11,12,13"
                v_ots=v.upper().split(',')
            elif o == "-w":  # dt
                if v=="0" or v.upper()=="ALL":
                    v="2,3,4,5,6,7,8,9,10,11,12,13,14,15,16"
                v_dts=v.upper().split(',')
            elif o == "-n":
                v_sql_num = v
            elif o == "-f":
                v_rpt_filename = v
            elif o == "-m":
                v_rpt_emails = v.split(',')
            elif o == "-u":
                v_obj_num = v
            elif o == "-z":
                v_obj_size = v

        if v_instance_name=="":
            v_report_dbinfo=v_report_dbinfos[0]
        else:
            for o in v_report_dbinfos:
                if v_instance_name==o[2]:
                    v_report_dbinfo = o
                    break
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
            v_rpt_filename='rpt_db_'+v_report_dbinfo[0].replace('.','_')+'_'+v_report_dbinfo[2]+'_'+v_report_date.replace('-','_')+'.html'
        if '.' not in v_rpt_filename:
            v_rpt_filename=v_rpt_filename+'.html'
    except getopt.GetoptError,msg:
        print_help()
        exit()
    v_begin_snap_id = str(query_begin_snap_id(v_report_dbinfo,v_report_date,v_begin_hour))
    v_end_snap_id = str(query_end_snap_id(v_report_dbinfo,v_report_date,v_end_hour))
    v_page = print_html_header()
    print_db_header(v_page,v_report_dbinfo,v_report_date,v_begin_hour,v_end_hour,v_begin_snap_id,v_end_snap_id)
    
    if "OBJ" in v_options:
        v_page << br()
        v_options.remove('OBJ')
        if v_obj_num!="":
            v_objdata = query_ora_obj_size_by_num(v_report_dbinfo,v_obj_num)
            print_html_ora_obj_size_tab(v_page,v_objdata)
        else:
            v_objdata = query_ora_obj_size_by_size(v_report_dbinfo,v_obj_size)
            print_html_ora_obj_size_tab(v_page,v_objdata)

    v_snap_data = query_snap_data(v_report_dbinfo,v_begin_snap_id,v_end_snap_id)
    print_html_snap_tab(v_page,v_snap_data)

    for option in v_options:
        if cmp(option,"OS")==0:
            v_drawdata = query_os_data(v_report_dbinfo,v_begin_snap_id,v_end_snap_id)
            print_html_os_tab(v_page,v_drawdata)
            v_pic_draw_files=[]
            for o in v_ots:
                v_pic_draw_files.append(draw_line_pic(v_drawdata,int(o),OTS[int(o)],v_report_dbinfo,v_report_date))
                if len(v_pic_draw_files)==3:
                    print_html_os_db_pic(v_page,v_pic_draw_files,v_output_type)
                    v_pic_files = v_pic_files + v_pic_draw_files
                    v_pic_draw_files=[]
            if len(v_pic_draw_files)>0:
                print_html_os_db_pic(v_page,v_pic_draw_files,v_output_type)
                v_pic_files = v_pic_files + v_pic_draw_files
            v_page << br()
            v_page << h1('', cl='awr')
        elif cmp(option,"DB")==0:
            v_drawdata = query_db_data(v_report_dbinfo,v_report_date,v_begin_hour,v_end_hour)
            print_html_db_tab(v_page,v_drawdata)
            v_pic_draw_files=[]
            for o in v_dts:
                v_pic_draw_files.append(draw_line_pic(v_drawdata,int(o),DTS[int(o)],v_report_dbinfo,v_report_date))
                if len(v_pic_draw_files)==3:
                    print_html_os_db_pic(v_page,v_pic_draw_files,v_output_type)
                    v_pic_files = v_pic_files + v_pic_draw_files
                    v_pic_draw_files=[]
            if len(v_pic_draw_files)>0:
                print_html_os_db_pic(v_page,v_pic_draw_files,v_output_type)
                v_pic_files = v_pic_files + v_pic_draw_files
            v_page << br()
            v_page << h1('', cl='awr')
        elif cmp(option,"SQL")==0:
            v_db_id = str(query_db_id(v_report_dbinfo))
            v_inst_num = str(query_inst_num(v_report_dbinfo))
            # print sql ordered by elapsed time
            v_header=['SQL Id','Elapsed Time(s)','CPU TIME(s)','Executions','Elap per Exec(s)','SQL Module','SQL Text']
            v_sql = SQL_ORDERED_BY_ELAPSED_TIME.\
                replace('&beg_snap',v_begin_snap_id).\
                replace('&end_snap',v_end_snap_id).\
                replace('&dbid',v_db_id).\
                replace('&inst_num',v_inst_num).\
                replace('&sql_num',v_sql_num)            
            v_sqldata = query_sql_data(v_report_dbinfo,v_sql)
            print_html_sql_tab(v_page,v_sqldata,'SQL order by Elapsed Time',v_header)

            for o in v_sqldata:
                if o[0] not in v_tuning_sql:
                    v_tuning_sql.append(o[0])
            # print sql ordered by cpu time
            v_header=['SQL Id','Elapsed Time(s)','CPU TIME(s)','Executions','Elap per Exec(s)','SQL Module','SQL Text']
            v_sql = SQL_ORDERED_BY_CPU_TIME.\
                replace('&beg_snap',v_begin_snap_id).\
                replace('&end_snap',v_end_snap_id).\
                replace('&dbid',v_db_id).\
                replace('&inst_num',v_inst_num).\
                replace('&sql_num',v_sql_num)
            v_sqldata = query_sql_data(v_report_dbinfo,v_sql)
            print_html_sql_tab(v_page,v_sqldata,'SQL order by CPU Time',v_header)
            
            for o in v_sqldata:
                if o[0] not in v_tuning_sql:
                    v_tuning_sql.append(o[0])
            # print sql ordered by gets
            v_header=['SQL Id','Buffer Gets','Executions','Gets per Exec','% Total','CPU Time(s)','Elapsed Time(s)','SQL Module','SQL Text']
            v_sql = SQL_ORDERED_BY_GETS.\
                replace('&beg_snap',v_begin_snap_id).\
                replace('&end_snap',v_end_snap_id).\
                replace('&dbid',v_db_id).\
                replace('&inst_num',v_inst_num).\
                replace('&sql_num',v_sql_num)
            v_sqldata = query_sql_data(v_report_dbinfo,v_sql)
            print_html_sql_tab(v_page,v_sqldata,'SQL order by Gets',v_header)

            for o in v_sqldata:
                if o[0] not in v_tuning_sql:
                    v_tuning_sql.append(o[0])

            # print sql ordered by reads
            v_header=['SQL Id','Pyhsical Reads','Executions','Reads per Exec','% Total','CPU Time(s)','Elapsed Time(s)','SQL Module','SQL Text']
            v_sql = SQL_ORDERED_BY_READS.\
                replace('&beg_snap',v_begin_snap_id).\
                replace('&end_snap',v_end_snap_id).\
                replace('&dbid',v_db_id).\
                replace('&inst_num',v_inst_num).\
                replace('&sql_num',v_sql_num)
            v_sqldata = query_sql_data(v_report_dbinfo,v_sql)
            print_html_sql_tab(v_page,v_sqldata,'SQL order by Reads',v_header)
            
            for o in v_sqldata:
                if o[0] not in v_tuning_sql:
                    v_tuning_sql.append(o[0])
            # print sql ordered by executions
            v_header=['SQL Id','Executions','Rows Processed','Rows per Exec','CPU per Exec(s)','Elap per Exec(s)','SQL Module','SQL Text']
            v_sql = SQL_ORDERED_BY_EXECUTIONS.\
                replace('&beg_snap',v_begin_snap_id).\
                replace('&end_snap',v_end_snap_id).\
                replace('&dbid',v_db_id).\
                replace('&inst_num',v_inst_num).\
                replace('&sql_num',v_sql_num)
            v_sqldata = query_sql_data(v_report_dbinfo,v_sql)
            print_html_sql_tab(v_page,v_sqldata,'SQL order by Executions',v_header)
            
            for o in v_sqldata:
                if o[0] not in v_tuning_sql:
                    v_tuning_sql.append(o[0])
            v_page << br()
            v_page << h1('', cl='awr')
            # print sql detail info
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

        else:
            print_help()
            exit()
        
    if v_output_type == "FILE":
        v_page.printOut(file=v_rpt_filename)    
        print "create report database report file ... " + v_rpt_filename
    elif v_output_type == "EMAIL":
        send_rpt_mail(v_page,v_pic_files,v_rpt_emails,v_report_date,v_report_dbinfo)
        print "mail report database report to ... " + ",".join(list(v_rpt_emails))
