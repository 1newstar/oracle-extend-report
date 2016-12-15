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
import matplotlib.pyplot as plt  
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from pyh import *

SQL_ORDERED_BY_USAGE="""
SELECT SERVICE_NAME,
       ROUND(DB_TIME / 1000000, 1),
       ROUND(DB_CPU / 1000000, 1),
       PHY_READS,
       LOG_READS
  FROM (SELECT S1.SERVICE_NAME,
               SUM(DECODE(S1.STAT_NAME, 'DB time', S1.DIFF, 0)) DB_TIME,
               SUM(DECODE(S1.STAT_NAME, 'DB CPU', S1.DIFF, 0)) DB_CPU,
               SUM(DECODE(S1.STAT_NAME, 'physical reads', S1.DIFF, 0)) PHY_READS,
               SUM(DECODE(S1.STAT_NAME, 'session logical reads', S1.DIFF, 0)) LOG_READS
          FROM (SELECT E.SERVICE_NAME SERVICE_NAME,
                       E.STAT_NAME STAT_NAME,
                       E.VALUE - B.VALUE DIFF
                  FROM DBA_HIST_SERVICE_STAT B, DBA_HIST_SERVICE_STAT E
                 WHERE B.SNAP_ID = &beg_snap
                   AND E.SNAP_ID = &end_snap
                   AND B.INSTANCE_NUMBER = &inst_num
                   AND E.INSTANCE_NUMBER = &inst_num
                   AND B.DBID = &dbid
                   AND E.DBID = &dbid
                   AND B.STAT_ID = E.STAT_ID
                   AND B.SERVICE_NAME_HASH = E.SERVICE_NAME_HASH) S1
         GROUP BY S1.SERVICE_NAME
         ORDER BY DB_TIME DESC, SERVICE_NAME)
"""
def query_ora_schema_size(p_dbinfo):
    db = p_dbinfo
    conn=cx_Oracle.connect(db[3]+'/'+db[4]+'@'+db[0]+':'+db[1]+'/'+db[2])
    cursor = conn.cursor()
    cursor.execute("SELECT owner,round(SUM(bytes)/1024/1024,0) size_gb,0 from dba_segments WHERE owner NOT IN ('ANONYMOUS','CTXSYS','DBSNMP','EXFSYS','MDDATA','MDSYS','MGMT_VIEW','OLAPSYS','ORDPLUGINS','ORDSYS','OUTLN','SCOTT','SI_INFORMTN_SCHEMA','SYS','SYSMAN','SYSTEM','WK_TEST','WKPROXY','WKSYS','WMSYS','XDB') GROUP BY owner ORDER BY size_gb DESC")
    records = list(cursor.fetchall())
    cursor.execute("select round(sum(bytes)/1024/1024,0) from dba_segments")
    l_total_size = cursor.fetchall()[0][0]
    
    results = []
    l_sum_size=0
    l_sum_pct=0.0
    for rec in records:
        results.append([rec[0],rec[1],round(float(rec[1])*100/l_total_size,2)])
        l_sum_size+=rec[1]
        l_sum_pct+=round(float(rec[1])*100/l_total_size,2)
    results.append(['ORACLE_INTERNAL',l_total_size -l_sum_size,round(100.0-l_sum_pct,2)])
   
    cursor.close()
    conn.close()
    return results

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

def draw_pie_size_pic(p_dbinfo,p_reportdate,p_drawdata):
    l_drawdata = p_drawdata
    l_labels = []
    l_sizes = []
    l_nums = []
    for o in l_drawdata:
        l_labels.append(o[0])
        l_sizes.append(o[1])
        l_nums.append(o[2])
    fig = plt.figure(figsize=(6,6))
    ax = fig.add_subplot(1,1,1)
    ax.pie(l_sizes, labels=l_labels, autopct='%1.1f%%') 
    filename=p_dbinfo[0].replace('.','_')+'_'+p_reportdate.replace('-','_')+'_pie_size.png'
    plt.savefig(filename)
    return filename

def draw_pie_usage_pic(p_dbinfo,p_reportdate,p_drawdata,p_usage_type):
    l_type = 0
    l_total_val=0
    l_sum_pct=0.0
    l_draw = []

    if p_usage_type == 'DB_TIME':
        l_type = 1
    elif p_usage_type == 'CPU_TIME':
        l_type = 2
    elif p_usage_type == 'PHY_READS':
        l_type = 3
    elif p_usage_type == 'LOG_READS':
        l_type = 4

    for rec in p_drawdata:
        l_total_val+=rec[l_type]

    for rec in p_drawdata:
        l_draw.append([rec[0],rec[l_type],round(float(rec[l_type])*100/l_total_val,2)])

    l_labels = []
    l_sizes = []
    l_nums = []
    for o in l_draw:
        l_labels.append(o[0])
        l_sizes.append(o[1])
        l_nums.append(o[2])
    fig = plt.figure(figsize=(6,6))
    ax = fig.add_subplot(1,1,1)
    ax.pie(l_sizes, labels=l_labels, autopct='%1.1f%%')
    filename=p_dbinfo[0].replace('.','_')+'_'+p_reportdate.replace('-','_')+'_pie_usage_'+p_usage_type+'.png'
    plt.savefig(filename)
    return filename

def print_html_header(p_dbinfo):
    page = PyH(p_dbinfo[2]+' Schema Usage Report')
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
    return page

def print_db_header(p_page,p_dbinfo,p_reportdate,p_begin_hour,p_end_hour,p_begin_snap_id,p_end_snap_id):
    p_page << br()
    l_header = ['DATABASE IP','INSTANCE_NAME','REPORT_DATE','BEGIN_HOUR','END_HOUR','BEGIN_SNAP_ID','END_SNAP_ID']
    l_data = [p_dbinfo[0],p_dbinfo[2],p_reportdate,p_begin_hour,p_end_hour,p_begin_snap_id,p_end_snap_id]
    
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

def print_html_usage_tab(p_page,p_data,p_sqltype,p_header):
    l_page = p_page
    l_data = p_data
    l_header = p_header
    l_page << h3(p_sqltype, cl='awr')

    mytab = l_page << table(border='1',width=800)
    headtr = mytab << tr()
    for i in range(0,len(l_header)):
        td_tmp = headtr << td(l_header[i])
        td_tmp.attributes['class']='awrbg'
        td_tmp.attributes['align']='center'

    for j in range(0,len(l_data)):
        tabtr = mytab << tr()
        for i in range(0,len(l_data[j])):
            td_tmp = tabtr << td()
            if l_data[j][i]:
                td_tmp = tabtr << td(l_data[j][i])
            else:
                td_tmp = tabtr << td('0')

            if j%2==0:
                td_tmp.attributes['class']='awrc'
            else:
                td_tmp.attributes['class']='awrnc'

            if i==1:
                td_tmp.attributes['align']='left'
            else:
                td_tmp.attributes['align']='right'
               
    l_page << br()

def print_html_schema_size_tab_pic(p_page,p_sizedata,p_pic_filename,p_output_type):
    l_page = p_page
    l_data = p_sizedata
    l_output = p_output_type
    l_header = ['SCHEMA_NAME','SCHEMA_SIZE(GB)','TOTAL_PCT(%)']

    l_page << h3('ORACLE SCHEMA SIZE', cl='awr')

    mytab_p = l_page<<table()
    headtr_p = mytab_p<<tr()
    td_p_1 = headtr_p<<td()

    mytab = td_p_1<< table(border='1',width=400)  ##left table
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
            if i==0 :
                td_tmp.attributes['align']='left'
            else:
                td_tmp.attributes['align']='right'

    td_p_2 = headtr_p<<td()
    td_p_2.attributes['valign']="top"
    if l_output == "FILE":
        img1 = td_p_2 << img(src=p_pic_filename)
    else:
        img1 = td_p_2 << img(src='cid:'+p_pic_filename.split('.')[0])
    img1.attributes['align']='left'
    img1.attributes['align']='top'

    p_page << br()

def print_html_schema_usage_tab_pic(p_page,p_usage_data,p_usage_type,p_pic_filename,p_output_type):
    l_page = p_page
    l_data = p_usage_data
    l_output = p_output_type
    l_header = ['SCHEMA_NAME',p_usage_type,'TOTAL_PCT(%)']
    l_type = 0
    l_total_val=0
    l_sum_pct=0.0
    l_draw = []

    if p_usage_type == 'DB_TIME':
        l_type = 1
    elif p_usage_type == 'CPU_TIME':
        l_type = 2 
    elif p_usage_type == 'PHY_READS':
        l_type = 3
    elif p_usage_type == 'LOG_READS':
        l_type = 4

    for rec in l_data:
        l_total_val+=rec[l_type]

    for rec in l_data:
        l_draw.append([rec[0],rec[l_type],round(float(rec[l_type])*100/l_total_val,2)])
    l_draw=sorted(l_draw,key=lambda l_draw:l_draw[2],reverse=True)

    l_page << h3('ORACLE SCHEMA ordered by '+p_usage_type, cl='awr')

    mytab_p = l_page<<table()
    headtr_p = mytab_p<<tr()
    td_p_1 = headtr_p<<td()

    mytab = td_p_1<< table(border='1',width=400)  ##left table
    headtr = mytab << tr()
    for i in range(0,len(l_header)):
        td_tmp = headtr << td(l_header[i])
        td_tmp.attributes['class']='awrbg'
        td_tmp.attributes['align']='center'

    for o in l_draw:
        tabtr = mytab << tr()
        for i in range(0,len(o)):
            td_tmp = tabtr << td(o[i])
            td_tmp.attributes['class']='awrc'
            if i==0 :
                td_tmp.attributes['align']='left'
            else:
                td_tmp.attributes['align']='right'

    td_p_2 = headtr_p<<td()
    td_p_2.attributes['valign']="top"
    if l_output == "FILE":
        img1 = td_p_2 << img(src=p_pic_filename)
    else:
        img1 = td_p_2 << img(src='cid:'+p_pic_filename.split('.')[0])

    p_page << br()


def send_rpt_mail(p_page,p_pic_files,p_rpt_emails,p_report_date,p_report_dbinfo):
    html_tmpfile='/tmp/html_tmpfile'
    html_text=''

    msgRoot = MIMEMultipart('related')
    msgRoot['Subject'] = 'Report Schema Report_'+p_report_dbinfo[0]+' (' + p_report_date +')'

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

def print_help():
    print "Usage:"
    print "    ./rpt_schema_usage.py -i <ip> -d <yyyy-mm-dd> -b <hour> -e <hour> -f html_file|-m abc@xxx.com"
    print "    -i : database ip address"
    print "    -d : report date"
    print "    -b : report begin hour (between 0 and 23)"
    print "    -e : report end hour (between 0 and 23)"
    print "    -f : output file name"
    print "    -m : output email address"

if __name__ == "__main__":
    v_dbinfos=[]
    for o in open('/monitor/rpt_db.ini').readlines():
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
    v_output_type=""   #FILE or EMAIL
    v_db_id=""
    v_inst_num=""
    v_pic_filename=""
    v_pic_files=[]
    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:d:b:e:f:m:")

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
            elif o == "-f":
                v_rpt_filename = v
            elif o == "-m":
                v_rpt_emails = v.split(',')

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
            v_rpt_filename='rpt_schema_usage_'+v_report_dbinfo[0].replace('.','_')+'_'+v_report_dbinfo[2]+'_'+v_report_date.replace('-','_')+'.html'

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

    v_db_id = str(query_db_id(v_report_dbinfo))
    v_inst_num = str(query_inst_num(v_report_dbinfo))

    v_page = print_html_header(v_report_dbinfo)
    print_db_header(v_page,v_report_dbinfo,v_report_date,v_begin_hour,v_end_hour,v_begin_snap_id,v_end_snap_id)
    v_page << br()
   
    # print schema size tab and pic
    v_sizedata = query_ora_schema_size(v_report_dbinfo)
    v_pic_filename = draw_pie_size_pic(v_report_dbinfo,v_report_date,v_sizedata)
    print_html_schema_size_tab_pic(v_page,v_sizedata,v_pic_filename,v_output_type)
    if v_output_type == "EMAIL":
        v_pic_files.append(v_pic_filename)


    # print schema usage tab and pic
    v_usage_sql = SQL_ORDERED_BY_USAGE.\
                replace('&beg_snap',v_begin_snap_id).\
                replace('&end_snap',v_end_snap_id).\
                replace('&dbid',v_db_id).\
                replace('&inst_num',v_inst_num)
    v_usage_data = query_sql_data(v_report_dbinfo,v_usage_sql)

    v_pic_filename = draw_pie_usage_pic(v_report_dbinfo,v_report_date,v_usage_data,'DB_TIME')
    print_html_schema_usage_tab_pic(v_page,v_usage_data,'DB_TIME',v_pic_filename,v_output_type)
    if v_output_type == "EMAIL":
        v_pic_files.append(v_pic_filename)

    v_pic_filename = draw_pie_usage_pic(v_report_dbinfo,v_report_date,v_usage_data,'CPU_TIME')
    print_html_schema_usage_tab_pic(v_page,v_usage_data,'CPU_TIME',v_pic_filename,v_output_type)
    if v_output_type == "EMAIL":
        v_pic_files.append(v_pic_filename)

    v_pic_filename = draw_pie_usage_pic(v_report_dbinfo,v_report_date,v_usage_data,'PHY_READS')
    print_html_schema_usage_tab_pic(v_page,v_usage_data,'PHY_READS',v_pic_filename,v_output_type)
    if v_output_type == "EMAIL":
        v_pic_files.append(v_pic_filename)

    v_pic_filename = draw_pie_usage_pic(v_report_dbinfo,v_report_date,v_usage_data,'LOG_READS')
    print_html_schema_usage_tab_pic(v_page,v_usage_data,'LOG_READS',v_pic_filename,v_output_type)
    if v_output_type == "EMAIL":
        v_pic_files.append(v_pic_filename)

    if v_output_type == "FILE":
        v_page.printOut(file=v_rpt_filename)    
        print "create report database report file ... " + v_rpt_filename
    elif v_output_type == "EMAIL":
        send_rpt_mail(v_page,v_pic_files,v_rpt_emails,v_report_date,v_report_dbinfo)
        print "mail report database report to ... " + ",".join(list(v_rpt_emails))
