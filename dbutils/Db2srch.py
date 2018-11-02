import ibm_db_sa
import os
import datetime
from sqlalchemy import *
from sqlalchemy.engine import reflection

import configparser
import logging
import sys
import time
import subprocess ### calling external program
import dbutils.propertyReader as pr


def querydb(tab,srcCol, srcValue,tab_pks,strSql   ):

    #strSql = ""
    #strSql = "SELECT * from {}.{} a  WHERE {} = '{}' ".format(prop_dict['des.db.schema'],tab,srcCol,srcValue)
    print("SQL = ", strSql)
    updtSql= ""
    print("========================================================================================================\n")
#    tabmeta = Table(tab, metadata, autoload=True, autoload_with=db2, schema='UNIPROD')
    rs = db2.execute(strSql)
    #srchitems['BASE_ORDER_NBR_CODE'] = 'U0075400003647'
    #srchitems['QUOTE_ID'] = 'EF22CD84000003423708ABF2642C97D4'
    print("procesing tab:{}; for column:{}; value {}".format(tab,srcCol, srcValue))
    print("========================================================================================================\n")
    print("========================================================================================================\n")
    try:
        for row in rs:
            updtSql="Update {}.{} set ".format(prop_dict['des.db.schema'], tab)
            for col, colvalue in row.items():
                for k, v in srchitems.items():
                    if v in str(colvalue).upper().strip():
                        print ("colanme :{} value: {}; new value {}".format(col, colvalue, k))
                        updtSql = updtSql + " \n\t{} = '{}' /*replacing {}*/,\n".format(col, k, colvalue)
                        for pk, pkval in tab_pks.items(): #getting the pk value for updating by PKS
                            tab_pks[pk] = row[pk]
            print (updtSql)
            if (updtSql!= "Update {}.{} set ".format(prop_dict['des.db.schema'],tab)):
                logging.info("Update SQL before getUpdateSql call :\n {}\n".format(updtSql))
                updtSql = getUpdateSql(updtSql,tab_pks, srcCol, srcValue )
                logging.info("Update SQL for {}:\n {}\n".format(tab,updtSql))

    except Exception:
        logging.error("Error while gen update sql for {}:: \n {}\n".format(tab,updtSql))
        logging.critical(sys.exc_info()[0])
    finally:
        rs.close();

##############################################################################################
def getUpdateSql(sUpdSql, tab_pks, search_col, search_col_value ):
    swhereCon = " \n\t1 = 1 "
    for pk,val  in tab_pks.items():  # getting the pk value for updating by PKS
        swhereCon  = swhereCon + " \n\tand {} \t= '{}'".format(pk, val)
    sUpdSql = sUpdSql + " Where {}".format(swhereCon )  + " \n\tand {} \t= '{}'".format( search_col,search_col_value)
    return sUpdSql.format(swhereCon)
###############################################################################################

def loaddic(sfilename):
    sPhaseLvl_QVDTable_Name = {}
    with open(sfilename ) as fh:
        for line in fh:
            colname, Colvalue= line.strip().split(',')
            sPhaseLvl_QVDTable_Name[colname] = Colvalue.strip()
    return sPhaseLvl_QVDTable_Name

def srchdb_1():
    sPhaseLvl_QVDTable_Name = {}

    sPhaseLvl_QVDTable_Name['OPPORTUNITY_ID'] = 'EF16FFF9000003423708ABF26A026A8D'
    sPhaseLvl_QVDTable_Name['BASE_ORDER_NBR_CODE'] = 'U0075400003647'
    sPhaseLvl_QVDTable_Name['QUOTE_ID'] = 'EF22CD84000003423708ABF2642C97D4'
    sPhaseLvl_QVDTable_Name = {}
    #sPhaseLvl_QVDTable_Name = eval(open(prop_dict['tab.col.value']).read())

    sPhaseLvl_QVDTable_Name = loaddic(prop_dict['tab.col.value'])

    srchitems = loaddic(prop_dict['tab.col.val.search'])
    sSelSql = ""
    schemaObjs = db2.table_names(sSchema)

    for tab in schemaObjs:
        if (tab.upper()=='CLAIM_02_RAW'):
            try:
                tabmeta = Table(tab, metadata, autoload=True, autoload_with=db2, schema=sSchema)
                tab_pks = {}
                for pk in tabmeta.primary_key:
                    print(pk)
                    tab_pks[pk.name]=pk.name
                if sPhaseLvl_QVDTable_Name:
                    for key, val in sPhaseLvl_QVDTable_Name.items():
                       # print(key, val)
                        for col in tabmeta.c:
                            if (col.name.upper()== key.upper()):
                                sSelSql = genSelSql(tabmeta, col, val)
                                print(sSelSql)
                                querydb(tabmeta.name.upper(), col.name.upper(), val, tab_pks,sSelSql)
                else:
                    sSelSql = genSelSqlnoWhere(tabmeta)
                    querydb(tabmeta.name.upper(), "", "", tab_pks,sSelSql)


            except Exception:
                pass

def genSelSql(tabobj, colobj, colvalue ):
    return "SELECT  * FROM {}.{}  a  WHERE {} = '{}'".format(prop_dict['des.db.schema'], tabobj.name.upper(), colobj.name.upper(), colvalue )

def genSelSqlnoWhere(tabobj):
    #dynamically dteremine ty[pe of database and use limit/fetch / rownnum <100]
    return "SELECT  * FROM {}.{}  a  fetch first 100 rows only".format(prop_dict['des.db.schema'], tabobj.name.upper())




########################################################################################################################
config = pr.getPropInstance(  os.path.dirname(__file__) , '/PycharmProjects/QvdGen/NLP.properties')

prop_dict = dict(config.items('QVS_SETUP'))
### LOG FILE
scriptName = sys.argv[0].split("/")[-1][0:-3]
dateTimeStamp = time.strftime('%Y%m%d%H%M%S') #in the format YYYYMMDDHHMMSS
userName = os.getlogin()
logFile = prop_dict["logfilepath"]  + scriptName + "_" + userName + "_" + dateTimeStamp + ".log"

## LOGGING SETTINGS
logging.basicConfig(filename=logFile,level=logging.DEBUG, format=prop_dict["log.format"], datefmt=prop_dict["log.date.format"])
root = logging.getLogger()
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(prop_dict["log.format"], datefmt=prop_dict["log.date.format"])
ch.setFormatter(formatter)
root.addHandler(ch)
logging.info("Starting ")


db_uri= prop_dict['des.db.type']+"://" + prop_dict['des.db.user'] + ":" + prop_dict['des.db.password'] + "@" + prop_dict['des.db.server']  + ":" + prop_dict['des.db.port']  + "/" + prop_dict['des.db']
#db_uri='ibm_db_sa://tgaj2:MMF8PGRS@qad551a:50000/DSDWHS01'

#db_uri='ibm_db_sa://tgaj2:MMF8PGRS@prdd551a:50000/dpdwhs01'
db2 = create_engine(db_uri)
sSchema= prop_dict['des.db.schema']
#logging.info(db2.table_names(sSchema))
schemaObjs = db2.table_names(sSchema)

metadata = MetaData(bind=db2)
insp = reflection.Inspector.from_engine(db2)
logging.info(insp.get_table_names(schema=sSchema))

srchitems = {}
srchdb_1()