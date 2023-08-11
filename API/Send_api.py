import os
from datetime import datetime, timedelta
import requests
import json
import cx_Oracle
import pandas as pd
import time

connect = cx_Oracle.connect("DB_ID", "DB_PW", "host/DB")
c = connect.cursor()

today = datetime.today().strftime('%Y-%m-%d')
yesterday = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d')

## DB 조회 
FLT = pd.read_sql("SELECT * FROM BIS_FLT_SCH_HD WHERE FLT_DT BETWEEN TO_DATE('"+yesterday+"', 'YYYY-MM-DD') AND TO_DATE('"+today+"', 'YYYY-MM-DD')", con=connect)

time.sleep(5)
connect.close()

FLT_COPY = FLT.copy()
FLT_COPY = FLT_COPY.fillna('')

FLT_COPY['FLT_DT'] = FLT_COPY['FLT_DT'].astype(str)
FLT_COPY['REG_DT'] = FLT_COPY['REG_DT'].astype(str)
FLT_COPY['UPD_DT'] = FLT_COPY['UPD_DT'].astype(str)
FLT_COPY['FLT_SEQ'] = FLT_COPY['FLT_SEQ'].astype(str)

##### 받는곳 맞게 이름 변경
FLT_COPY.rename(columns={'FLT_NO': 'fltNo', 'FLT_DT': 'fltDt', 'FLT_SEQ': 'fltSeq',
                         'FM_LOC_CD': 'fmLocCd', 'TO_LOC_CD': 'toLocCd', 'FLT_STATUS': 'fltStatus',
                         'FLT_TP': 'fltTp', 'AIRCRAFT_TP': 'aircraftTp', 'HL_NO': 'hlNo',
                         'FLT_AIRLINE_NM': 'fltAirlineNm', 'FLT_AIRLINE_CD': 'fltAirlineCd',
                         'FLT_STD': 'fltStd', 'FLT_ETD': 'fltEtd', 'FLT_ATD': 'fltAtd',
                         'FLT_STA': 'fltSta', 'FLT_ETA': 'fltEta', 'FLT_ATA': 'fltAta',
                         'FLT_TIME': 'fltTime', 'REG_USR_NO': 'regUsrNo', 'REG_DT': 'regDt',
                         'UPD_USR_NO': 'updUsrNo', 'UPD_DT': 'updDt'}, inplace=True)

FLT_COPY['rowNum'] = ''

for i in range(len(FLT_COPY)):
    FLT_COPY['rowNum'][i] = str(i + 1)

FLT_COPY = FLT_COPY[['rowNum', 'fltNo', 'fltDt', 'fltSeq', 'fmLocCd', 'toLocCd', 'fltStatus',
       'fltTp', 'aircraftTp', 'hlNo', 'fltAirlineNm', 'fltAirlineCd', 'fltStd',
       'fltEtd', 'fltAtd', 'fltSta', 'fltEta', 'fltAta', 'fltTime',
       'regUsrNo', 'regDt', 'updUsrNo', 'updDt']]

flt_params = FLT_COPY.to_dict('records')

api_url = 'api 주소'
apiDate = datetime.now().strftime('%Y%m%d')

params = {'emitter': 'BIS',
          'eventCode': 'UPDATE_BIS_FLT_SCH_HD',
          'eventDate': apiDate,
          'payload': flt_params
          }

headers = {'Content-Type': 'application/json;charset=utf-8'}

response = requests.post(url=api_url, data=json.dumps(params), headers=headers)
status_code = response.status_code

print(status_code)
print(response.text)
