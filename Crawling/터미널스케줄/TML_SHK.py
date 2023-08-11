import time
import pandas as pd
from bs4 import BeautifulSoup
import chromedriver_autoinstaller
from selenium import webdriver
from pytz import timezone
import datetime
import cx_Oracle
import re

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.alert import Alert
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

import logging
import warnings
from logging.handlers import RotatingFileHandler
from src.DbUtil.ENV import DBEnv


def Crawling(download_path, target_db_name):

  chrome_options = Options()
  chrome_options.add_argument("--headless")
  chrome_options.add_argument("--no-sandbox")
  chrome_options.add_argument('--disable-dev-shm-usage')
  chrome_options.add_argument('--window-size=1920x1080')
  chrome_options.add_experimental_option('prefs', {'download.default_directory':download_path})
  
  chrome_ver = chromedriver_autoinstaller.get_chrome_version().split('.')[0]  # 크롬드라이버 버전 확인
  
  if chrome_ver != '99':
      chromedriver_autoinstaller.install(True)
      driver = webdriver.Chrome(service=Service('/home/ec2-user/chromedriver_linux64/chromedriver'), options=chrome_options)
  else:
      # chromedriver_autoinstaller.install(True)
      driver = webdriver.Chrome(service=Service('/home/ec2-user/chromedriver_linux64/chromedriver'), options=chrome_options)
  
  driver.get('https://cpluswsc.hit.com.hk/frontpage/#/')
  
  driver.get('http://eport.scctcn.com/query/VesselSchedule')
  
  driver.implicitly_wait(5)
  driver.maximize_window()
  
  time.sleep(2)
  
  xconnection = DBEnv.xConnection(target_db_name)
  cursor = xconnection.cursor()
  
  now = datetime.datetime.now()
  n_day = now + datetime.timedelta(days=14)
  sql = "SELECT VSL_NAME FROM SCH_VSL_CRR WHERE PORT_CD IN ('CNSHK') AND ETB_DT BETWEEN TO_DATE('"+str(now.strftime('%Y-%m-%d'))+"', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('"+str(n_day.strftime('%Y-%m-%d'))+"', 'YYYY-MM-DD HH24:MI:SS') GROUP BY VSL_NAME"
  
  VSL_LIST = pd.read_sql(sql, con=xconnection)
  
  cursor.close()
  xconnection.close()
  
  res = driver.page_source
  soup = BeautifulSoup(res, 'html.parser')
  
  title = soup.select('#divIndex > div.searchInde-show > div.searchInde-mtable > table > thead.pfix > tr')[0].text.split('\n')[1:22]
  
  total = []
  f_list = []
  try:
      for i in range(len(VSL_LIST)):
          driver.find_element(By.XPATH, '//*[@id="divIndex"]/div[1]/div[1]/div[1]/ul/li[2]/div/input').click()
  
          driver.find_element(By.XPATH, '//*[@id="divIndex"]/div[1]/div[1]/div[1]/ul/li[2]/div/input').send_keys(VSL_LIST['VSL_NAME'][i])
          driver.implicitly_wait(5)
          
          try:
              driver.find_element(By.XPATH, '//*[@id="divIndex"]/div[1]/div[1]/div[1]/ul/li[2]/div/div/table/tbody/tr').click()
          except Exception as e:
              pass
  
          driver.implicitly_wait(5)
  
          res = driver.page_source
          soup = BeautifulSoup(res, 'html.parser')
  
          option = len(soup.select('div.searchInde-search-text > ul > li:nth-child(3) > select > option'))
  
          driver.implicitly_wait(5)
          try:
  
              if option >= 2:
                  for ot in range(1, option):
                      driver.find_element(By.XPATH, '//*[@id="divIndex"]/div[1]/div[1]/div[1]/ul/li[3]/select').click()
                      driver.find_element(By.XPATH, '//*[@id="divIndex"]/div[1]/div[1]/div[1]/ul/li[3]/select/option['+str(ot)+']').click()
                      driver.find_element(By.XPATH, '//*[@id="divIndex"]/div[1]/div[1]/div[2]/a').click()
  
                      time.sleep(1)
                      res = driver.page_source
                      soup = BeautifulSoup(res, 'html.parser')
                      time.sleep(1)
  
                      try:
                          tr = soup.select('#divIndex > div.searchInde-show > div.searchInde-mtable > table > tbody > tr')[
                                   0].text.split('\n')[1:22]
                      except Exception as e:
                          print(e)
                          pass
  
                      total.append(tr)
              else:
                  time.sleep(1)
                  driver.find_element(By.XPATH, '//*[@id="divIndex"]/div[1]/div[1]/div[2]/a').click()
  
  
                  time.sleep(1)
                  res = driver.page_source
                  soup = BeautifulSoup(res, 'html.parser')
                  time.sleep(1)
  
                  try:
                      tr = soup.select('#divIndex > div.searchInde-show > div.searchInde-mtable > table > tbody > tr')[0].text.split('\n')[1:22]
                  except Exception as e:
                      pass
  
                  total.append(tr)
          except Exception as e:
              f_list.append(VSL_LIST['VSL_NAME'][i])
              pass
  
          driver.find_element(By.XPATH, '//*[@id="divIndex"]/div[1]/div[1]/div[1]/ul/li[2]/div/input').clear()
  
          driver.implicitly_wait(3)
  except Exception as e:
      print(e)
  
  driver.quit()
  
  SHK = pd.DataFrame(data=total, columns=title)
  
  SHK = SHK.drop_duplicates()
  
  SHK.rename(columns={SHK.columns[1]:'ETA_DT', SHK.columns[2]:'SLAN_CD', SHK.columns[6]: 'VSL_NAME',
                      SHK.columns[7]:'CRR_CD', SHK.columns[8]:'수입', SHK.columns[9]:'VOY_NO',
                      SHK.columns[11]:'ETB_DT', SHK.columns[12]:'ETD_DT',SHK.columns[13]:'ATA_DT',
                      SHK.columns[14]:'ATD_DT'}, inplace=True)
  
  SHK.reset_index(drop=True, inplace=True)
  
  for j in range(len(SHK)):
      SHK.loc[j, 'VOY_NO'] = SHK.loc[j, '수입'] + '-' + SHK.loc[j, 'VOY_NO']
      if SHK.loc[j, 'ETB_DT'] != '':
          SHK.loc[j, 'ETB_DT'] = SHK.loc[j, 'ETB_DT'] + ' 00:00:00'
  
  SHK['TML_CD'] = 'SHK'
  SHK['VVD_CD'] = ''
  
  now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
  SHK['REG_USR_NO'] = '0'
  SHK['REG_DT'] = now
  SHK['UPD_USR_NO'] = '0'
  SHK['UPD_DT'] = now
  SHK['SHIFT_CNT'] = 0
  SHK['DISC_CNT'] = 0
  SHK['LOAD_CNT'] = 0
  
  SHK['DISC_CNT'] = SHK['DISC_CNT'].astype(float)
  SHK['LOAD_CNT'] = SHK['LOAD_CNT'].astype(float)
  SHK['SHIFT_CNT'] = SHK['SHIFT_CNT'].astype(float)
  
  SHK['ATB_DT'] = ''
  SHK['ATD_DT'] = ''
  SHK['OPEN_DT'] = ''
  SHK['TML_UPD_DT'] = ''
  SHK['CHANGE_DT'] = ''
  SHK['BERTHING_FLG'] = ''
  SHK['CLOSE_DT'] = ''
  SHK['WORK_START_DT'] = ''
  SHK['WORK_CMPL_DT'] = ''
  SHK['BERTHING_NO'] = ''
  
  
  SHK = SHK[['TML_CD', 'VSL_NAME', 'VVD_CD', 'VOY_NO', 'CRR_CD', 'SLAN_CD', 'BERTHING_FLG', 'ETB_DT', 'ETD_DT',
             'BERTHING_NO', 'CLOSE_DT', 'DISC_CNT', 'LOAD_CNT', 'SHIFT_CNT', 'WORK_START_DT', 'WORK_CMPL_DT',
                       'REG_USR_NO', 'REG_DT', 'UPD_USR_NO', 'UPD_DT', 'ATB_DT', 'ATD_DT', 'OPEN_DT', 'TML_UPD_DT']]
  
  for i in range(len(SHK)):
      if len(SHK['CRR_CD'][i]) > 3:
          SHK.loc[i, 'CRR_CD'] = ''
      if len(SHK['SLAN_CD'][i]) > 10:
          SHK.loc[i, 'SLAN_CD'] = ''
  
  
  total_list = list()
  for i in range(len(SHK)):
      total_list.append(SHK.values[i].tolist())

def DB_INSERT_DELETE(target_db_name):

  xconnection = DBEnv.xConnection(target_db_name)
  cursor = xconnection.cursor()

  insert_sql = """
      MERGE INTO TML_SCH A
           USING ( SELECT NVL(:1, 'N/A') as TML_CD, NVL(:2, 'N/A') as VSL_NAME,
                NVL(:3, 'N/A') as VVD_CD,
                NVL(:4, 'N/A') as VOY_NO, NVL(:5, 'N/A') as CRR_CD, NVL(:6, 'N/A') as SLAN_CD,
                NVL(:7, 'N/A') as BERTHING_FLG, TO_DATE(:8, 'YYYY-MM-DD HH24:MI:SS') as ETB_DT, TO_DATE(:9, 'YYYY-MM-DD HH24:MI:SS') as ETD_DT,
                NVL(:10, 'N/A') as BERTHING_NO, TO_DATE(:11, 'YYYY-MM-DD HH24:MI:SS') as CLOSE_DT, NVL(:12, 0) as DISC_CNT, NVL(:13, 0) as LOAD_CNT,
                NVL(:14, 0) as SHIFT_CNT, TO_DATE(:15, 'YYYY-MM-DD HH24:MI:SS') as WORK_START_DT, TO_DATE(:16, 'YYYY-MM-DD HH24:MI:SS') as WORK_CMPL_DT,
                NVL(:17, 'N/A') as REG_USR_NO, TO_DATE(:18, 'YYYY-MM-DD HH24:MI:SS') AS REG_DT, NVL(:19, 'N/A') as UPD_USR_NO,
                TO_DATE(:20, 'YYYY-MM-DD HH24:MI:SS') AS UPD_DT, TO_DATE(:21, 'YYYY-MM-DD HH24:MI:SS') as ATB_DT, TO_DATE(:22, 'YYYY-MM-DD HH24:MI:SS') as ATD_DT,
                TO_DATE(:23, 'YYYY-MM-DD HH24:MI:SS') as OPEN_DT, TO_DATE(:24, 'YYYY-MM-DD HH24:MI:SS') as TML_UPD_DT FROM DUAL) B

                ON (A.VSL_NAME = B.VSL_NAME AND
                    A.TML_CD = B.TML_CD AND
                    A.VVD_CD = B.VVD_CD AND
                    A.VOY_NO = B.VOY_NO)

           WHEN MATCHED THEN
                 UPDATE SET  A.ETB_DT = B.ETB_DT
                            ,A.ETD_DT = B.ETD_DT
                            ,A.CLOSE_DT = B.CLOSE_DT
                            ,A.UPD_DT = B.UPD_DT
                            ,A.ATB_DT = B.ATB_DT
                            ,A.ATD_DT = B.ATD_DT
                            ,A.OPEN_DT = B.OPEN_DT
                            ,A.TML_UPD_DT = B.TML_UPD_DT

           WHEN NOT MATCHED THEN
                INSERT (A.TML_CD, A.VSL_NAME, A.VVD_CD, A.VOY_NO, A.CRR_CD, A.SLAN_CD,
                    A.BERTHING_FLG, A.ETB_DT, A.ETD_DT, A.BERTHING_NO, A.CLOSE_DT,
                    A.DISC_CNT, A.LOAD_CNT, A.SHIFT_CNT, A.WORK_START_DT, A.WORK_CMPL_DT,
                    A.REG_USR_NO, A.REG_DT, A.UPD_USR_NO, A.UPD_DT, A.ATB_DT, A.ATD_DT, A.OPEN_DT, A.TML_UPD_DT)
                VALUES (B.TML_CD, B.VSL_NAME, B.VVD_CD, B.VOY_NO, B.CRR_CD, B.SLAN_CD,
                    B.BERTHING_FLG, B.ETB_DT, B.ETD_DT, B.BERTHING_NO, B.CLOSE_DT,
                    B.DISC_CNT, B.LOAD_CNT, B.SHIFT_CNT, B.WORK_START_DT, B.WORK_CMPL_DT,
                    B.REG_USR_NO, B.REG_DT, B.UPD_USR_NO, B.UPD_DT, B.ATB_DT, B.ATD_DT, B.OPEN_DT, B.TML_UPD_DT)
      """

  cursor.executemany(insert_sql, total_list, batcherrors = True, arraydmlrowcounts = True)
  rowCounts = cursor.getarraydmlrowcounts()
  xconnection.commit()

  delete_sql = """
        DELETE
        FROM TML_SCH
        WHERE ROWID IN (
        SELECT ROWID FROM(
        SELECT A.TML_CD, A.VSL_NAME, A.ETB_DT, A.ROWID, B.MAXROWID
        FROM TML_SCH A
        INNER JOIN
        (
        SELECT TML_CD, VSL_NAME, ETB_DT, MAX(ROWID) AS MAXROWID
        FROM TML_SCH
        GROUP BY TML_CD, VSL_NAME, ETB_DT
        HAVING COUNT (ETB_DT) > 1
        ORDER BY TML_CD, VSL_NAME, ETB_DT
        )B
        ON A.TML_CD = B.TML_CD AND A.VSL_NAME = B.VSL_NAME AND A.ETB_DT = B.ETB_DT
        ORDER BY A.TML_CD, A.VSL_NAME, A.ETB_DT)
        WHERE ROWID < MAXROWID)
      """

  cursor.execute(delete_sql)
  xconnection.commit()

  cursor.close()
  xconnection.close()

  tml_logger.info('----------- SHK Insert FINSH -----------')


if __name__ == '__main__':
  warnings.filterwarnings(action='ignore')

  tml_logger = logging.getLogger('TML_SCH')
  tml_logger.setLevel(logging.INFO)
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

  if tml_logger.hasHandlers():
      tml_logger.handlers.clear()

  file_handler = RotatingFileHandler('/home/ec2-user/fromJenkins/bin/log/SHK.log', maxBytes=1024 * 1024 * 5, backupCount=5)
  file_handler.setFormatter(formatter)
  tml_logger.addHandler(file_handler)

  tml_logger.info('----------- SHK START ----------')
  tml_logger.info(datetime.datetime.now(timezone('Asia/Seoul')).strftime('%Y-%m-%d %H:%M:%S'))

  target_db_name = '####'
  download_path = '/home/ec2-user/fromJenkins/bin/download/'

  try:
    Crawling(download_path, target_db_name)
    DB_INSERT_DELETE(target_db_name)
  except Exception as e:
    print(e)
    tml_logger.error(e)

  tml_logger.info('---------- SHK END ----------')
