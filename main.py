#!/usr/bin/python

import pandas
import jaydebeapi
import os

def sql_script(path_script, date_string): # Функция запуска sql-скрипта
	f = open(path_script,'r')
	s = f.read()
	sql = s.replace('[date]', date_string)
	sql_coms = sql.replace('\n', ' ').split(';')[:-1]
	for sql_com in sql_coms:
	    curs.execute(sql_com)
	f.close()

def backup(path, dir, end): # Функция бэкапа в папку dir с добавлению к имени файла окочания end
    os.rename(path, path[0: path_term.rfind(r'/') + 1] + dir + r'/' + path[ path.rfind(r'/') + 1:-1] + path_term[-1]  + end)

# Коннектимся с сервером
print('=== ETL start ===')
print('Connecting...')
conn = jaydebeapi.connect( 
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:de1m/samwisegamgee@de-oracle.chronosavant.ru:1521/deoracle',
['de1m','samwisegamgee'],
'/home/de1m/ojdbc8.jar'
)
curs = conn.cursor()
conn.jconn.setAutoCommit(False)
print('Connected')

# Очистка STG (скрипт truncate.sql)
print('Truncating STG-tables...')
sql_script('/home/de1m/krlv/sql_scripts/truncate.sql','')
print('Truncated')

# Получение путей к файлам и даты из имени файла
print('Reading files...')
lstdir = os.listdir(r"/home/de1m/krlv")
for i in lstdir:
	if i.find('passport_blacklist_') > -1: 
		path_blk = i
	elif i.find('terminals_') > -1: path_term = i
	elif i.find('transactions_') > -1: path_trans = i
date_trans = path_trans[-12:-4]
date_blk = path_blk[-13:-5]
date_term = path_term[-13:-5]


# Считываем датафреймы из файлов
df_blk = pandas.read_excel( path_blk, sheet_name='blacklist', header=0, index_col=None )
df_term = pandas.read_excel( path_term, sheet_name='terminals', header=0, index_col=None )
df_trans = pandas.read_csv( path_trans, sep = ';' )

# Приводим все в строки и меняем десятичный разделитель с запятой на точку
df_blk = df_blk.astype(str)
df_term = df_term.astype(str)
df_trans = df_trans.astype(str)
for i in range(0,len(df_trans['amount'])):
	df_trans['amount'][i]= df_trans['amount'][i].replace(',','.')
print('Readed')

# 1.ЗАПОЛНЕНИЕ STG
# Вставляем датафреймы в таблицы
print('Loading files to STG...')
curs.executemany( "insert into de1m.krlv_stg_blcklst( entry_dt, passport_num ) values ( to_date( ?, 'YYYY-MM-DD HH24:MI:SS' ), ? )", df_blk.values.tolist() )
curs.executemany( "insert into de1m.krlv_stg_terminals( terminal_id, terminal_type, terminal_city, terminal_address ) values ( ?, ?, ?, ? )", df_term.values.tolist() )
curs.executemany( "insert into de1m.krlv_stg_transactions( trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal  ) values ( ?, to_date( ?, 'YYYY-MM-DD HH24:MI:SS' ), cast( ? as decimal(12,2)), ?, ?, ?, ? )", df_trans.values.tolist() )
print('Loaded')

# Заполняем остальные STG (скрипт extract.sql)
print('Extract changes to STG...')
sql_script('/home/de1m/krlv/sql_scripts/extract.sql','')
print('Extracted')

# 2. ВЫДЕЛЕНИЕ ВСТАВОК И ИЗМЕНЕНИЙ (скрипт transform_load.sql)
print('Loading changes to DWH...')
sql_script('/home/de1m/krlv/sql_scripts/transform_load.sql', date_term)


# 3.ОБРАБОТКА УДАЛЕНИЙ (скрипт deleted.sql)
sql_script('/home/de1m/krlv/sql_scripts/deleted.sql', date_term)
print('Loaded')

# 4. ОБНОВЛЕНИЕ МЕТАДАННЫХ (скрипт meta.sql)
print('Updating metadata...')
sql_script('/home/de1m/krlv/sql_scripts/meta.sql', date_blk)
print('Updated')

# Фиксируем изменения
print('Commit')
conn.commit()

 # 2. ПОСТРОЕНИЕ ОТЧЕТА НА ДАТУ ЗАГРУЖЕННОГО ФАЙЛА ТРАНЗАКЦИЙ
print('Report creating...')
sql_script('/home/de1m/krlv/sql_scripts/report.sql', date_trans)
print('Created')

# Фиксируем изменения в таблице отчетов
print('Commit')
conn.commit()

# Закрываем коннект
curs.close()
conn.close()
print('Connect closed')

# Отправляем использованные файлы в архив 
backup(path_term, 'arhive', '.backup')
backup(path_trans, 'arhive', '.backup')
backup(path_blk, 'arhive', '.backup')
print('Backup done')
print('=== ETL complete ====')