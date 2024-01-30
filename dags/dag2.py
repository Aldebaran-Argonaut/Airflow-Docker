from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import requests
from bs4 import BeautifulSoup
import math
import re
from datetime import datetime, timedelta
import locale
from cryptography.fernet import Fernet
import json
import tempfile
import os
import psycopg2
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from airflow.models import Variable
import base64
import paramiko
from cryptography.fernet import InvalidToken
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
import string


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
}
pattern = re.compile(r'\d+')

session = requests.Session()
retry = Retry(connect=10, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)


# # Transform password into bytes

key_bytes = Variable.get('chave').encode('utf-8')
cipher_suite = Fernet(key_bytes)

# Cryptography functions and database inputs


def extract_page_number(soup):
    url = f"{soup}"
    pages = session.get(url, headers=headers)
    soup_content = BeautifulSoup(pages.content, 'html.parser')

    pagination = soup_content.find('ul', class_='pagination pagination-lg')
    span = pagination.find_all('span')
    print(span)
    itens_number_text = span[2].text
    transform_itens_number = itens_number_text.replace(".", "")

    try:
        list_of_integer_number = re.findall(pattern, transform_itens_number)
    except ValueError:
        print("Error to convert itens to integer.")
        return None
    number_of_itens = int(list_of_integer_number[2])
    result = number_of_itens / 25
    last_page = math.ceil(result)
    return last_page

# # Functions to encrypt data


def encryption(**kwargs):
    ti = kwargs['ti']
    source_task_result = kwargs.get('source_task_result')
    table = kwargs.get('table')
    print(source_task_result)
    print(key_bytes)

    encrypted_bytes = cipher_suite.encrypt(json.dumps(
        source_task_result, ensure_ascii=False).encode('utf-8'))
    ti.log.info(encrypted_bytes)

    encrypted_str = base64.b64encode(encrypted_bytes).decode('utf-8')
    # ti.xcom_push_binary(key='encrypted_data', value=encrypted_bytes)

    # SFTP Connection Settings
    host = Variable.get('host')
    username = Variable.get('username')
    port = 22
    password = Variable.get('password')

    # encrypted_bytes = base64.b64decode(encrypted_text).encode('utf-8')

    # Temp file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as temp_file:
        temp_filename = temp_file.name
        temp_file.write(encrypted_bytes)
        temp_file.seek(0)
        temp_file.close()

        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        remote_path_upload = f"C:\\Users\\Lenovo\\Desktop\\Projeto Data WH\\data output\\{table}.txt"
        sftp.put(temp_filename, remote_path_upload)

        os.remove(temp_filename)

    return encrypted_str


# Functions to decryption and loading

def decryption_and_loading(encrypted_text, table, master_key):
    # SFTP Connection Settings
    host = Variable.get('host')
    username = Variable.get('username')
    port = 22
    password = Variable.get('password')
   
    
    # Download a file from the SFTP server
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    remote_path_download = f"C:\\Users\\Lenovo\\Desktop\\Projeto Data WH\\data output\\{table}.txt"
    local_path_download = 'remote_file_path_to_download.txt'
    sftp.get(remote_path_download, local_path_download)
    sftp.close()
    transport.close()
    connection = None
    
    # Decrypt encrypted file data
    try:
        with open(local_path_download, 'rb') as encrypted_file:
            encrypted_date = encrypted_file.read()
            decrypt_text = cipher_suite.decrypt(encrypted_date)
            decrypt_date = decrypt_text.decode('utf-8')
            decrypt_date = decrypt_date.strip('"')
            decrypt_date_fixed = decrypt_date.replace("'", '"')
            decrypt_dictionary = json.loads(decrypt_date_fixed)
 
        keys = []
        values = []
        
        for dictionary in decrypt_dictionary:   
            for key, value in dictionary.items():
                keys.append(key)
                values.append(value)
    
        
        host = Variable.get('DBhost')
        database = Variable.get('DBdatabase')
        user = Variable.get('DBuser')
        password = Variable.get('DBpassword')
    
        db_config = {
            'host': host,
            'database': database,
            'user': user,
            'password': password}
            
        # Create a connection to the PostgreSQL database
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        # Query the database for existing keys 
        sql_query = f"SELECT {master_key} FROM {table};"
        
        # Execute the SQL query
        cursor.execute(sql_query)
        date_of_database = cursor.fetchall()
        
        # Run SQL query to get the unique keys that exist in the database
        desired_key = f'{master_key}'
        
        existing_keys = [value1 for tupla in date_of_database for value1 in tupla]
        existing_keys = set(existing_keys)
        

        new_keys = {dictionary[desired_key] for dictionary in decrypt_dictionary}
        new_keys = {int(number) for number in new_keys}
        print(new_keys)
        # Import only keys that do not already exist in the database
        keys_new = new_keys - existing_keys
        keys_new = list(keys_new)
        print(keys_new)
        
        data_to_insert = []

        for dictionary in decrypt_dictionary:
            # Checks if the desired key is present in the dictionary
            if int(dictionary[desired_key]) in keys_new:
                # Add the complete dictionary to the list
                data_to_insert.append(dictionary)
        print(data_to_insert)
                
                
        try:
            for dictionary_insert in data_to_insert:
                sql_query = f"INSERT INTO {table} ({', '.join(dictionary_insert.keys())}) VALUES ({', '.join(['%s' for _ in dictionary_insert.values()])})"
                
                tupla_values = tuple(dictionary_insert.values())

                # Execute the SQL query
                cursor.execute(sql_query,tupla_values)

                # Commit to confirm the transaction
                connection.commit()
                print("Successful insertion.") 
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
                
    except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
        
    finally:
        # Close the database connection
        if connection:
            cursor.close()
            connection.close()
    
    return 


def extract_players_informations_cbf(**kwargs):
    players_informations = []
    ti = kwargs['ti']
    last_page = ti.xcom_pull(task_ids='task_extract_page_number1')
    pages = last_page

    for number_page in range(1, pages + 1):
        url = f"https://www.cbf.com.br/futebol-brasileiro/atletas/campeonato-brasileiro-serie-a/2023?atleta=&page={number_page}&csrt=5141281007868528954"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
        }

        try:
            site = session.get(url, headers=headers)
            soup = BeautifulSoup(site.content, "html.parser")
            body = soup.find('tbody')
            lines = body.find_all('tr')

            for line in lines:
                columns = line.find_all('a')
                name_columns = columns[0]
                link = name_columns['href']
                id_player = re.findall(pattern, link)
                img = line.find('img')
                title = "title nor found" if img is None else img['alt']

                players_informations.append({
                    'id_players': id_player[0],
                    'name': columns[0].text.strip(),
                    'nickname': columns[1].text.strip(),
                    'team': title,
                    'year': id_player[1]
                })

        except Exception as e:
            print(f"Error on page {number_page}: {str(e)}")

    return players_informations

def extract_teams_informations_cbf():
    teams_informations = []
    try:
        url_team = "https://www.cbf.com.br/futebol-brasileiro/times/campeonato-brasileiro-serie-a/2023"
        site = session.get(url_team, headers=headers)
        soup_times = BeautifulSoup(site.content, "html.parser")
        row = soup_times.find_all('div', class_='col-md-3 p-10')

        for time in row:
            a = time.find('a')
            link = a['href']
            img = a.find('img')
            name = img['alt']
            team_reference = re.findall(pattern, link)

            if name != 'Esporte Clube Bahia - BA' and name != 'Atlético Mineiro - MG':
                teams_informations.append({
                    'id_team': team_reference[1],
                    'name': name,
                    'year': team_reference[0]
                })
    except Exception as e:
        print(f"Error on page: {str(e)}")
    return teams_informations

def extract_referees_informations_cbf(**kwargs):
    referees_informations = []
    ti = kwargs['ti']
    last_page = ti.xcom_pull(task_ids='task_extract_page_number2')
    pages = last_page

    for number_page in range(1, pages + 1):
        url = f"https://www.cbf.com.br/a-cbf/arbitragem/relacao-arbitros?p=&l=&f=0&c=0&j=0&page={number_page}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
        }

        try:
            site = session.get(url, headers=headers)
            soup = BeautifulSoup(site.content, "html.parser")
            body = soup.find('tbody')
            lines = body.find_all('tr')

            for line in lines:
                columns = line.find_all('td')
                columns_name = columns[0]
                a = columns_name.find('a')
                link = a['href']
                id_referees = re.findall(pattern, link)
                name = columns[0].find('b')
                function = columns[1]
                federation = columns[2]

                referees_informations.append({
                    'id_referees': id_referees[0],
                    'name': name.text.strip(),
                    'function': function.text.strip(),
                    'federation': federation.text.strip()
                })

        except Exception as e:
            print(f"Error on page {number_page}: {str(e)}")

    return referees_informations


def extract_referees_statistics_cbf():
    referees_statistics = []
    id_referees_statistics = 0
    for number_page in range(1, 380 + 1):
        url = f"https://www.cbf.com.br/futebol-brasileiro/competicoes/campeonato-brasileiro-serie-a/2023/{number_page}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
        }
        try:
            site = session.get(url, headers=headers)
            soup = BeautifulSoup(site.content, "html.parser")
            body = soup.find('tbody')
            lines = body.find_all('tr')

            for line in lines:
                a = line.find('a')
                link = a['href']
                id_referees = re.findall(pattern, link)
                id_referees = id_referees[0]
                functions = line.find('th')
                columns = line.find_all('td')
                name = columns[0]
                federation = columns[1]
                id_referees_statistics += 1

                referees_statistics.append({
                    'id_referees_statistics': id_referees_statistics,
                    'id_match': number_page,
                    'id_referees': id_referees,
                    'functions': functions.text.strip(),
                    'name': name.text.strip(),
                    'federation': federation.text.strip()
                })
                print(referees_statistics)
        except Exception as e:
            print(f"Error on page {number_page}: {str(e)}")

    return referees_statistics


def extract_players_statistics_cbf():
    players_statistics = []
    id_players_statistics = 0
    list_player = []
    for number_page in range(1, 380+1):
        url = f"https://www.cbf.com.br/futebol-brasileiro/competicoes/campeonato-brasileiro-serie-a/2023/{number_page}#escalacao"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
        }
        try:
            site = requests.get(url, headers=headers)
            soup = BeautifulSoup(site.content, "html.parser")
            lineup = soup.find(
                'div', class_='col-xs-12 col-sm-8 col-md-8 col-lg-8')
            columns = lineup.find_all(
                'ul', class_=re.compile(r'\blist list-unstyled\b'))

            for index_column, column in enumerate(columns):
                name = []
                lista_name = soup.find_all('div', class_='col-sm-2 col-md-2 col-lg-2 hidden-xs nopadding text-center')
                for el in lista_name:
                    name.append(el.text.strip())     
                if index_column % 2 == 0 or index_column == 0:
                    name = name[0] 
                else:
                    name = name[1] 
                if column.find("li", string="Lineup not released"):
                    continue
                line_li = column.find_all('li')
                if line_li:
                    for li in line_li:   
                        players = li.find('a')
                        if players:
                            players_id = str(players)
                            six_digit_numbers = int(re.findall(r'\d{6}', players_id)[0])
                            players = players.text.strip() 
                            # Checks if players is None and, if so, assigns the string "No Name"
                        if players is None:
                            continue
                        role = 'Starting' if index_column < 2 else 'Reserve'
                        icon = li.find_all('i')
                        goal = 0
                        own_goal = 0
                        yellow_card = 0
                        red_card = 0
                        time = []
                        minutes = []
                        id_players_statistics += 1
                        for i in icon:
                            classes_i = i.get('class')

                            if 'small' in classes_i and 'icon' in classes_i and 'icon-yellow-card' not in classes_i:
                                goal += 1
                                if 'title' in i.attrs:
                                    time_of_match = i['title']
                                    list_time_of_match = re.findall(
                                        pattern, time_of_match)

                                    if list_time_of_match:
                                        if len(list_time_of_match) == 3:
                                            time_val = int(
                                                list_time_of_match[2])
                                            minutes_val = int(
                                                list_time_of_match[0]) + int(list_time_of_match[1])

                                        else:
                                            time_val = int(
                                                list_time_of_match[1])
                                            minutes_val = int(
                                                list_time_of_match[0])
                                        time.append(time_val)
                                        minutes.append(minutes_val)
                            if 'small' in classes_i and 'icon' in classes_i and 'icon-yellow-card' in classes_i:
                                yellow_card += 1
                            if 'small' in classes_i and 'icon' in classes_i and 'icon-red-card' in classes_i:
                                red_card += 1
                        div_list_phrese = soup.find(
                            'div', class_="col-xs-3 col-sm-3 text-left hidden-xs")
                        list_phrase = div_list_phrese.find_all(
                            'p', class_='time-jogador color-red')
                        if list_phrase:
                            for list_ in list_phrase:
                                list_player_name_match = re.match(
                                    r'\w+', list_.text.strip())
                                if list_player_name_match:
                                    list_player_name = list_player_name_match.group()
                                    list_player_name_split = list_player_name.split()
                                    #here I already have the list of people with an own goal
                                    for player_name in list_player_name_split:
                                        list_player.append(players)
                                        for players1 in list_player:
                                            if players1 == player_name:
                                                own_goal = 1
                                            else:
                                                own_goal = 0
                                else:
                                    list_player_name = None
                        goal -= own_goal
                        players_statistics.append({
                            'id_players_statistic': id_players_statistics,
                            'id_match': number_page,
                            'id_players': six_digit_numbers,
                            'name_teams': name,
                            'name_players': players,
                            'role': role,
                            'goal': goal,
                            'own_goal': own_goal,
                            'time': time,
                            'minutes': minutes,
                            'yellow_card': yellow_card,
                            'red_card': red_card
                        })
                else:
                    print(
                        "No players were found in the column. Continuing in the next column....")
        except Exception as e:
            print(f"Error on page {number_page}: {str(e)}")
    return players_statistics


def extract_match_results_cbf():
    match_results = []
    id_match_results = 0
    print(f"Locale no Apache Airflow: {locale.getlocale()}")
    for number_page in range(1, 380+1):
        url = f"https://www.cbf.com.br/futebol-brasileiro/competicoes/campeonato-brasileiro-serie-a/2023/{number_page}#escalacao"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
        }
        try:
            site = session.get(url, headers=headers)
            soup = BeautifulSoup(site.content, "html.parser")
            match_data = soup.find('div', class_='col-sm-8')
            match_data_elements = match_data.find_all(
                'span', class_="text-2 p-r-20")
            scoreboard = soup.find('div', class_='placar-wrapper')
            strong = scoreboard.find_all('strong', class_='time-gols block')

            # List to store the goals of both teams
            teams_goals = [int(team.text.strip()) for team in strong]

            for i, teams in enumerate(strong):
                team = i+1
                if i == 0:
                    id_match_results = (number_page*2)-1
                elif i == 1:
                    id_match_results = (number_page*2)
                # Nome
                name = []
                scoreboard_elements = scoreboard.find_all(
                    'h3', class_='time-nome color-white')
                
                for el in scoreboard_elements:
                    name.append(el.text.strip())
            

                score = 0

                # Logic to award points based on the number of goals
                if teams_goals[0] > teams_goals[1]:
                    score = 3 if i == 0 else 0
                elif teams_goals[0] < teams_goals[1]:
                    score = 3 if i == 1 else 0
                else:
                    score = 1

                for index_elements, elements in enumerate(match_data_elements):
                    if index_elements == 0:
                        local = elements.text.strip()
                    elif index_elements == 1:
                        date_complete = elements.text.strip()
                        match = re.search(r',(.*)', date_complete)
                        date_original = match.group(1).strip()

                match_results.append({
                    'id_match_result': id_match_results,
                    'id_match': number_page,
                    'local': local,
                    'date': date_original,
                    'team': team,
                    'name': name[i],
                    'goal': teams_goals[i],
                    'score': score
                })
        except Exception as e:
            print(f"Error on page {number_page}: {str(e)}")
    return match_results


########################################################################################
# coloquei concurrency = 1 por causas da minha maquina nao suportar o processamento em paralelo das task, porem se sua maquina tiver recurso suficinete, acredito que voce possa retirar esse limitador.
with DAG('dag2', start_date=datetime(2024, 1, 1), schedule_interval=timedelta(days=30), catchup=False, concurrency=1) as dag:

    # task_extract_page_number1 = PythonOperator(task_id='task_extract_page_number1',
    #                                            python_callable=extract_page_number,
    #                                            provide_context=True,
    #                                            op_args=[
    #                                                'https://www.cbf.com.br/futebol-brasileiro/atletas/campeonato-brasileiro-serie-a/2023?atleta=&page=1&csrt=3199419627270262597'],
    #                                            )

    # task_extract_players_informations_cbf = PythonOperator(task_id='task_extract_players_informations_cbf',
    #                                                        python_callable=extract_players_informations_cbf,
    #                                                        depends_on_past=True,
    #                                                        wait_for_downstream=True,
    #                                                        provide_context=True,
    #                                                        )

    # task_extract_teams_informations_cbf = PythonOperator(task_id='task_extract_teams_informations_cbf',
    #                                                      python_callable=extract_teams_informations_cbf,
    #                                                      provide_context=True,
    #                                                      )
    # task_extract_page_number2 = PythonOperator(task_id='task_extract_page_number2',
    #                                            python_callable=extract_page_number,
    #                                            provide_context=True,
    #                                            op_args=[
    #                                                'https://www.cbf.com.br/a-cbf/arbitragem/relacao-arbitros'],
    #                                            )

    # task_extract_referees_informations_cbf = PythonOperator(task_id='task_extract_referees_informations_cbf',
    #                                                         python_callable=extract_referees_informations_cbf,
    #                                                         depends_on_past=True,
    #                                                         wait_for_downstream=True,
    #                                                         provide_context=True,
    #                                                         )

    # task_extract_referees_statistics_cbf = PythonOperator(task_id='task_extract_referees_statistics_cbf',
    #                                                       python_callable=extract_referees_statistics_cbf,
    #                                                       provide_context=True,
    #                                                       )

    # task_extract_players_statistics_cbf = PythonOperator(task_id='task_extract_players_statistics_cbf',
    #                                                      python_callable=extract_players_statistics_cbf,
    #                                                      provide_context=True,
    #                                                      )

    # task_extract_match_results_cbf = PythonOperator(task_id='task_extract_match_results_cbf',
    #                                                 python_callable=extract_match_results_cbf,
    #                                                 provide_context=True,
    #                                                 )

    # task_encryption1 = PythonOperator(
    #     task_id='task_encryption1',
    #     python_callable=encryption,
    #     provide_context=True,
    #     depends_on_past=True,
    #     wait_for_downstream=True,
    #     op_kwargs={
    #         'source_task_result': '{{ task_instance.xcom_pull(task_ids="task_extract_players_informations_cbf") }}',
    #         'table': 'relational.players_informations', },
    # )

    # task_decryption1 = PythonOperator(
    #     task_id='task_decryption1',
    #     python_callable=decryption_and_loading,
    #     provide_context=True,
    #     depends_on_past=True,
    #     wait_for_downstream=True,
    #     op_args=['{{ ti.xcom_pull(task_ids="task_encryption1")}}',
    #              'relational.players_informations', 'id_players'],
    # )

    # task_encryption2 = PythonOperator(
    #     task_id='task_encryption2',
    #     python_callable=encryption,
    #     provide_context=True,
    #     depends_on_past=True,
    #     wait_for_downstream=True,
    #     op_kwargs={
    #         'source_task_result': '{{ task_instance.xcom_pull(task_ids="task_extract_teams_informations_cbf") }}',
    #         'table': 'relational.teams_informations', },
    # )

    # task_decryption2 = PythonOperator(
    #     task_id='task_decryption2',
    #     python_callable=decryption_and_loading,
    #     provide_context=True,
    #     depends_on_past=True,
    #     wait_for_downstream=True,
    #     op_args=['{{ ti.xcom_pull(task_ids="task_encryption2")}}',
    #              'relational.teams_informations', 'id_team'],
    # )

    # task_encryption3 = PythonOperator(
    #     task_id='task_encryption3',
    #     python_callable=encryption,
    #     provide_context=True,
    #     depends_on_past=True,
    #     wait_for_downstream=True,
    #     op_kwargs={
    #         'source_task_result': '{{ task_instance.xcom_pull(task_ids="task_extract_referees_informations_cbf") }}',
    #         'table': 'relational.referees_informations', },
    # )

    # task_decryption3 = PythonOperator(
    #     task_id='task_decryption3',
    #     python_callable=decryption_and_loading,
    #     provide_context=True,
    #     depends_on_past=True,
    #     wait_for_downstream=True,
    #     op_args=['{{ ti.xcom_pull(task_ids="task_encryption3")}}',
    #              'relational.referees_informations', 'id_referees'],
    # )
    # task_encryption4 = PythonOperator(
    #     task_id='task_encryption4',
    #     python_callable=encryption,
    #     provide_context=True,
    #     depends_on_past=True,
    #     wait_for_downstream=True,
    #     op_kwargs={
    #         'source_task_result': '{{ task_instance.xcom_pull(task_ids="task_extract_referees_statistics_cbf") }}',
    #         'table': 'relational.referees_statistics', },
    # )

    # task_decryption4 = PythonOperator(
    #     task_id='task_decryption4',
    #     python_callable=decryption_and_loading,
    #     provide_context=True,
    #     depends_on_past=True,
    #     wait_for_downstream=True,
    #     op_args=['{{ ti.xcom_pull(task_ids="task_encryption4")}}',
    #              'relational.referees_statistics', 'id_referees_statistics'],
    # )
    task_encryption5 = PythonOperator(
        task_id='task_encryption5',
        python_callable=encryption,
        provide_context=True,
        depends_on_past=True,
        wait_for_downstream=True,
        op_kwargs={
            'source_task_result': '{{ task_instance.xcom_pull(task_ids="task_extract_players_statistics_cbf") }}',
            'table': 'relational.players_statistics', },
    )

    task_decryption5 = PythonOperator(
        task_id='task_decryption5',
        python_callable=decryption_and_loading,
        provide_context=True,
        depends_on_past=True,
        wait_for_downstream=True,
        op_args=['{{ ti.xcom_pull(task_ids="task_encryption5")}}',
                 'relational.players_statistics', 'id_players_statistic'],
    )
    # task_encryption6 = PythonOperator(
    #     task_id='task_encryption6',
    #     python_callable=encryption,
    #     provide_context=True,
    #     depends_on_past=True,
    #     wait_for_downstream=True,
    #     op_kwargs={
    #         'source_task_result': '{{ task_instance.xcom_pull(task_ids="task_extract_match_results_cbf") }}',
    #         'table': 'relational.match_results', },
    # )

    # task_decryption6 = PythonOperator(
    #     task_id='task_decryption6',
    #     python_callable=decryption_and_loading,
    #     provide_context=True,
    #     depends_on_past=True,
    #     wait_for_downstream=True,
    #     op_args=['{{ ti.xcom_pull(task_ids="task_encryption6")}}',
    #              'relational.match_results', 'id_match_result'],
    # )

    # task_extract_page_number1 >> task_extract_players_informations_cbf >> task_encryption1 >> task_decryption1
    # task_extract_teams_informations_cbf >> task_encryption2 >> task_decryption2
    # task_extract_page_number2 >> task_extract_referees_informations_cbf >> task_encryption3 >> task_decryption3
    # task_extract_referees_statistics_cbf >> task_encryption4 >> task_decryption4
    # task_extract_players_statistics_cbf
    task_encryption5 >> task_decryption5
    # task_extract_match_results_cbf >> task_encryption6 >> task_decryption6
