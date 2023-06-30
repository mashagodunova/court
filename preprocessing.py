# Создание файла со скриптами текстов
import pandas as pd
import re
import os
import numpy as np
import json
import urllib2
data_sum = pd.read_json("case_summaries.json", orient=None, typ='frame', dtype=True,
                                 convert_axes=True, convert_dates=True, keep_default_dates=True)
pt = "./cases/"
for f in os.listdir(pt):
    if re.search("\.(?!json)", f):
        f_pt = os.path.join(pt, f)
        f_new = re.sub("\.(?!json)", "_", f)
        file_save_path = os.path.join(pt, f_new)
        os.rename(f_pt, file_save_path)
spis = []
counter = 0
c_d = {}
#Проходимся по папке репозитория скриптов с транскриптами каждого выступления
for i in os.listdir(pt):
    if counter < 10000:
        if re.search("t0", i):
            file_path = os.path.join(pt, i)
            with open(file_path) as f:
                name = "OA" + re.sub("\.json", "", i)
                name = re.sub("-", "_", name)
                spis.append((counter, i, name))
                c_d[name] = json.load(f)['transcript'] #записываем скрипты и названия в словарь


col = ['transcript_id', 'title', 'speaker', 'speaker_ID', 'speaker_role',
                 'speaker_appointing_pres', 'text_start', 'text_stop', 'text'] #создаем колонки для датафрема с текстами и информацией по ним и говорящим
df = pd.DataFrame(columns=col)
processed = []
for t_id in spis:
    t_id = t_id[2]
    if c_d[t_id]:
        t = c_d[t_id]
    for s in t['sections']:
        for i in range(len(s['turns'])):
            cases_c_d = {}
            speaker = s['turns'][i]['speaker']
            if speaker:
                cases_c_d['speaker_ID'] = speaker['ID']
                cases_c_d['speaker'] = speaker['name']
                if speaker['roles'] and speaker['roles'] is not None: #если спикер - судья Верховного суда, записываем, кем назначен
                    cases_c_d['speaker_role'] = speaker['roles'][0]['type']
                    cases_c_d['speaker_appointing_pres'] = speaker['roles'][0]['appointing_president']
                else:
                    cases_c_d['speaker_role'] = "not_a_justice"  # в ином случае записываем, что спикер - не судья, колонку с назначением заполняем как NA
                    cases_c_d['speaker_appointing_pres'] = "NA"
            else:
                pass
            cases_c_d['text_start'] = s['turns'][i]['text_blocks'][0]['start'] #записываем начало речи (время)
            cases_c_d['text'] = ""
            for text_block in s['turns'][i]['text_blocks']: #объединяем блоки с текстами для каждой речи
                cases_c_d['text'] += text_block['text'] + " "
            cases_c_d['text_stop'] = text_block['stop'] #записываем окончание речи
            cases_c_d['title'] = t['title'] #записываем название дела
            cases_c_d['transcript_id'] = re.sub("OA", "", t_id)
            df = df.append(pd.Series(cases_c_d), ignore_index=True)
            processed.append(t_id)
    df.to_csv('cases.csv', encoding='utf-8')
#Создание файла с метадатой по кейсам
pth = "./case_summaries/"
for row in data_sum['href']:
    link = str(row)
    response = urllib2.urlopen(link)
    retrieved_json = json.load(response)
    file_name = re.sub("https://api.oyez.org/cases/", "", link)
    file_name = re.sub("/","_", file_name)
    file_name = re.sub("-","_", file_name) + ".json"

    file_save_path = os.path.join(pth, file_name)
    with open(file_save_path, 'w') as f:
        json.dump(retrieved_json, f)

sum_cols = ['transcript_id', 'summary_id', 'term', 'case_name', 'lower_court', 'first_party',
                     'first_party_label', 'second_party', 'second_party_label', 'advocates', 'decision_type',
                     'winning_party', 'majority_vote', 'minority_vote', 'judge']
#создаем колонки и датафрейм с метаданными по кейсам
summaries_df = pd.DataFrame(columns=sum_cols)

summary_list = []

counter = 0
path = "./case_summaries/"

for n in os.listdir(path):
    if counter < 10000:


        file_path = os.path.join(path, n)
        with open(file_path) as f:
            c_d = {}

            summary_name = "OA" + re.sub("\.json", "", n)

            summary_list.append((counter, n))
            current_summary = json.load(f)
            if type(current_summary) == list:
                pass
            else: #если внутри файла лежит словарь, записываем в датафрейм всю информацию по делу
                c_d['transcript_id'] = summary_name
                c_d['summary_id'] = current_summary['ID']
                c_d['term'] = current_summary['term']
                c_d['case_name'] = current_summary['name']

                if current_summary['lower_court']: #если присутствует информация о суде низшей инстанции, записываем его имя
                    c_d['lower_court'] = current_summary['lower_court']['name']
                # записываем информацию по сторонам разбирательства
                c_d['first_party'] = current_summary['first_party']
                c_d['first_party_label'] = current_summary['first_party_label']
                c_d['second_party'] = current_summary['second_party']
                c_d['second_party_label'] = current_summary['second_party_label']

                if current_summary['advocates']: #создаем список адвокатов по каждому делу
                    c_d['advocates'] = []
                    for l in range(len(current_summary['advocates'])):
                        if current_summary['advocates'][l]['advocate']:
                            c_d['advocates'].append(
                                (current_summary['advocates'][l]['advocate']['name'],
                                 current_summary['advocates'][l]['advocate_description']))

                if current_summary['decisions']:
                    judge_dict = {} #записываем информацию по типу решения (большинство или только суд), выигравшей стороне, о количестве голосов за проигрывшую и выигравшую стороны
                    c_d['decision_type'] = current_summary['decisions'][0]['decision_type']
                    c_d['winning_party'] = current_summary['decisions'][0]['winning_party']
                    c_d['majority_vote'] = current_summary['decisions'][0]['majority_vote']
                    c_d['minority_vote'] = current_summary['decisions'][0]['minority_vote']

                    if current_summary['decisions'][0]['votes'] is not None: #если информация по кол-ву голосов есть, записываем ее в словарь с судьями
                        for v in range(len(current_summary['decisions'][0]['votes'])):
                            judge_name = current_summary['decisions'][0]['votes'][v]['member']['name']
                            judge_dict[judge_name] = {}

                            judge_dict[judge_name]['vote'] = current_summary['decisions'][0]['votes'][v]['vote']

                            judge_dict[judge_name]['seniority'] = current_summary['decisions'][0]['votes'][v][
                                'seniority']
                            judge_dict[judge_name]['ideology'] = current_summary['decisions'][0]['votes'][v]['ideology']

                        c_d['judge'] = judge_dict

            summaries_df = summaries_df.append(pd.Series(c_d), ignore_index=True)
summaries_df.to_csv('results.csv', encoding='utf-8')