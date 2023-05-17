import requests
import pandas as pd
import json
import re
import pymysql, time
from applications.common.utils import log
from sqlalchemy import create_engine
from applications.common.utils import tool
from applications.common.utils.mysqldb import MysqlDB

from urllib import parse


class OnlineInfo(object):

    def __init__(self):
        self.sql_config = {
            "ip": '192.168.1.77',
            "port": 3306,
            "user": "root",
            "passwd": "",
            "db": ""
        }
        HOSTS_URI = f""

        data_excel_path = "D://123.xlsx"

        self.db = MysqlDB(**self.sql_config)

    @staticmethod
    def type_to_example(type):
        if type == 'boolean':
            return 1
        if type == 'number':
            return 0
        if type == 'integer':
            return 0
        if type == 'string':
            return ''

        return type

    def fix_example(self, x, req_res_dict, deep=0):
        deep += 1
        if deep > 4:
            return {}
        if 'type' not in x:
            if "schema" in x:
                return self.fix_example(x['schema'], req_res_dict, deep)
            elif '$ref' in x:
                target = x['$ref'].split('/')[-1]
                return self.fix_properties(target, req_res_dict, deep)
            else:
                return ''

        type = x['type']

        if type == 'array':
            if 'items' not in x:
                return []
            if '$ref' not in x['items']:
                return [self.type_to_example(x['items']['type'])]
            target = x['items']['$ref'].split('/')[-1]
            return self.fix_properties(target, req_res_dict, deep)
        if type == 'object':
            if 'items' not in x:
                return {}
            if '$ref' not in x['items']:
                return {}
            target = x['items']['$ref'].split('/')[-1]
            return self.fix_properties(target, req_res_dict, deep)

        return self.type_to_example(type)

    def fix_properties(self, req, req_res_dict, deep=0):
        if deep > 4:
            return {}
        if req not in req_res_dict:
            return {}
        if "properties" not in req_res_dict[req]:
            return {}
        deep += 1
        req_dict = req_res_dict[req]['properties']
        return dict(zip([x for x in req_dict.keys()],
                        [x.get('example', self.fix_example(x, req_res_dict, deep)) for x in
                         req_dict.values()]))

    def get_swagger_datas(self):
        db = self.db
        passowrd = parse.quote_plus("1")
        swagger_urls = ["ai-action-statistic"]
        for j in range(30):
            time.sleep(10)
            new_swagger_urls = set()
            for i, url in enumerate(swagger_urls):
                # url = "ai-reward-points"
                if url in ["loginservice"]:
                    continue
                headers = {}
                doc_url = f"swagger"
                res = requests.get(doc_url, headers=headers)
                if not res.ok:
                    continue

                res.encoding = 'utf8'
                new_doc_url = f'http'
                res2 = requests.get(new_doc_url, headers=headers)
                if not res2.ok:
                    new_doc_url = ''
                path = ''
                out_list = []
                try:
                    data = res.json()
                except:
                    data = re.sub('true', 'True', res.text)
                    data = re.sub('false', 'False', data)
                    try:
                        data = eval(data)
                    except Exception as e:
                        log.err(e)
                        print(f"{i} faild {url} ")
                        continue
                try:
                    if "paths" not in data:
                        continue
                    path_datas = data['paths']
                    req_res_dict = data.get('definitions', {})
                    skip = 0
                    for res_key in req_res_dict:
                        if '�' in res_key:
                            new_swagger_urls.add(url)
                            skip = 1
                            break
                    if skip:
                        continue
                    u_numb, i_numb, skip_numb = 0, 0, 0
                    for path, path_data in path_datas.items():
                        # path_info = path_data['post'] if "post" in path_data else path_data['get']

                        method = next(path_data.__iter__())
                        path_info = path_data[method]
                        if req_res_dict:
                            try:
                                if "parameters" in path_info:
                                    for parameter in path_info['parameters']:
                                        schema = parameter.get('schema')
                                        if schema:
                                            if "$ref" in schema:
                                                req = schema['$ref'].split('/')[-1]
                                                if req not in req_res_dict:
                                                    path_info['请求样例'] = req
                                                    path_info['请求参数'] = req
                                                else:
                                                    req_data = req_res_dict[req]
                                                    path_info['请求样例'] = self.fix_properties(req, req_res_dict)
                                                    path_info['请求参数'] = req_data
                                                break
                                            else:
                                                path_info['请求样例'] = schema
                                                path_info['请求参数'] = schema

                                    else:
                                        path_info['请求样例'] = dict(zip([x['name'] for x in path_info['parameters']],
                                                                     [self.fix_example(x, req_res_dict) for x in
                                                                      path_info['parameters']]))
                                        path_info['请求参数'] = path_info['parameters']

                                    if "schema" not in path_info['responses']['200'] or '$ref' not in \
                                            path_info['responses']['200']['schema']:
                                        path_info['响应样例'] = {}
                                        path_info['响应参数'] = {}
                                    else:
                                        res_demo = path_info['responses']['200']['schema']['$ref'].split('/')[-1]
                                        if res_demo not in req_res_dict:
                                            path_info['响应样例'] = res_demo
                                            path_info['响应参数'] = res_demo
                                        else:
                                            res_data = req_res_dict[res_demo]
                                            path_info['响应样例'] = self.fix_properties(res_demo, req_res_dict)
                                            path_info['响应参数'] = res_data
                            except Exception as e:
                                log.err(e)

                        swagger_url = f'http'
                        json_data = json.dumps(path_info, ensure_ascii=False)
                        save_dict = {
                            'description': data['info']['description'] if "description" in data['info'] else None,
                            'title': data['info']['title'] if "title" in data['info'] else None,
                            'host': data['host'],
                            'tags': path_info['tags'][0],
                            'summary': path_info['summary'],
                            'uri': path,
                            'url': swagger_url,
                            "method": method,
                            "service": url,
                            'json_data': json_data
                        }
                        out_list.append(save_dict)

                        online_data = db.find(f"select json_data from swagger where service='{url}' and uri='{path}'",
                                              to_json=1)
                        if online_data:
                            continue
                        if '�' in json_data:
                            # new_swagger_urls.add(url)
                            save_dict['skip'] = 1

                        if not online_data:
                            i_sql = tool.make_insert_sql("swagger", save_dict)
                            db.add(i_sql)
                            i_numb += 1
                            continue
                        online_json_data = json.loads(online_data[0])
                        # 未解决各种各样的乱码问题，写出详细逻辑
                        for data_key, data_value in online_json_data.items():
                            path_data = path_info[data_key]
                            if re.findall(r'�', str(path_data)):
                                continue
                            if path_data != data_value:
                                u_numb += 1
                                u_sql = tool.make_update_sql("swagger", save_dict,
                                                             condition=f"service='{url}' and uri='{path}'")
                                db.update(u_sql)
                                break
                        else:
                            skip_numb += 1

                    print(i, j, len(swagger_urls), url, len(path_datas), i_numb, skip_numb, u_numb)
                except Exception as e:
                    log.err(e)
                    print(f"{i} faild {url}, {doc_url} , [{path}]")

                # datas = pd.DataFrame(out_list)
                #
                # datas.to_sql("swagger", engine, schema="test_platform_new", if_exists='append', index=True,
                #              chunksize=None, dtype=None)
            swagger_urls = new_swagger_urls

    def get_mysql_tables(self):
        db = self.db
        table_names = db.find("show databases;", to_json=1)

        passowrd = parse.quote_plus("Xingrui@DCDB123")
        engine = create_engine(
            f"mysql+pymysql://root:{passowrd}@192.168.1.77:3306/test_platform_new?charset=utf8")

        for table in table_names:
            db_name = table['Database']
            if db_name in ['information_schema']:
                continue
            q_sql = f"""
                            SELECT
                                table_name,
                                table_rows
                            FROM
                                information_schema.tables
                            WHERE
                                table_schema = '{db_name}'
                            ORDER BY table_rows desc;"""
            online_data = db.find(q_sql, to_json=1)
            db.find(f"use {db_name}")
            for table in online_data:
                table_name = table['table_name']
                table_rows = table['table_rows']
                if table_rows != 0 and not table_rows:
                    continue
                try:
                    table_info = db.find(f"show create table `{table_name}`", to_json=1)
                except:
                    continue
                if not table_info:
                    continue
                for i, data in enumerate(table_info):
                    if 'Create Table' not in data:
                        continue

                    create_info = data['Create Table']
                    comment_list = []
                    for comment_line in create_info.split('\n'):
                        comment_now = re.findall(r'`(.*?)`.*COMMENT\s*\'(.*?)\',', comment_line)
                        if not comment_now:
                            comment_now = re.findall(r'^\s+`(.*?)`', comment_line)
                            if not comment_now:
                                continue
                            comment_now = (comment_now[0], '')
                        else:
                            comment_now = comment_now[0]
                        comment_list.append(comment_now)
                    table_comment = re.findall(r'COMMENT\=\'(.*?)\'$', create_info)
                    table_comment = table_comment[0] if table_comment else ""
                    table_info[i]['table_comment'] = table_comment
                    table_info[i]['fields'] = ";".join([x[0] for x in comment_list])
                    table_info[i]['comments'] = ";".join([x[1] for x in comment_list])
                    table_info[i]['table_rows'] = table_rows
                datas = pd.DataFrame(table_info)
                datas['db'] = db_name
                datas.to_sql("dev_mysql_tables", engine, schema="test_platform_new", if_exists='append', index=True,
                             chunksize=None, dtype=None)

            time.sleep(0.5)


if __name__ == '__main__':
    oi = OnlineInfo()
    oi.get_mysql_tables()
    # get_mysql_tables()
    # a = re.findall(r'(((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})\.){3}','123.123.123.123')
    # b = '25[0-5]|2[0-4]\d|[0-1]?\d{1,2}'
    # fix_b = "\.".join([f'({b})']*4)
    # # a = re.search(r'%s' % fix_b, '123.124.223.25')
    # a = re.findall(r'%s' % fix_b, 'recivate 123.124.223.25 to 123.124.223.23')
    # # a = re.findall(r'', '123.124.223.25')
    # print(a)
