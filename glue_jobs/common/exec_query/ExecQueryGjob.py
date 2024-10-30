import string
import glob
import traceback
import snowflake.connector

class ExecQuery:
    def __init__(self, conf, preprocess=None, schema=None, target=None, delete_where_clause=None, connection=None):
        self.conf = conf
        self.preprocess = preprocess
        self.schema = schema
        self.target = target
        self.delete_where_clause = delete_where_clause
        self.connection = connection

    def replace_param(self, query_txt, kargs):
        template_txt = string.Template(query_txt)
        query_txt = template_txt.safe_substitute(kargs)

        return query_txt

    def get_query(self, query_txt_path, kargs):
        with open(query_txt_path, mode='r', encoding='utf-8') as f:
            query_txt = f.read()
        if len(kargs) != 0:
            query_txt = self.replace_param(query_txt, kargs)
        return query_txt

    def get_connection(self, params):
        return snowflake.connector.connect(**params)

    def exec_query(self, query_txt):
        try:
            cur = self.connection.cursor()
            cur.execute(query_txt)
            print('exec suceess.')
        except Exception as e:
            raise Exception(e)

    def main(self, **kargs):
        try:
            root_key = 'exec_query'
            query_txt_path = self.conf[root_key]['query_path']
            keyword = self.conf[root_key]['keyword']
            query_txt = None

            if self.connection is None:
                params = self.conf[root_key]['connection_info']
                self.connection = self.get_connection(params)

            if self.preprocess == "delete":
                query_delete = """delete from %s.%s""" % (self.schema,self.target)
                if self.delete_where_clause is not None and self.delete_where_clause != "":
                    query_delete = query_delete + self.delete_where_clause
                query_delete = query_delete+";"
                print(query_delete)
                try:
                    self.exec_query(query_delete)
                except Exception as e:
                    self.connection.rollback()
                    raise Exception(e)

            sql_files = glob.glob(query_txt_path + '*')
            for sql_file in sql_files:
                if any(checkstr in sql_file for checkstr in keyword):
                    query_txt = self.get_query(sql_file, kargs)
                else:
                    continue
                if query_txt is not None:
                    print(query_txt)
                    try:
                        self.exec_query(query_txt)
                    except Exception as e:
                        self.connection.rollback()
                        raise Exception(e)
            self.connection.commit()
        except Exception as e:
            raise Exception(traceback.format_exc())
        finally:
            if self.connection is not None:
                self.connection.close()