from calendar import month
import json
import glob
import traceback
import string

class LoadConfig:
    def __init__(self, conffile_path, conffile_name) -> None:
        self.conffile_path = conffile_path
        self.conffile_name = conffile_name
    
    def load_config(self, **kargs):
        config = ''
        try:        
            # [Referenced files path]に設定された設定ファイルをGlue実行環境で検索する
            files = glob.glob(self.conffile_path)
            for file in files:
                # Glue環境に配置されている設定ファイルの名前を取得
                filename = file.split('/')[-1]
                # Glue環境に配置されている設定ファイルの絶対パスを取得
                if self.conffile_name == filename:
                    config = file

            # 設定ファイルの絶対パスが取得できていない場合(=設定ファイルが存在しない)エラー
            if config is None or config == '':
                raise Exception('not found config file.')

            # 設定ファイルを読み込む
            with open(config, mode='r', encoding='utf-8') as f:
                json_txt = f.read()

            if len(kargs) != 0:
                json_txt = self.__replace_param(json_txt, kargs)
            conf = json.loads(json_txt)

            return conf
        except Exception as e:
            raise Exception(traceback.format_exc())


    def __replace_param(self, json_txt, kargs):
        template_txt = string.Template(json_txt)
        json_txt = template_txt.safe_substitute(kargs)

        return json_txt