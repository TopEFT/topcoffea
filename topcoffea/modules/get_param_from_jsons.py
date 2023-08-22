import json

class GetParam:

    def __init__(self,params_json_path):
        self.__path = params_json_path

    def __call__(self,param_name):
        with open(self.__path) as f_params:
            params = json.load(f_params)
            param_val = params[param_name]
        return param_val
