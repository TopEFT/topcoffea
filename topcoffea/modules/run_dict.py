from topcoffea.modules.get_param_from_jsons import GetParam
from topcoffea.modules.paths import topcoffea_path
run_dict = GetParam(topcoffea_path("params/params.json"))

def is_run2(year):
    ''' Check if a year is in Run 2 '''
    return year in run_dict("Run2")


def is_run3(year):
    ''' Check if a year is in Run 3 '''
    return year in run_dict("Run3")
