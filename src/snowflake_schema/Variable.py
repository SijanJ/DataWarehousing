import json


class Variables:
    def __init__(self):
        self.var = dict()
        with open('config.json')as f:
            self.var = json.load(f)

    def get(self, variable_name):
        if self.exists(variable_name):
            return self.var[variable_name]
        else:
            return None

    def set(self, variable_name, variable_value):
        self.var[variable_name] = variable_value

    def exists(self, variable_name):
        if variable_name in self.var.keys():
            return 1
        else:
            return 0




