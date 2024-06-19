import pandas as pd
class ExcelConfig:
    def __init__(self, parameter_file, sheet_name, defaults=None):
        self.parameters = self.load_parameters(parameter_file, sheet_name)
        self.defaults = defaults or {}

    @staticmethod
    def load_parameters(file, sheet):
        df = pd.read_excel(file, sheet_name=sheet)
        # Fill NaN values with None to handle blanks in the Excel file
        df = df.fillna('')
        return df.set_index('Parameter')['Value'].to_dict()

    def get(self, key):
        # Return the parameter value if it exists, otherwise return the default value
        value = self.parameters.get(key, self.defaults.get(key))
        # Handle empty string as missing value if required
        if value == '':
            value = self.defaults.get(key)
        return value