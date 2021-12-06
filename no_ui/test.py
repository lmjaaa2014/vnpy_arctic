setting_filename: str = "data_recorder_setting.json"

file_name = setting_filename.split(".")[0]
setting_filename = f"{file_name}_aaaa.json"
print(setting_filename)