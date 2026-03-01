import datetime
from pathlib import Path
from Variable import Variables


class Logger:
    def __init__(self, v: Variables):
        self.v = v
        if not self.v.get("SCRIPT_NAME"):
            self.v.set("SCRIPT_NAME", "Nightly_Batch")
        log_path = Path(__file__).resolve().parents[1] / self.v.get("LOG_PATH")
        log_path.mkdir(parents=True, exist_ok=True)

        script_name = self.v.get("SCRIPT_NAME")
        current_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        log_file_name = str(script_name) + "_" + current_time + ".log"
        log_file = log_path / log_file_name
        self.log_file = open(log_file, 'w')

    def message(self, msg):
        self.log_file.write(str(datetime.datetime.now()))
        self.log_file.write(": ")
        self.log_file.write(msg)
        self.log_file.write("\n")
        self.log_file.flush()

    def close(self):
        self.log_file.close()