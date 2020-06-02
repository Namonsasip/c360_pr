import subprocess
import shlex
import os

pipeline_to_run = os.environ["pipeline_to_run"]
env_to_use = os.environ["CONF"]

kedro_run_cmd = "kedro run --pipeline={} --env={}".format(pipeline_to_run, env_to_use)

print(kedro_run_cmd)


def run_command(command):
    process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, cwd='/home/cdsw/customer360/',
                               encoding='utf8')
    with open("logfile.txt", "w") as log_file:
        while process.poll() is None:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                if output.startswith("Above") or output.startswith("Below"):
                    pass
                else:
                    print(output.strip())
                    log_file.write(output)
    process.communicate()
    return process.returncode


run_command(kedro_run_cmd)
