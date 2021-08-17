import subprocess
import shlex
import os, sys

path_project = os.environ["PATH_PROJECT"]
pipeline_to_run = os.environ["PIPELINE_TO_RUN"]
env_to_use = os.environ["CONF"]
p_increment = os.environ["RUN_INCREMENT"]
p_partition = os.environ["RUN_PARTITION"]
p_features = os.environ["RUN_FEATURES"]


kedro_run_cmd = "kedro run --pipeline={} --env={}".format(pipeline_to_run, env_to_use)

print(kedro_run_cmd)

def run_command(command):
    process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE,
                               cwd=path_project, encoding='utf8', bufsize=-1)
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print(output.strip())
    rc = process.poll()
    if rc:
        print("Return Code {}".format(rc))
    return rc


return_flag = run_command(kedro_run_cmd)
if return_flag:
    if return_flag != 0:
        sys.exit(2)