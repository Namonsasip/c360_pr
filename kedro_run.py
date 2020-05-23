import subprocess
import shlex

def run_command(command):
    process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, cwd='/home/cdsw/customer360/')
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print(output.strip())
    rc = process.poll()
    return rc

run_command("kedro run --pipeline=customer_profile_to_l1_pipeline")