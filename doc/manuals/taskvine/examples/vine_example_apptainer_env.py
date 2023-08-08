#! /usr/bin/env python

import ndcctools.taskvine as vine

import subprocess
import argparse


def env_from_image(manager, image_path, run_script="run_command_in_apptainer.sh"):
    # construct the mini task. We only need the mini task for its sandbox to
    # create the environment structure, thus we use the command ":" as no-op.
    mt = vine.Task(":")

    runner = m.declare_file(run_script, cache=True)
    image  = m.declare_file(image_path, cache=True)

    mt.add_input(runner, "env_dir/bin/run_in_env")
    mt.add_input(image,  "env_dir/image.sif")

    # the output of the mini task is the environment directory
    mt.add_output(image,  "env_dir")

    # tell the manager that this is a mini task.
    return m.declare_minitask(mt)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            prog="vine_example_apptainer_env.py",
            description="This example TaskVine program shows how to construct a custom environment to execute Tasks inside an Apptainer container.")

    parser.add_argument('--image', action='store', help='Container image to use. (To get an example image, try: apptainer pull docker://ghcr.io/apptainer/lolcow)', default="lolcow_latest.sif")
    args = parser.parse_args()


    m = vine.Manager(port=0, ssl=True)
    print(f"listening for workers at port {m.port}")

    # tell the manager that this is a mini task.
    env = env_from_image(m, args.image)

    # now we define our regular task, and attach the environment to it.
    t = vine.Task("/bin/echo 'from inside apptainer!'")
    t.set_cores(1)
    t.add_environment(env)

    m.submit(t)

    factory = vine.Factory(manager=m)
    factory.max_workers = 1
    factory.min_workers = 0
    factory.cores = 1

    with factory:
        while not m.empty():
            t = m.wait(5)
            if not t:
                continue

            if t.successful():
                print(f"task {t.id} completed: {t.std_output}")
            else:
                print(f"task {t.id} failed. return status: {t.result_string}. exit code: {t.exit_code}")


