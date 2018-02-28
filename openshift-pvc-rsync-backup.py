#!/usr/bin/python3

# Copyright (C) 2018, Gabriel Mainberger
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published
# by the Free Software Foundation; either version 2 of the License,
# or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, see <http://www.gnu.org/licenses/>.

from subprocess import Popen, PIPE
from datetime import datetime
import shlex
import os
import glob
import shutil

def validate_empty_die(variable, msg):
        if((variable == None) or (variable == "")):
                print(msg)
                exit()

def oc_run_jsonpath(cmd):
        args = shlex.split(cmd, posix=True)
        print(args)
        p = Popen(args, stdout=PIPE, stderr=PIPE)
        stdout, stderr = p.communicate()
        stdout_string = stdout.decode()
        if stdout_string == "": return None
        return stdout_string.split(" ")

def oc_run(cmd):
        args = shlex.split(cmd)
        p = Popen(args, stdout=PIPE, stderr=PIPE)
        stdout, stderr = p.communicate()
        return stdout.decode()

def shell_run(args):
        p = Popen(args, stdout=PIPE, stderr=PIPE)
        stdout, stderr = p.communicate()
        return stdout.decode()

def snapshot_directory_mkdir(path, debug=False):

        validate_empty_die(path, "snapshot_directory_mkdir: path variable invalid -> DIE")

        os.makedirs(path, exist_ok=True)
        print("SNAPSHOT DIRECTORY: " + path)

def hardlink_copy(lastsnapshot_path, currentsnapshot_path):
        
        args = []
        args.append("cp")
        args.append("-aRl")
        args.append(lastsnapshot_path + "/.")
        args.append(currentsnapshot_path)
        print("HARDLINK COPY: " + str(args))
        shell_run(args)

def oc_rsync(oc_binary, oc_config_file, project, pod, mount_path, destination_path):

        validate_empty_die(pod, "oc_rysnc: pod variable invalid -> DIE")
        
        args = []
        args.append(oc_binary)
        args.append("--config=" + oc_config_file)
        args.append("-n")
        args.append(project)
        args.append("rsync")
        args.append(pod + ":" + mount_path + "/.")
        args.append(destination_path)
        print("RSYNC: " + str(args))
        shell_run(args)

def subdirectory_last_created(path):
        
        sub_path = ""
        if os.path.exists(path):
                try:
                        sub_path = max(glob.glob(os.path.join(path, '*/')), key=os.path.getmtime)
                except:
                        sub_path = ""
        return sub_path

def cleanup_empty_directory(path):
        if path == "/": return
        if path == "/home": return
        if path == "/root": return
        if not os.path.exists(path):
                print("Existiert nicht!")
                return

        # Check for files in the directory
        for root, dirs, files in os.walk(path):
                if(len(files) > 0):
                        return

        shutil.rmtree(path)
        print("DIR REMOVED: " + path)


def snapshot_create(project, pod, pod_backup_path, backup_start_time, mountPath, oc_binary, oc_config_file):
   
        print(project + ":" + pod + " " + mountPath)
        
        currentsnapshot_path_without_mount_path = os.path.normpath(os.path.join(pod_backup_path, backup_start_time))
        currentsnapshot_path = os.path.normpath(currentsnapshot_path_without_mount_path + mountPath)

        snapshot_directory_mkdir(currentsnapshot_path)

        # Check if there was a last pod snapshot
        print(pod_lastsnapshot_path)
        if (pod_lastsnapshot_path != ""):
                                        
                # Check if there wan a valid last snapshot directory
                lastsnapshot_path = os.path.normpath(pod_lastsnapshot_path + mountPath)
                print(lastsnapshot_path)
                if os.path.exists(lastsnapshot_path):
                                                
                        # Create Hardlinks
                        hardlink_copy(lastsnapshot_path, currentsnapshot_path)

        # Rsync Backup
        oc_rsync(oc_binary, oc_config_file, project, pod, mountPath, currentsnapshot_path)

        cleanup_empty_directory(currentsnapshot_path_without_mount_path)

        print("\n")


def list_volume_mounts(project, pod, pvc, pod_backup_path, backup_start_time, oc_binary, oc_config_file):

        # List Volume Mounts of the Pod, if it is an Persistent Volume Claim
        mount_paths = None
        mount_paths = oc_run_jsonpath(oc_tool_project_cmd + "get pod " + pod + " -o jsonpath='{.spec.containers[].volumeMounts[?(@.name==\"" + pvc + "\")].mountPath}'")
        if mount_paths == None: return
        for mount_path in mount_paths:
                snapshot_create(project, pod, pod_backup_path, backup_start_time, mount_path, oc_binary, oc_config_file)


oc_binary = "./oc"
oc_config_file = "admin.kubeconfig"
oc_url = "https://master:8443"

cluster_name = "mycluster"

backup_start_time_date = datetime.utcnow()
backup_start_time = backup_start_time_date.strftime("%Y%m%d-%H%M%S")
oc_tool_cmd = "./oc --config=" + oc_config_file + " "


# Login
oc_run(oc_tool_cmd + "login -u system:admin " + oc_url)

projects = oc_run_jsonpath(oc_tool_cmd + "get projects -o jsonpath='{.items[?(@.status.phase==\"Active\")].metadata.name}'")
for project in projects:
        pods = None
        oc_tool_project_cmd = None

        oc_tool_project_cmd = oc_tool_cmd + " -n " + project + " "

        # Get Running Pods
        pods = oc_run_jsonpath(oc_tool_project_cmd + "get pods -o jsonpath='{.items[?(@.status.phase==\"Running\")].metadata.name}'")

        # List Running Pods
        if pods == None: continue
        for pod in pods:

                # Variables
                pod_base_name = "-".join(pod.split("-")[:-2])
                pod_backup_path = os.path.join("openshift-pvc-rsync-backup", cluster_name, project, pod_base_name, "backup")

                pod_lastsnapshot_path = subdirectory_last_created(pod_backup_path)

                # List Persistent Volume Claims of the Pod
                pvcs = None
                pvcs = oc_run_jsonpath(oc_tool_project_cmd + "get pod " + pod + " -o jsonpath='{.spec.volumes[?(@.persistentVolumeClaim)].name}'")
                if pvcs == None: continue
                for pvc in pvcs:
                        list_volume_mounts(project, pod, pvc, pod_backup_path, backup_start_time, oc_binary, oc_config_file)
                

