#!/usr/bin/env python

from fabric.api import *
from fabric.colors import *

env.user = "apps"

env.roledefs = {
    "offline": [ "qaapp0" ]
}

env.use_ssh_config = True

local_dir = ""
remote_dir = "/apps/svr/offline"

def putfile(s, d):
    with settings(warn_only=True):
        result = put(s, d)
    if result.failed and not confirm("put tar file failed, Continue[Y/N]"):
        abort("aborting file put: %s-----%s" % (s, d))
    else:
        print green("Successfully put " + s + " to dir " + d)


def checkmd5(s, d):
    with settings(warn_only=True):
        lmd5 = local("md5 %s" % s, capture=True).split(' ')[-1]
        rmd5 = run("md5sum %s" % d).split(' ')[0]
    if lmd5 == rmd5:
        return True
    else:
        return False

def putJar(name):
    with cd(remote_dir + "/jars"):
        if checkmd5("target/" + name, remote_dir + "/jars/" + name) == False:
            putfile("target/" + name, remote_dir + "/jars/")

def deployConfig(upload_files):
    with settings(warn_only=True):
        if run("test -d %s" % remote_dir).failed:
            run("mkdir -p %s/{bin,conf,jars,logs,lib}" % remote_dir)
    with cd(remote_dir):
        for f in upload_files:
            if checkmd5(f, remote_dir + '/' + f) == False:
                putfile(f, remote_dir + '/' + f)
        run("chmod +x bin/*.sh")

@task
@roles("offline")
def deployServer():
    with lcd("./server"):
        # local("mvn clean package -DskipTests")
        putJar("server-0.3.0.jar")

    upload_files = [
        "conf/application.conf",
        "conf/log4j2.xml",
        "bin/start-server.sh",
    ]
    deployConfig(upload_files)

@task
@roles("offline")
def deployApi():
    with lcd("./api"):
        local("mvn clean package -DskipTests")
        putJar("api-0.3.0.jar")

@task
@roles("offline")
def deployJobs():
    with lcd("./jobs"):
        local("mvn package -DskipTests")
        putJar("jobs-0.3.0.jar")

@task
@roles("offline")
def deploy():
    deployApi()
    deployJobs()
    deployServer()
