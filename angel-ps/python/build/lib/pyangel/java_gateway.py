#
# Tencent is pleased to support the open source community by making Angel available.
#
# Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
#
# Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
#
# https://opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions and
#

import os
import sys
import select
import signal
import shlex
import socket
import struct
from subprocess import Popen, PIPE

if sys.version >= '3':
    xrange = range

from py4j.java_gateway import java_import, JavaGateway, GatewayClient
from pyangel.find_angel_home import _find_angel_home

def read_int(stream):
    length = stream.read(4)
    if not length:
        raise EOFError
    return struct.unpack("!i", length)[0]

def launch_gateway(conf=None):
    """
    launch jvm gateway
    :param conf: configuration that ml_runner must have
    :return:
    """
    if "PYANGEL_GATEWAY_PORT" in os.environ:
        gateway_port = int(os.environ["PYANGEL_GATEWAY_PORT"])
    else:
        ANGEL_HOME = _find_angel_home()
        # Launch the Py4j gateway

        if os.environ.get("PYANGEL_LOCAL_MODE") == "True":
            script = "./bin/angel-local-submit"
        else:
            script = "./bin/angel-submit"
        command = [os.path.join(ANGEL_HOME, script)]
        if conf:
            for k, v in conf.getAll():
                command += ['--conf', '%s=%s' % (k, v)]
        submit_args = os.environ.get("PYANGEL_SUBMIT_ARGS",
                                     "--angel.app.submit.class com.tencent.angel.api.python.PythonGatewayServer")
        command = command + shlex.split(submit_args)

        # Start a socket that will be used by PythonGatewayServer to communicate its port to python sub-proc
        callback_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        callback_socket.bind(('127.0.0.1', 0))
        callback_socket.listen(1)
        callback_host, callback_port = callback_socket.getsockname()
        env = dict(os.environ)
        env['_PYANGEL_CALLBACK_HOST'] = callback_host
        env['_PYANGEL_CALLBACK_PORT'] = str(callback_port)

        # Don't send ctrl-c / SIGINT to the Java gateway:
        def preexec_func():
            signal.signal(signal.SIGINT, signal.SIG_IGN)
        proc = Popen(command, stdin=PIPE, preexec_fn=preexec_func, env=env)

        gateway_port = None
        # We use select() here in order to avoid blocking indefinitely if the subprocess dies
        # before connecting
        while gateway_port is None and proc.poll() is None:
            timeout = 1  # (seconds)
            readable, _, _ = select.select([callback_socket], [], [], timeout)
            if callback_socket in readable:
                gateway_connection = callback_socket.accept()[0]
                # Determine which ephemeral port the server started on:
                gateway_port = read_int(gateway_connection.makefile(mode="rb"))
                gateway_connection.close()
                callback_socket.close()
        if gateway_port is None:
            raise Exception("Java gateway process exited before sending the driver its port number")

    # Connect to the gateway
    gateway = JavaGateway(GatewayClient(port=gateway_port), auto_convert=True)

    return gateway
