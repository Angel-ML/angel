#
# Tencent is pleased to support the open source community by making Angel available.
#
# Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License") you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
#
# https://opensource.org/licenses/Apache-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions and
#

from pyangel.context import Configuration

class LocalFileSystem(object):
    """
    FileSystem API for checksumed local filesystem.
    """

    _jvm = Configuration._jvm
    _jfs = None

    def __init__(self, conf=None):
        _jfs = _jvm.org.apache.hadoop.fs.LocalFileSystem


    DEFAULT_FS = "file:///"

class CommonConfigurationKeys(object):
    """
    This class contains constans for configuration keys used in the
    common code.

    It is a simple wrapper for org.apache.hadoop.fs.CommonConfigurationKeys
    """


    #Default location for user home directories #
    FS_HOME_DIR_KEY = "fs.homeDir"
    # Default value for FS_HOME_DIR_KEY #
    FS_HOME_DIR_DEFAULT = "user"
    # Default umask for files created in HDFS #
    FS_PERMISSIONS_UMASK_KEY = "fs.permissions.umask-mode"
    # Default value for FS_PERMISSIONS_UMASK_KEY #
    FS_PERMISSIONS_UMASK_DEFAULT = 18
    # How often does RPC client send pings to RPC server #
    IPC_PING_INTERVAL_KEY = "ipc.ping.erval"
    # Default value for IPC_PING_INTERVAL_KEY #
    IPC_PING_INTERVAL_DEFAULT = 60000  # 1 min
    # Enables pings from RPC client to the server #
    IPC_CLIENT_PING_KEY = "ipc.client.ping"
    # Default value of IPC_CLIENT_PING_KEY #
    IPC_CLIENT_PING_DEFAULT = True
    # Responses larger than this will be logged #
    IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY = "ipc.server.max.response.size"
    # Default value for IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY #
    IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT = 1024 * 1024
    # Number of threads in RPC server reading from the socket #
    IPC_SERVER_RPC_READ_THREADS_KEY = "ipc.server.read.threadpool.size"
    # Default value for IPC_SERVER_RPC_READ_THREADS_KEY #
    IPC_SERVER_RPC_READ_THREADS_DEFAULT = 1

    IPC_MAXIMUM_DATA_LENGTH = "ipc.maximum.data.length"

    IPC_MAXIMUM_DATA_LENGTH_DEFAULT = 64 * 1024 * 1024

    # How many calls per handler are allowed in the queue. #
    IPC_SERVER_HANDLER_QUEUE_SIZE_KEY = "ipc.server.handler.queue.size"
    # Default value for IPC_SERVER_HANDLER_QUEUE_SIZE_KEY #
    IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT = 100

    # Internal buffer size for Lzo compressordecompressors #/
    IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_KEY = "io.compression.codec.lzo.buffersize"
    # Default value for IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_KEY #
    IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_DEFAULT = 64 * 1024

    # This is for specifying the implementation for the mappings from
    # hostnames to the racks they belong to
    NET_TOPOLOGY_CONFIGURED_NODE_MAPPING_KEY = "net.topology.configured.node.mapping"

    # Internal buffer size for Snappy compressordecompressors #/
    IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY = "io.compression.codec.snappy.buffersize"

    # Default value for IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY #
    IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT = 256 * 1024

    # Internal buffer size for Snappy compressordecompressors #/
    IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_KEY = "io.compression.codec.lz4.buffersize"

    # Default value for IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY #
    IO_COMPRESSION_CODEC_LZ4_BUFFERSIZE_DEFAULT = 256 * 1024

    # Service Authorization
    HADOOP_SECURITY_SERVICE_AUTHORIZATION_REFRESH_POLICY = "security.refresh.policy.protocol.acl"

    HADOOP_SECURITY_SERVICE_AUTHORIZATION_GET_USER_MAPPINGS = "security.get.user.mappings.protocol.acl"

    HADOOP_SECURITY_SERVICE_AUTHORIZATION_REFRESH_USER_MAPPINGS = "security.refresh.user.mappings.protocol.acl"

    SECURITY_HA_SERVICE_PROTOCOL_ACL = "security.ha.service.protocol.acl"

    SECURITY_ZKFC_PROTOCOL_ACL = "security.zkfc.protocol.acl"

    SECURITY_CLIENT_PROTOCOL_ACL = "security.client.protocol.acl"
    SECURITY_CLIENT_DATANODE_PROTOCOL_ACL = "security.client.datanode.protocol.acl"

    SECURITY_DATANODE_PROTOCOL_ACL = "security.datanode.protocol.acl"

    SECURITY_INTER_DATANODE_PROTOCOL_ACL = "security.er.datanode.protocol.acl"

    SECURITY_NAMENODE_PROTOCOL_ACL = "security.namenode.protocol.acl"
    SECURITY_QJOURNAL_SERVICE_PROTOCOL_ACL = "security.qjournal.service.protocol.acl"
    HADOOP_SECURITY_TOKEN_SERVICE_USE_IP = "hadoop.security.token.service.use_ip"
    HADOOP_SECURITY_TOKEN_SERVICE_USE_IP_DEFAULT = True

    # HA health monitor and failover controller.

    # How often to retry connecting to the service.
    HA_HM_CONNECT_RETRY_INTERVAL_KEY = "ha.health-monitor.connect-retry-erval.ms"
    HA_HM_CONNECT_RETRY_INTERVAL_DEFAULT = 1000

    # How often to check the service.
    HA_HM_CHECK_INTERVAL_KEY = "ha.health-monitor.check-erval.ms"
    HA_HM_CHECK_INTERVAL_DEFAULT = 1000

    # How long to sleep after an unexpected RPC error.
    HA_HM_SLEEP_AFTER_DISCONNECT_KEY = "ha.health-monitor.sleep-after-disconnect.ms"
    HA_HM_SLEEP_AFTER_DISCONNECT_DEFAULT = 1000

    # Timeout for the actual monitorHealth() calls. *
    HA_HM_RPC_TIMEOUT_KEY = "ha.health-monitor.rpc-timeout.ms"
    HA_HM_RPC_TIMEOUT_DEFAULT = 45000

    # Timeout that the FC waits for the new active to become active
    HA_FC_NEW_ACTIVE_TIMEOUT_KEY = "ha.failover-controller.new-active.rpc-timeout.ms"
    HA_FC_NEW_ACTIVE_TIMEOUT_DEFAULT = 60000

    # Timeout that the FC waits for the old active to go to standby
    HA_FC_GRACEFUL_FENCE_TIMEOUT_KEY = "ha.failover-controller.graceful-fence.rpc-timeout.ms"
    HA_FC_GRACEFUL_FENCE_TIMEOUT_DEFAULT = 5000

    # FC connection retries for graceful fencing
    HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES = "ha.failover-controller.graceful-fence.connection.retries"
    HA_FC_GRACEFUL_FENCE_CONNECTION_RETRIES_DEFAULT = 1

    # Timeout that the CLI (manual) FC waits for monitorHealth, getServiceState
    HA_FC_CLI_CHECK_TIMEOUT_KEY = "ha.failover-controller.cli-check.rpc-timeout.ms"
    HA_FC_CLI_CHECK_TIMEOUT_DEFAULT = 20000

    # Static user web-filter properties.
    # See StaticUserWebFilter.
    HADOOP_HTTP_STATIC_USER = "hadoop.http.staticuser.user"
    DEFAULT_HADOOP_HTTP_STATIC_USER = "dr.who"

    # EnableDisable aliases serving from jetty
    HADOOP_JETTY_LOGS_SERVE_ALIASES = "hadoop.jetty.logs.serve.aliases"
    DEFAULT_HADOOP_JETTY_LOGS_SERVE_ALIASES = True

    # Path to the Kerberos ticket cache.  Setting this will force
    # UserGroupInformation to use only this ticket cache file when creating a
    # FileSystem instance.
    KERBEROS_TICKET_CACHE_PATH = "hadoop.security.kerberos.ticket.cache.path"

    HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_KEY = "hadoop.security.uid.cache.secs"

    HADOOP_SECURITY_UID_NAME_CACHE_TIMEOUT_DEFAULT = 4*60*60  # 4 hours

    IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY = "ipc.client.fallback-to-simple-auth-allowed"
    IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT = False
