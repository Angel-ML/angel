@echo off
rem Tencent is pleased to support the open source community by making Angel available.
rem
rem Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
rem
rem Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
rem compliance with the License. You may obtain a copy of the License at
rem
rem https://opensource.org/licenses/Apache-2.0
rem
rem Unless required by applicable law or agreed to in writing, software distributed under the License
rem is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
rem or implied. See the License for the specific language governing permissions and limitations under
rem the License.
rem
rem ----------------------------------------------------------------------------------------
rem One-shot launcher for an Angel STANDALONE distributed cluster (PS-only, Windows).
rem
rem Usage:
rem   angel-cluster-up.bat <ip1,ip2,ip3,...> [extra angel-standalone-submit args...]
rem
rem The 1st IP is treated as the MASTER (this node MUST be the one running this script).
rem The 2..N IPs are used as PS hosts. No Worker processes are launched
rem (running mode = ANGEL_PS).
rem
rem See the bash version (angel-cluster-up) for full documentation.
rem ----------------------------------------------------------------------------------------

setlocal enabledelayedexpansion

if "%~1"=="" (
    echo Usage: %~nx0 ^<ip1,ip2,ip3,...^> [submit args...] 1>&2
    exit /b 1
)

set "IP_CSV=%~1"
shift

rem Split IP_CSV into MASTER_IP + PS_HOSTS
set "MASTER_IP="
set "PS_HOSTS="
set "first=1"
for %%I in ("%IP_CSV:,=" "%") do (
    set "ip=%%~I"
    if defined first (
        set "MASTER_IP=!ip!"
        set "first="
    ) else (
        if defined PS_HOSTS (
            set "PS_HOSTS=!PS_HOSTS!,!ip!"
        ) else (
            set "PS_HOSTS=!ip!"
        )
    )
)

if "%MASTER_IP%"=="" (
    echo IP list cannot be empty 1>&2
    exit /b 1
)
if "%PS_HOSTS%"=="" (
    echo IP list must contain at least 1 PS entry after the master 1>&2
    exit /b 1
)

call :count "%PS_HOSTS%"  PS_COUNT

if "%JAVA_HOME%"=="" (
    echo JAVA_HOME is not set 1>&2
    exit /b 1
)

set "BIN_DIR=%~dp0"
if "%ANGEL_HOME%"=="" (
    pushd "%BIN_DIR%.."
    set "ANGEL_HOME=%CD%"
    popd
)
if "%ANGEL_LIB%"=="" set "ANGEL_LIB=%ANGEL_HOME%\lib"
if not exist "%ANGEL_LIB%" (
    echo ANGEL_LIB directory not found: %ANGEL_LIB% 1>&2
    exit /b 1
)
if "%ANGEL_REMOTE_LIB%"=="" set "ANGEL_REMOTE_LIB=%ANGEL_LIB%/*"
if "%ANGEL_SSH_USER%"=="" set "ANGEL_SSH_USER=%USERNAME%"
if "%ANGEL_SSH_PORT%"=="" set "ANGEL_SSH_PORT=22"
if "%ANGEL_REMOTE_JAVA%"=="" set "ANGEL_REMOTE_JAVA=java"

echo [angel-cluster-up] master       : %MASTER_IP%
echo [angel-cluster-up] PS hosts     : %PS_HOSTS%       (count=%PS_COUNT%)
echo [angel-cluster-up] running mode : ANGEL_PS  (no Worker processes)
echo [angel-cluster-up] ssh user/port: %ANGEL_SSH_USER%@^<host^>:%ANGEL_SSH_PORT%
echo [angel-cluster-up] remote cp    : %ANGEL_REMOTE_LIB%

set "SUBMIT=%BIN_DIR%angel-standalone-submit.bat"
if not exist "%SUBMIT%" (
    echo missing %SUBMIT% 1>&2
    exit /b 1
)

rem Use AngelPSClusterMain so no AppSubmitter is required. The process prints
rem "ANGEL_MASTER=<ip>:<port>" on stdout once the cluster is up, then blocks until killed.
if "%ANGEL_MAIN_CLASS%"=="" set "ANGEL_MAIN_CLASS=com.tencent.angel.utils.AngelPSClusterMain"

set "CLUSTER_OPTS=-Dangel.am.host=%MASTER_IP%"
set "CLUSTER_OPTS=%CLUSTER_OPTS% -Dangel.running.mode=ANGEL_PS"
set "CLUSTER_OPTS=%CLUSTER_OPTS% -Dangel.standalone.hosts=%PS_HOSTS%"
set "CLUSTER_OPTS=%CLUSTER_OPTS% -Dangel.standalone.ps.hosts=%PS_HOSTS%"
set "CLUSTER_OPTS=%CLUSTER_OPTS% -Dangel.ps.number=%PS_COUNT%"
set "CLUSTER_OPTS=%CLUSTER_OPTS% -Dangel.workergroup.number=0"
set "CLUSTER_OPTS=%CLUSTER_OPTS% -Dangel.standalone.ssh.user=%ANGEL_SSH_USER%"
set "CLUSTER_OPTS=%CLUSTER_OPTS% -Dangel.standalone.ssh.port=%ANGEL_SSH_PORT%"
set "CLUSTER_OPTS=%CLUSTER_OPTS% -Dangel.standalone.remote.java=%ANGEL_REMOTE_JAVA%"
set "CLUSTER_OPTS=%CLUSTER_OPTS% -Dangel.standalone.remote.classpath=%ANGEL_REMOTE_LIB%"
if not "%ANGEL_SSH_OPTS%"==""       set "CLUSTER_OPTS=%CLUSTER_OPTS% -Dangel.standalone.ssh.opts=%ANGEL_SSH_OPTS%"
if not "%ANGEL_REMOTE_WORKDIR%"=="" set "CLUSTER_OPTS=%CLUSTER_OPTS% -Dangel.standalone.remote.workdir=%ANGEL_REMOTE_WORKDIR%"
if not "%ANGEL_REMOTE_LOG_DIR%"=="" set "CLUSTER_OPTS=%CLUSTER_OPTS% -Dangel.standalone.remote.log.dir=%ANGEL_REMOTE_LOG_DIR%"
if not "%ANGEL_PS_OPTS%"==""        set "CLUSTER_OPTS=%CLUSTER_OPTS% -Dangel.ps.java.opts=%ANGEL_PS_OPTS%"
if not "%ANGEL_MASTER_LOCATION_FILE%"=="" set "CLUSTER_OPTS=%CLUSTER_OPTS% -Dangel.master.location.file=%ANGEL_MASTER_LOCATION_FILE%"

call "%SUBMIT%" %CLUSTER_OPTS% %*
exit /b %ERRORLEVEL%

:count
setlocal enabledelayedexpansion
set "S=%~1"
set /a N=0
for %%X in ("%S:,=" "%") do set /a N+=1
endlocal & set "%~2=%N%"
goto :eof
