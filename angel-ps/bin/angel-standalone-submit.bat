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
rem -------------------------------------------------------------------------------------------------
rem Submit an Angel job in STANDALONE deploy mode (Windows).
rem -------------------------------------------------------------------------------------------------

setlocal enabledelayedexpansion

set "ANGEL_VERSION=2.5.0"

if "%JAVA_HOME%"=="" (
    echo JAVA_HOME is not set 1>&2
    exit /b 1
)
set "PATH=%JAVA_HOME%\bin;%PATH%"
set "JAVA=%JAVA_HOME%\bin\java.exe"

set "BIN_DIR=%~dp0"
if "%ANGEL_HOME%"=="" (
    pushd "%BIN_DIR%.."
    set "ANGEL_HOME=%CD%"
    popd
)

if "%ANGEL_LIB%"=="" set "ANGEL_LIB=%ANGEL_HOME%\lib"

if not exist "%ANGEL_LIB%" (
    echo ANGEL_LIB directory not found: %ANGEL_LIB% 1>&2
    echo Set ANGEL_LIB to the directory containing Angel jars and hadoop-* jars. 1>&2
    exit /b 1
)

set "CP="
for %%f in ("%ANGEL_LIB%\*.jar") do (
    if "!CP!"=="" (
        set "CP=%%f"
    ) else (
        set "CP=!CP!;%%f"
    )
)

if not "%ANGEL_USER_JAR%"=="" set "CP=%CP%;%ANGEL_USER_JAR%"

if not "%HADOOP_CONF_DIR%"=="" set "CP=%HADOOP_CONF_DIR%;%CP%"

set "CLASSPATH=%CP%"

set "DEPLOY_MODE_OPT=-Dangel.deploy.mode=STANDALONE"
if "%ANGEL_MAIN_CLASS%"=="" set "ANGEL_MAIN_CLASS=com.tencent.angel.utils.AngelRunJar"
set "CLASS=%ANGEL_MAIN_CLASS%"

set PYTHONHASHSEED=0
"%JAVA%" %ANGEL_JAVA_OPTS% %CLASS% %DEPLOY_MODE_OPT% %*
exit /b %ERRORLEVEL%
