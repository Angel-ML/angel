#!/usr/bin/env bash

if [ $# -eq 0 -o $1 = "-h" -o $1 = "--help" ]; then
    echo "Usage: lauchps -logpath logpath -temppath temppath [-psnum psnum] [-psmen psmen]"
    exit
fi

until [ $# -eq 0 ]; do
    flag=0

    if [ $1 = "-logpath" -o $1 = "logpath" ]; then
        shift
        logpath=$1
        shift
        if [ $# -eq 0 ]; then
            break
        fi
        flag=1
    fi

    if [ $1 = "-temppath" -o $1 = "temppath" ]; then
        shift
        temppath=$1
        shift
        if [ $# -eq 0 ]; then
            break
        fi
        flag=1
    fi

    if [ $1 = "-psnum" -o $ $1 = "psnum" ]; then
        shift
        psnum=$1
        shift
        if [ $# -eq 0 ]; then
            break
        fi
        flag=1
    fi

    if [ $1 = "-psmen" -o $ $1 = "psmem" ]; then
        shift
        psmem=$1
        shift
        if [ $# -eq 0 ]; then
            break
        fi
        flag=1
    fi

    if [ ${flag} -eq 0 -a $# -ne 0 ]; then
        echo "ERROR!"
        echo "Usage: lauchps -logpath logpath -temppath temppath [-psnum psnum] [-psmen psmen]"
        exit
    fi
done

if [ -z ${logpath} ]; then
    echo "logpath must set!"
    exit
fi

if [ -z ${temppath} ]; then
    echo "temppath must set!"
    exit
fi

if [ -z ${psnum} ]; then
    psnum=5
fi

if [ -z ${psmem} ]; then
    psmem=10
fi


${ANGEL_HOME}/bin/angel-submit \
     -Dangel.output.path.deleteonexist=true \
     -Dangel.am.log.level=DEBUG \
     -Dangel.ps.log.level=DEBUG \
     -Dangel.log.path=${logpath} \
     -Daction.type=serving \
     -Dangel.app.submit.class=com.tencent.angel.serving.laucher.ServingRunner  \
     -Dangel.serving.temp.path=${temppath} \
     -Dangel.ps.number=${psnum} \
     -Dangel.ps.memory.gb=${psmem} \
     -Dqueue=root.g_teg_angel.g_teg_angel-offline \
     -Dangel.job.name=angel_serving
