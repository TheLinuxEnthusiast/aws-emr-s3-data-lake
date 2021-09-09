#!/bin/bash

usage(){ 
    echo "Usage $0 [-t|test] [-n|name] [-k|key] [-s|subnet] [-p|profile] [-h|help] [-i|cluster_id]" 
    exit 1
}

TEST=1
CLUSTER_NAME=
KEY_NAME=
SUBNET_NAME=
PROFILE_NAME=
CLUSTER_ID=
while getopts ":tn:k:s:p:hi:" arg
do
    case "$arg" in
        t)
            echo "Auto terminate disabled"
            TEST=0
            CLUSTER_NAME="test-cluster"
            KEY_NAME="emr-key"
            SUBNET_NAME="subnet-ef7fe884"
            PROFILE_NAME="admin"
            ;;
        n)
            CLUSTER_NAME="${OPTARG}"
            ;;
        k)
            KEY_NAME="${OPTARG}"
            ;;
        s)
            SUBNET_NAME="${OPTARG}"
            ;;
        p)
            PROFILE_NAME="${OPTARG}"
            ;;
        h)
            usage
            ;;
        i)
            CLUSTER_ID="${OPTARG}"
            ;;
        *)
            usage
            ;;
    esac

done
shift $((OPTIND-1))


# Make sure all information is provided
if [ -z "${CLUSTER_NAME}" ] || [ -z "${KEY_NAME}" ] || [ -z "${SUBNET_NAME}" ] || [ -z "${PROFILE_NAME}" ] ;then
    usage
else
    echo "${CLUSTER_NAME}"
    echo "${KEY_NAME}"
    echo "${SUBNET_NAME}"
    echo "${PROFILE_NAME}"
fi

# Cannot destroy and create a cluster at the same time. Specify Cluster id only to destroy a particular cluster.
if [ ! -z ${CLUSTER_ID} ] && [ ! -z "${CLUSTER_NAME}" ] || [ ! -z "${KEY_NAME}" ] || [ ! -z "${SUBNET_NAME}" ]; then
    usage
fi


callTestEMR(){

    aws emr create-cluster \
    --name "${CLUSTER_NAME}" \
    --use-default-roles \
    --release-label emr-5.28.0 \
    --instance-count 3 \
    --applications Name=Spark  \
    --ec2-attributes KeyName="${KEY_NAME}",SubnetId="${SUBNET_NAME}" \
    --instance-type m5.xlarge \
    --profile "${PROFILE_NAME}" \
    --auto-terminate
    
}

callEMR(){

    aws emr create-cluster \
    --name "${CLUSTER_NAME}" \
    --use-default-roles \
    --release-label emr-5.28.0 \
    --instance-count 3 \
    --applications Name=Spark  \
    --ec2-attributes KeyName="${KEY_NAME}",SubnetId="${SUBNET_NAME}" \
    --instance-type m5.xlarge \
    --profile "${PROFILE_NAME}"
    
}


deleteEMR(){

    aws emr terminate-clusters \
    --cluster-ids "${CLUSTER_ID}" \
    --profile "${PROFILE_NAME}"
    
}

main(){
    [ ! -z "${CLUSTER_ID}" ] && deleteEMR && exit 0
    [ "${TEST}" = "0" ] && callTestEMR || callEMR
    #echo "main"
    #callTestEMR
}

main

