#!/bin/bash

usage() { 
        echo "Usage $0 [-p| S3 path] [-h|help]" 
        exit 0
}


S3_PATH=
PROFILE="admin"
while getopts "p:h" o;
do
	case "${o}" in
	p)
		S3_PATH="${OPTARG}"
	;;
	h)
		usage
	;;
	*)
		usage
	;;
	esac
done
shift $(($OPTIND-1))

if [ -z "${S3_PATH}" ];
then
    usage
else
    echo "${S3_PATH}"
fi

uploadToS3(){
    aws s3 cp /home/workspace/prototype.ipynb "${S3_PATH}" --profile "${PROFILE}"
}


main(){
    uploadToS3

}

main

