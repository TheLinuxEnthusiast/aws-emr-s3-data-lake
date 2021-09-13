# Source global definitions
if [ -f /etc/bashrc ]; then
  . /etc/bashrc
fi

# set the default region for the AWS CLI
export AWS_DEFAULT_REGION=us-west-2
export JAVA_HOME=/etc/alternatives/jre
export HOME=/home/hadoop
export PYSPARK_PYTHON=python3
