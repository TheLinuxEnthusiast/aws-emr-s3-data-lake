### Sparkify : AWS data lake with pyspark

**Name: Darren Foley**

**Creation Date: 2021-09-14**

**Email: darren.foley@ucdconnect.ie**

<br>

### Project Description

<br>

| File Name              | File Type        | Description                                                                   |
|-----------------------:|:----------------:|:-----------------------------------------------------------------------------:|
| .bashrc                | config           | Sample bashrc for EMR instance                                                | 
| .gitignore             | config           | List of files/directories to not include in source control                    |
| EMR-with-bootstrap.sh  | shell            | EMR creation script with a bootstrap execution script                         |
| EMR.sh                 | shell            | EMR creation script without bootstrap                                         |
| README.md              | markdown         | Readme file                                                                   |
| bootstrap.sh           | shell            | Bootstrap script for ensuring environment is ready for running pyspark script |
| dl-template.cfg        | config           | Sample dl template config (Required before running etl.py)                    |
| etl-test.py            | python           | Test etl script for testing if pipeline is working                            |
| etl.py                 | python           | ETL script for processing entire sparkify dataset                             |
| prototype.ipynb        | python notebook  | Testing notebook for prototyping etl code                                     |
| sample-query.py        | python           | Sample query to prove that data has been written correctly to S3              |
| updateGit.sh           | shell            | Script for updating git parameters on admin server                            |
| uplpadToS3.sh          | shell            | Test script for uploading files to S3                                         |
|                        |                  |                                                                               |

<br> 

#### Project set-up

<br>

<p>The project pipeline is designed to be executed from within an EMR instance on AWS. The instance must have the following properties:</p>

1. Region: us-west-2 (To minimise geographical distance between EMR, source S3 and destination S3 buckets)

2. Nodes: Single master node and 2 worker nodes running emr version emr-5.28.0. Instance type = m5.xlarge.

3. Applications to be installed: Spark


<p>For convenience, the script "EMR-with-bootstrap.sh" is a wrapper for the aws cli which executes the following command</p>


```

    aws emr create-cluster \
    --name "${CLUSTER_NAME}" \
    --use-default-roles \
    --release-label emr-5.28.0 \
    --instance-count 3 \
    --applications Name=Spark  \
    --ec2-attributes KeyName="${KEY_NAME}",SubnetId="${SUBNET_NAME}" \
    --instance-type m5.xlarge \
    --profile "${PROFILE_NAME}" \
    --bootstrap-actions Path="s3://sparkify-etl-code-df/bootstrap.sh",Args=[${BUCKET_NAME}]

```


<p>You simply need to provide the various input parameters to create the EMR instance. It can be run like this from the command line:</p>

```

# ./EMR-with-bootstrap.sh -h
Usage ./EMR-with-bootstrap.sh [-n|name] [-k|key] [-s|subnet] [-p|profile] [-b|bucket Name] [-h|help]

# ./EMR-with-bootstrap.sh -n "etl-cluster" -k "etl-key" -s "subnet-9f5738c2" -p "admin" -b "sparkify-data-lake-df"

```

1. [-n] -  Name of cluster

2. [-k] -  Key-pair created for connecting to EMR

3. [-s] - Subnet into which the EMR will be created

4. [-p] - Profile credentials for AWS user (Must be created beforehand)

5. [-b] - Output bucket Name (I have used sparkify-data-lake-df)

<br>

#### EMR Environment Set-up and ETL execution

<p>The bootstrap script should set up most of the environment variables required to run the ETL.py script. Check the dl.cfg to make sure the destination bucket is provided. Source Code is pulled from a public bucket called </p>

**sparkify-etl-code-df**

<p> otherwise, you'll need to clone the git repo onto the master node and set up by running </p>

**./bootstrap.sh 'bucket_name'**

<p>. bucket_name the destination bucket name with where you would like to push the output parquet files to.</p>

<br>

<p>You can run the etl.py pipeline by submitting the code from the masternode like so:</p>

```

# /usr/bin/spark-submit --master yarn ./etl.py

```

<p>If everything works, the pyspark job should take approx <p/> 

**20min**

<p>. You should see output directories, one for each table:</p>

sparkify-data-lake-df/

    -> dim_user/
    
    -> dim_artist/
    
    -> dim_song/
    
    -> dim_time/
    
    -> fact_songPlay/

<br>

### Verify Data Lake with Sample Query

<br>

<p>The script sample-query.py is a simple analytical query which helps verify that parquet files were written correctly ot S3.</p>

<p>The query will list top ten most active users within sparkify logs</p>

```

    SELECT
        u.first_name,
        u.last_name,
        sp.user_id,
        COUNT(DISTINCT sp.session_id) as session_count
    FROM songplay sp
    JOIN users u
    ON u.user_id = sp.user_id
    GROUP BY 
        u.first_name,
        u.last_name,
        sp.user_id
    ORDER BY session_count DESC
    LIMIT 10

```

<p>Ouput should look like this:</p>

|             |            |          |               |
| first_name  | last_name  | user_id  | session_count |
|------------:|:----------:|:--------:|:-------------:|
| Chloe       | Cuevas     | 49       | 28            |
| Tegan       | Levine     | 80       | 22            |
| Kate        | Harrell    | 97       | 13            |
| Lily        | Koch       | 15       | 11            |
| Aleena      | Kirby      | 44       | 10            |
| Ava         | Robinson   | 50       | 8             |
| Mohammad    | Rodriguez  | 88       | 8             |    
| Matthew     | Jones      | 36       | 8             |
| Layla       | Griffin    | 24       | 7             |
| Jacqueline  | Lynch      | 29       | 7             |
|             |            |          |               |

<p>Output should be sent to an output folder </p>

**query_output/**

<p>. The file will be in csv format.</p>