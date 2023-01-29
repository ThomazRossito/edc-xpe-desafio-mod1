resource "aws_sfn_state_machine" "stepfunction" {
  name       = "StepFunctionExecEmr"
  role_arn   = "arn:aws:iam::${var.account_id}:role/StepFunctionRole" #trocar para a role do ambiente
  definition = <<EOF
{
  "Comment": "A description of my state machine",
  "StartAt": "EMR CreateCluster",
  "States": {
    "EMR CreateCluster": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:createCluster.sync",
      "Parameters": {
        "Name": "tarn-emr-xpe-desafio-mod1-stf",
        "Configurations": [
          {
            "Classification": "spark-hive-site",
            "Properties": {
                "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            }
          },
          {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.submit.deployMode": "cluster",
                "spark.speculation": "false",
                "spark.sql.adaptive.enabled": "true",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
            }
          },
          {
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "true"
            }
          }
        ],
        "ServiceRole": "EMR_DefaultRole",
        "JobFlowRole": "EMR_EC2_DefaultRole",
        "ReleaseLabel": "emr-6.8.0",
        "Applications": [
          {"Name": "Hive"},
          {"Name": "Hadoop"},
          {"Name": "JupyterEnterpriseGateway"},
          {"Name": "JupyterHub"},
          {"Name": "Hue"},
          {"Name": "Pig"},
          {"Name": "Livy"},
          {"Name": "Spark"}
        ],
        "LogUri": "s3://tarn-datalake-logs-433046906551",
        "VisibleToAllUsers": true,
        "Instances": {
          "KeepJobFlowAliveWhenNoSteps": true,
          "InstanceFleets": [
            {
              "InstanceFleetType": "MASTER",
              "Name": "Master",
              "TargetOnDemandCapacity": 1,
              "InstanceTypeConfigs": [
                {
                  "InstanceType": "m5d.xlarge"
                }
              ]
            },
            {
              "InstanceFleetType": "CORE",
              "Name": "Core",
              "TargetOnDemandCapacity": 2,
              "InstanceTypeConfigs": [
                {
                  "InstanceType": "m5d.xlarge"
                }
              ]
            }
          ]
        }
      },
      "ResultPath": "$.CreateClusterResult",
      "Next": "Run_ETL"
    },
    "Run_ETL":{
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:addStep.sync",
      "Parameters": {
        "ClusterId.$": "$.CreateClusterResult.ClusterId",
        "Step": {
          "Name": "SparkJob-desafio-mod1",
          "ActionOnFailure": "TERMINATE_CLUSTER",
          "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
              "spark-submit",
              "--master", "yarn",
              "--deploy-mode", "cluster",
              "s3://tarn-datalake-code-433046906551/code/pyspark/parquet/pipeline_ingestion_job_parquet.py"
            ]
          }
        }
      },
      "ResultPath": "$.sparkResult",
      "Next":"Terminate_Cluster"
    },
    "Terminate_Cluster": {
      "Type": "Task",
      "Resource": "arn:aws:states:::elasticmapreduce:terminateCluster.sync",
      "Parameters": {
        "ClusterId.$": "$.CreateClusterResult.ClusterId"
    },
    "ResultPath": "$.sparkResult",
    "Next":"StartCrawler"
    },
    "StartCrawler": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
      "Parameters": {
        "Name": "tarn-database-crawler"
      },
      "End": true
    }
  }
  }  
EOF
}