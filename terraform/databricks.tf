provider "databricks" {
  host  = var.DATABRICKS_HOST
  token = var.DATABRICKS_EDL_TOKEN
}

resource "databricks_job" "eaw_testing_job" {
  name                      = "eaw_testing_${var.ENV}"
  max_concurrent_runs       = 1
  max_retries               = 0
  min_retry_interval_millis = 900000

  schedule {
    pause_status           = var.jobEnabled
    quartz_cron_expression = "42 18 0 * * ?"
    timezone_id            = "US/Central"
  }

  dynamic "library" {
    for_each = ["timezonefinder","shapely"]
    content {
      pypi {
        package = library.value
      }
    }
  }

  new_cluster {
    policy_id = "9C60384B6B000588"
    custom_tags = {
      "component" = "databricks_eaw"
      "jobName" = "eaw_testing_${var.ENV}"
    }

    enable_elastic_disk          = true
    enable_local_disk_encryption = false
    node_type_id                 = "z1d.6xlarge"
    num_workers                  = 0
    spark_conf                   = {
      "fs.s3a.server-side-encryption-algorithm" = "AES256",
      "spark.driver.maxResultSize"              = 0,
      "spark.databricks.delta.retentionDurationCheck.enabled" = "false"
    }
    spark_env_vars               = {}
    spark_version                = var.spark_version
    ssh_public_keys              = []

    autoscale {
      max_workers = 20
      min_workers = 3
    }

    aws_attributes {
      availability           = "SPOT_WITH_FALLBACK"
      ebs_volume_count       = 1
      ebs_volume_size        = 100
      first_on_demand        = 1
      instance_profile_arn   = "arn:aws:iam::844652101329:instance-profile/collibra/AWS-EDL-DATABRICKS-PROD-EAW"
      spot_bid_price_percent = 110
      zone_id                = "auto"
    }
  }

  notebook_task {
      notebook_path = "src/drake_testing"
  }

  git_source {
    url = "https://github.deere.com/Engineering-Analytics-Warehouse/eaw-databricks-pipeline-jobs.git"
    provider = "gitHubEnterprise"
//    branch = var.git_branch
    branch = var.branch_name
  }
}

resource "databricks_permissions" "eaw_testing_permission" {
    job_id = databricks_job.eaw_testing_job.id

    access_control {
        group_name = "AWS-EDL-DATABRICKS-PROD-EAW"
        permission_level = "CAN_MANAGE"
    }
}