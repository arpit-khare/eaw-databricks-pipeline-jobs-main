variable "ENV" {
  type = string
}
variable "jobEnabled" {
  type = string
}

variable "spark_version" {
  type = string
}

variable "DATABRICKS_HOST" {
  type = string
}
variable "DATABRICKS_EDL_TOKEN" {
  type = string
}

variable "branch_name" {
  description = "The name of the branch that's being deployed"
}