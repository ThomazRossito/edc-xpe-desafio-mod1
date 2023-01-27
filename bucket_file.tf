resource "aws_s3_object" "s3_bucket_object_codes" {
  for_each = fileset("./code/pyspark/", "**")
  bucket   = "${var.prefix_name}-${var.bucket_names[0]}-${var.account_id}"
  key      = "code/pyspark/${each.value}"
  source   = "./code/pyspark/${each.value}"
  etag     = filemd5("./code/pyspark/${each.value}")

  depends_on = [
    aws_s3_bucket.buckets
  ]
}


# resource "aws_s3_object" "s3_bucket_object_spark" {
#   bucket = "datalake-tarn-code-433046906551"
#   key    = "emr-code/pyspark/job_csv_to_parquet.py"
#   acl    = "private"
#   source = "./code/job_spark.py"
#   etag   = filemd5("./code/job_spark.py")

#   depends_on = [
#     aws_s3_bucket.buckets
#   ]
# }