resource "aws_glue_crawler" "tarn_database_crawler" {
  name          = "tarn-database-crawler"
#   database_name = aws_glue_catalog_database.tarn_database_crawler.name
  database_name = "tarn-database-crawler"
  #   role          = aws_iam_role.example.arn
  role = "arn:aws:iam::433046906551:role/service-role/AWSGlueServiceRole-tarn-crw"

  s3_target {
    path = "${var.prefix_name}-${var.bucket_names[2]}-${var.account_id}/parquet/RAIS2020/"
  }
}