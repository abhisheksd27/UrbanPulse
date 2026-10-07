output "lake_bucket_id"       { value = aws_s3_bucket.lake.id }
output "lake_bucket_arn"      { value = aws_s3_bucket.lake.arn }
output "artifacts_bucket_id"  { value = aws_s3_bucket.artifacts.id }
output "artifacts_bucket_arn" { value = aws_s3_bucket.artifacts.arn }