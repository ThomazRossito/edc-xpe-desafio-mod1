data "aws_iam_policy" "AWSStepFunctionsFullAccess" {
  arn = "arn:aws:iam::aws:policy/AWSStepFunctionsFullAccess"
}
data "aws_iam_policy" "CloudWatchLogsFullAccess" {
  arn = "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
}
data "aws_iam_policy" "AWSLambdaRole" {
  arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaRole"
}
data "aws_iam_policy" "EMRRunJobFlowManagementScopedAccessPolicy" {
  arn = "arn:aws:iam::${var.account_id}:policy/service-role/ElasticMapReduceRunJobFlowManagementScopedAccessPolicy-e0978da6-15ff-49d1-9f21-da5bf57bc357"
}
data "aws_iam_policy" "ElasticMapReduceAddJobFlowStepsManagementFullAccess" {
  arn = "arn:aws:iam::${var.account_id}:policy/service-role/ElasticMapReduceAddJobFlowStepsManagementFullAccess-787682d9-dae7-4225-bd9b-2dede6b4fccd"
}
data "aws_iam_policy" "ElasticMapReduceTerminateJobFlowsManagementFullAccessPolicy" {
  arn = "arn:aws:iam::${var.account_id}:policy/service-role/ElasticMapReduceTerminateJobFlowsManagementFullAccessPolicy-6352a2c4-0f33-4ac6-9cd7-0b1d345a541c"
}
data "aws_iam_policy" "XRayAccessPolicy" {
  arn = "arn:aws:iam::${var.account_id}:policy/service-role/XRayAccessPolicy-feea7911-1622-45ee-aa83-d4bcf8b31242"
}


resource "aws_iam_role" "StepFunctionTarnRole" {
  name               = "StepFunctionRole"
  assume_role_policy = file("${var.local_file}/state-assume-policy.json")
}
resource "aws_iam_role_policy_attachment" "step_function_attach_policy_awsstepfunctionfullaccess" {
  role       = aws_iam_role.StepFunctionTarnRole.name
  policy_arn = data.aws_iam_policy.AWSStepFunctionsFullAccess.arn
}
resource "aws_iam_role_policy_attachment" "step_function_attach_policy_cloudwatchlogsfullaccess" {
  role       = aws_iam_role.StepFunctionTarnRole.name
  policy_arn = data.aws_iam_policy.CloudWatchLogsFullAccess.arn
}
resource "aws_iam_role_policy_attachment" "step_function_attach_policy_awslambdarole" {
  role       = aws_iam_role.StepFunctionTarnRole.name
  policy_arn = data.aws_iam_policy.AWSLambdaRole.arn
}
resource "aws_iam_role_policy_attachment" "EMR_RunJob_Flow_Management_Scoped_Access_Policy" {
  role       = aws_iam_role.StepFunctionTarnRole.name
  policy_arn = data.aws_iam_policy.EMRRunJobFlowManagementScopedAccessPolicy.arn
}
resource "aws_iam_role_policy_attachment" "EMR_Add_JobFlow_StepsManagement_FullAccess" {
  role       = aws_iam_role.StepFunctionTarnRole.name
  policy_arn = data.aws_iam_policy.ElasticMapReduceAddJobFlowStepsManagementFullAccess.arn
}
resource "aws_iam_role_policy_attachment" "EMR_Terminate_JobFlows_Management_FullAccess_Policy" {
  role       = aws_iam_role.StepFunctionTarnRole.name
  policy_arn = data.aws_iam_policy.ElasticMapReduceTerminateJobFlowsManagementFullAccessPolicy.arn
}
resource "aws_iam_role_policy_attachment" "XRay_Access_Policy" {
  role       = aws_iam_role.StepFunctionTarnRole.name
  policy_arn = data.aws_iam_policy.XRayAccessPolicy.arn
}

resource "aws_iam_role" "glue_job" {
  name               = "tarn-glue-crawler-job-role"
  path               = "/"
  description        = "Provides write permissions to CloudWatch Logs and S3 Full Access"
  assume_role_policy = file("./permissions/Role_GlueJobs.json")
}

resource "aws_iam_policy" "glue_job" {
  name        = "tarn-glue-crawler-job-policy"
  path        = "/"
  description = "Provides write permissions to CloudWatch Logs and S3 Full Access"
  policy      = file("./permissions/Policy_GlueJobs.json")
}

resource "aws_iam_role_policy_attachment" "glue_job" {
  role       = aws_iam_role.glue_job.name
  policy_arn = aws_iam_policy.glue_job.arn
}