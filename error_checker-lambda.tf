# AWS Lambda function
resource "aws_lambda_function" "aws_lambda_error_checker" {
  filename         = "error_checker.zip"
  function_name    = "${var.prefix}-error-checker"
  role             = aws_iam_role.aws_lambda_execution_role.arn
  handler          = "error_checker.error_checker_handler"
  runtime          = "python3.12"
  source_code_hash = filebase64sha256("error_checker.zip")
  timeout          = 600
  memory_size      = 1024
  vpc_config {
    subnet_ids         = data.aws_subnets.private_application_subnets.ids
    security_group_ids = data.aws_security_groups.vpc_default_sg.ids
  }
  file_system_config {
    arn              = data.aws_efs_access_point.fsap_error_checker.arn
    local_mount_path = "/mnt/data"
  }
}

# AWS Lambda role and policy
resource "aws_iam_role" "aws_lambda_execution_role" {
  name = "${var.prefix}-lambda-error-checker-execution-role"
  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "lambda.amazonaws.com"
        },
        "Action" : "sts:AssumeRole"
      }
    ]
  })
  permissions_boundary = "arn:aws:iam::${local.account_id}:policy/NGAPShRoleBoundary"
}

resource "aws_iam_role_policy_attachment" "aws_lambda_execution_role_policy_attach" {
  role       = aws_iam_role.aws_lambda_execution_role.name
  policy_arn = aws_iam_policy.aws_lambda_execution_policy.arn
}

resource "aws_iam_policy" "aws_lambda_execution_policy" {
  name        = "${var.prefix}-lambda-error-checker-execution-policy"
  description = "Write to CloudWatch logs, list and delete from S3, publish to SQS."
  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Sid" : "AllowCreatePutLogs",
        "Effect" : "Allow",
        "Action" : [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        "Resource" : "arn:aws:logs:*:*:*"
      },
      {
        "Sid" : "AllowVPCAccess",
        "Effect" : "Allow",
        "Action" : [
          "ec2:CreateNetworkInterface"
        ],
        "Resource" : concat([for subnet in data.aws_subnet.private_application_subnet : subnet.arn], ["arn:aws:ec2:${var.aws_region}:${local.account_id}:*/*"])
      },
      {
        "Sid" : "AllowVPCDelete",
        "Effect" : "Allow",
        "Action" : [
          "ec2:DeleteNetworkInterface"
        ],
        "Resource" : "arn:aws:ec2:${var.aws_region}:${local.account_id}:*/*"
      },
      {
        "Sid" : "AllowVPCDescribe",
        "Effect" : "Allow",
        "Action" : [
          "ec2:DescribeNetworkInterfaces"
        ],
        "Resource" : "*"
      },
      {
        "Sid" : "AllowEFSAccess",
        "Effect" : "Allow",
        "Action" : [
          "elasticfilesystem:ClientMount",
          "elasticfilesystem:ClientWrite",
          "elasticfilesystem:DescribeMountTargets"
        ],
        "Resource" : "${data.aws_efs_access_point.fsap_error_checker.file_system_arn}"
        "Condition" : {
          "StringEquals" : {
            "elasticfilesystem:AccessPointArn" : "${data.aws_efs_access_point.fsap_error_checker.arn}"
          }
        }
      },
      {
        "Sid" : "AllowListBucket",
        "Effect" : "Allow",
        "Action" : [
          "s3:ListBucket"
        ],
        "Resource" : "${data.aws_s3_bucket.generate_data.arn}"
      },
      {
        "Sid" : "AllowPutObject",
        "Effect" : "Allow",
        "Action" : [
          "s3:PutObject"
        ],
        "Resource" : "${data.aws_s3_bucket.generate_data.arn}/*"
      },
      {
        "Sid" : "AllowListTopics",
        "Effect" : "Allow",
        "Action" : [
          "sns:ListTopics"
        ],
        "Resource" : "*"
      },
      {
        "Sid" : "AllowPublishTopic",
        "Effect" : "Allow",
        "Action" : [
          "sns:Publish"
        ],
        "Resource" : "${data.aws_sns_topic.batch_failure_topic.arn}"
      },
      {
        "Sid" : "AllowSendMessage",
        "Effect" : "Allow",
        "Action" : [
          "sqs:SendMessage"
        ],
        "Resource" : [
          "${data.aws_sqs_queue.pending_jobs_aqua.arn}",
          "${data.aws_sqs_queue.pending_jobs_terra.arn}",
          "${data.aws_sqs_queue.pending_jobs_viirs.arn}"
        ]
      }
    ]
  })
}

# EventBridge schedule
resource "aws_scheduler_schedule" "aws_schedule_error_checker" {
  name       = "${var.prefix}-error-checker"
  group_name = "default"
  flexible_time_window {
    mode = "OFF"
  }
  schedule_expression = "cron(55 * * * ? *)"
  target {
    arn      = aws_lambda_function.aws_lambda_error_checker.arn
    role_arn = aws_iam_role.aws_eventbridge_error_checker_execution_role.arn
    input = jsonencode({
      "prefix" : "${var.prefix}",
      "account" : "${local.account_id}",
      "region" : "${var.aws_region}"
    })
  }
}

# EventBridge execution role and policy
resource "aws_iam_role" "aws_eventbridge_error_checker_execution_role" {
  name = "${var.prefix}-eventbridge-error-checker-execution-role"
  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "scheduler.amazonaws.com"
        },
        "Action" : "sts:AssumeRole"
      }
    ]
  })
  permissions_boundary = "arn:aws:iam::${local.account_id}:policy/NGAPShRoleBoundary"
}

resource "aws_iam_role_policy_attachment" "aws_eventbridge_error_checker_execution_role_policy_attach" {
  role       = aws_iam_role.aws_eventbridge_error_checker_execution_role.name
  policy_arn = aws_iam_policy.aws_eventbridge_error_checker_execution_policy.arn
}

resource "aws_iam_policy" "aws_eventbridge_error_checker_execution_policy" {
  name        = "${var.prefix}-eventbridge-error-checker-execution-policy"
  description = "Allow EventBridge to invoke a Lambda function."
  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Sid" : "AllowInvokeLambda",
        "Effect" : "Allow",
        "Action" : [
          "lambda:InvokeFunction"
        ],
        "Resource" : "${aws_lambda_function.aws_lambda_error_checker.arn}"
      }
    ]
  })
}