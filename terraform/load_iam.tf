resource "aws_iam_role" "load_lambda_role" {
  name_prefix        = "load-lambda-role-"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",

        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

data "aws_iam_policy_document" "load_s3_document" {
  statement {
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["arn:aws:s3:::transformed-data-20241120113916041500000001/*"]
  }
}

data "aws_iam_policy_document" "load_transformation_times_document" {
  statement {
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["arn:aws:s3:::transformation-times-20241120113916041500000002/*"]
  }
}

data "aws_iam_policy_document" "load_processed_extractions_document" {
  statement {
    actions   = ["s3:PutObject"]
    resources = ["arn:aws:s3:::loaded-extractions-20241127143342803700000001/*"]
  }
}

data "aws_iam_policy_document" "load_cw_document" {
  statement {
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup"]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/*"]
  }
  statement {
    effect    = "Allow"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/*"]
  }
}

resource "aws_iam_policy" "load_s3_policy" {
  name_prefix = "s3-load-lambda-policy-"
  policy      = data.aws_iam_policy_document.load_s3_document.json
}

resource "aws_iam_policy" "load_transform_times_policy" {
  name_prefix = "s3-transformation-times-lambda-policy-"
  policy      = data.aws_iam_policy_document.load_transformation_times_document.json
}

resource "aws_iam_policy" "load_cw_policy" {
  name_prefix = "cw-load-lambda-policy-"
  policy      = data.aws_iam_policy_document.cw_document.json
}

resource "aws_iam_policy" "load_processed_extractions_policy" {
  name_prefix = "cw-load-processed-extractions-policy"
  policy      = data.aws_iam_policy_document.load_processed_extractions_document.json
}

resource "aws_iam_role_policy_attachment" "load_lambda_s3_policy_attachment" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.load_s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "load_lambda_transformation_times_policy_attachment" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.load_transform_times_policy.arn
}

resource "aws_iam_role_policy_attachment" "load_lambda_cw_policy_attachment" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.load_cw_policy.arn
}

resource "aws_iam_policy" "load_lambda_secrets_manager" {
  name = "load_lambda_secrets_manager"
  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "secretsmanager:GetSecretValue"
        ],
        # "Resource" : "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:totesys-db-creds-*"
        "Resource" : "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "load_lambda_secret_attachment" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.load_lambda_secrets_manager.arn
}

resource "aws_iam_role_policy_attachment" "load_lambda_processed_extractions_bucket_attachment" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.load_processed_extractions_policy.arn
}

# role for load lambda
# get object from transformed data buckets
# get object, put object from transformation times bucket
# secrets manager access added 

