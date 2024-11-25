resource "aws_iam_role" "transform_lambda_role" {
  name_prefix        = "transform-lambda-role-"
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

data "aws_iam_policy_document" "transform_ingestion_s3_document" {
  statement {
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["arn:aws:s3:::ingestion-bucket-20241111133940921900000001/*"]
  }
}

resource "aws_iam_policy" "transform_ingestion_s3_policy" {
  name_prefix = "ingestion-s3-policy"
  policy      = data.aws_iam_policy_document.transform_ingestion_s3_document.json
}

resource "aws_iam_role_policy_attachment" "transform_ingestion_s3_attachment" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.transform_ingestion_s3_policy.arn
}


data "aws_iam_policy_document" "transform_cw_document" {
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

resource "aws_iam_policy" "transform_cw_policy" {
  name_prefix = "cw-transform-lambda-policy-"
  policy      = data.aws_iam_policy_document.transform_cw_document.json
}

resource "aws_iam_role_policy_attachment" "transform_cw_attachment" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.transform_cw_policy.arn
}


data "aws_iam_policy_document" "transform_times_document" {
  statement {
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["arn:aws:s3:::transformation-times-20241120113916041500000002/*", "arn:aws:s3:::transformation-times-20241120113916041500000002/"]
  }
}

resource "aws_iam_policy" "transform_times_policy" {
  name_prefix = "s3-transform-times-lambda-policy-"
  policy      = data.aws_iam_policy_document.transform_times_document.json
}

resource "aws_iam_role_policy_attachment" "transform_times_attachment" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.transform_times_policy.arn
}

resource "aws_iam_policy" "transform_lambda_secrets_manager" {
  name = "transform_lambda_secrets_manager"
  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "secretsmanager:GetSecretValue", "secretsmanager:ListSecrets"
        ],
        # "Resource" : "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:totesys-db-creds-*"
        "Resource" : "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "transform_secret_attachment" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.transform_lambda_secrets_manager.arn
}

data "aws_iam_policy_document" "s3_transform_extraction_times_document" {
  statement {
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["arn:aws:s3:::extraction-times-20241111134946737900000001/*", "arn:aws:s3:::transformed-data-20241120113916041500000001/*"]
  }
}

resource "aws_iam_policy" "transform_extraction_times_policy" {
  name_prefix = "s3-extraction-times-lambda-policy"
  policy      = data.aws_iam_policy_document.s3_transform_extraction_times_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_transform_extraction_policy_attachment" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.transform_extraction_times_policy.arn
}