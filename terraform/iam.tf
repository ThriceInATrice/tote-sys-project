resource "aws_iam_role" "lambda_role" {
  name_prefix = "extract-lambda-role-"
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


data "aws_iam_policy_document" "s3_document" {
    statement {
      actions = ["s3:PutObject"]
      resources = ["arn:aws:s3:::ingestion-bucket-20241111133940921900000001/*"]
    }
}

data "aws_iam_policy_document" "cw_document" {
    statement {
      actions = ["logs:CreateLogGroup"]
      resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_region.current.id}:*"]
      }
    statement {
    effect = "allow"
    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:/aws/lambda/${var.extract_lambda}:*"]
  }
}



resource "aws_iam_policy" "s3_policy" {
  name_prefix = "s3-ingestion-lambda-policy"
  policy = data.aws_iam_policy_document.s3_document.json
}

resource "aws_iam_policy" "cw_policy" {
  name_prefix = "cw-ingestion-lambda-policy"
  policy = data.aws_iam_policy_document.cw_document.json
}


resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
  role = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
  role = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
}