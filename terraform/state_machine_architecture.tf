# resource "aws_sfn_state_machine" "sfn_state_machine" {
#     name     = "my-state-machine"
#     role_arn = "aws_iam_role.iam_for_sfn.arn" # IAMS roll
#     definition = <<EOF

#     {
#         "Comment": "A description of my state machine",
#         "StartAt": "get_new_data",
#         "States": {
#           "get_new_data": {
#             "Type": "Task",
#             "Resource": "arn:aws:states:::lambda:invoke",
#             "OutputPath": "$.Payload",
#             "Parameters": {
#               "Payload.$": "$",
#               "FunctionName": "arn:aws:lambda:eu-west-2:881490134104:function:ingestion_lambda:$LATEST" #change this to the fn we need
#             },
#             "Retry": [
#               {
#                 "ErrorEquals": [
#                   "Lambda.ServiceException",
#                   "Lambda.AWSLambdaException",
#                   "Lambda.SdkClientException",
#                   "Lambda.TooManyRequestsException"
#                 ],
#                 "IntervalSeconds": 1,
#                 "MaxAttempts": 3,
#                 "BackoffRate": 2,
#                 "JitterStrategy": "FULL"
#               }
#             ],
#             "Next": "PutObject"
#           },
#           "PutObject": {
#             "Type": "Task",
#             "Parameters": {
#               "Body": {},
#               "Bucket": "MyData",
#               "Key": "MyData"
#             },
#             "Resource": "arn:aws:states:::aws-sdk:s3:putObject", #name of bucket
#             "End": true
#           }
#         }
#     EOF
# }