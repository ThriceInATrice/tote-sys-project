variable "extract_lambda" {
  type    = string
  default = "extract"
}

variable "python_runtime" {
  type    = string
  default = "python3.11"
}

variable "transform_lambda" {
  type    = string
  default = "transform"
}

variable "load_lambda" {
  type    = string
  default = "load"
}