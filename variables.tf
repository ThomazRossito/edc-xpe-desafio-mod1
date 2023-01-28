variable "location" {
  description = "variavel que indica a regiao dos recursos"
  type        = string
  default     = "us-east-1"
}

variable "account_id" {
  description = "identifica o account id"
  type        = string
  default     = "433046906551"
}

variable "prefix_name" {
  description = "prefix abreviatura meu nome"
  default     = "tarn"
}

variable "prefix_code" {
  description = "prefix bucket contem codigos"
  default     = "code"
}

variable "local_file" {
  description = "Diretório dos arquivos de role"
  default     = "./permissions"
}

variable "bucket_names" {
  description = "s3 bucket names"
  type        = list(string)
  default = [
    "datalake-code",
    "datalake-raw",
    "datalake-bronze",
    "datalake-logs"
    # "tarn-datalake-querys-athena"
  ]
}