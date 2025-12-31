variable "name"     { type = string }
variable "handler"  { type = string }
variable "runtime"  { type = string }
variable "zip_path" { type = string }
variable "env_vars" {
  type    = map(string)
  default = {}
}


