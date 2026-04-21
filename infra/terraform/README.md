# Terraform Usage

From the repository root, copy the example variables file and edit values (PowerShell):

```powershell
Copy-Item infra/terraform/terraform.tfvars.example infra/terraform/terraform.tfvars
```

Then run Terraform from the repository root:

```powershell
terraform -chdir=infra/terraform init
terraform -chdir=infra/terraform plan -var-file=terraform.tfvars
terraform -chdir=infra/terraform apply -var-file=terraform.tfvars
```
