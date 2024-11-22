# Terraform Project Setup Guide

This repository contains Terraform configurations to provision resources in Azure. Follow this guide to set up your environment and start using the code.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Project Structure](#project-structure)
- [Usage](#usage)

## Prerequisites

Before you begin, ensure you have the following installed:

1. **Git**:
   - Install Git from [Git's official website](https://git-scm.com/downloads).
   - Verify the installation:
     ```bash
     git --version
     ```

2. **Terraform**:
   - Install Terraform from [Terraform's official website](https://www.terraform.io/downloads.html).
   - Verify the installation:
     ```bash
     terraform -version
     ```

3. **Azure CLI**:
   - Install Azure CLI from [Azure CLI installation guide](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli).
   - Verify the installation:
     ```bash
     az version
     ```
   - Log in to Azure:
     ```bash
     az login
     ```

4. **Integrated Development Environment (IDE)**:
   - **Visual Studio Code (VS Code)** is recommended. You can download it from [VS Code official website](https://code.visualstudio.com/).
   - Install the **Terraform Extension** for syntax highlighting and IntelliSense support:
     - In VS Code, go to **Extensions** and search for `HashiCorp Terraform`.

## Installation

### 1. Clone the Repository
Clone this Git repository to your local machine using Git Bash or a terminal:
```bash
git clone [<remote-repo-URL>](https://github.com/aashil2495/Fitbit-Databricks-Lakehouse-Project.git)
cd Fitbit-Databricks-Lakehouse-Project.git

### 2. Configure Azure Authentication
Make sure you are logged in to your Azure account using Azure CLI:
```bash
az login

### 3. Initialize Terraform
Navigate to the folder containing your .tf files and initialize the Terraform environment:
```bash
terraform init
