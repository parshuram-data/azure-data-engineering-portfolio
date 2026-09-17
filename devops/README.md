# Azure DevOps CI/CD

This folder documents the CI/CD approach used for the Azure Retail Data Platform.

## Purpose

Azure DevOps can be used to manage source control, automated validation and deployment of Azure data engineering components.

## CI/CD Flow

```text
Developer
    |
    v
Git Repository
    |
    v
Pull Request
    |
    v
Validation
    |
    v
Build
    |
    v
Deploy to Development
    |
    v
Deploy to Test
    |
    v
Deploy to Production
