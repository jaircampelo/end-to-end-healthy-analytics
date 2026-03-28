# End-to-end Healthy Analytics

|Language/Culture|Doc|
|:--|:---|
|🇵🇹|README.md(atual)|
|🇺🇸|[README_pt-br.md](./README_pt-br.md)|

End-to-end project using PySUS library to ingest data from DATASUS to Microsoft Fabric trough Notebooks, and Git integration using pyFabricOps and GitHub Actions for CI/CD.

![architecture](support/architecture.png)

## Goals

This project was develop in order to enhance my skills with Analytics Engineer, covering subjects like: Medallion Architecture, CI/CD, Git integration, Python and PySpark languages, ETL/ELT, Data Visualization, Data Modeling, Data Orchestration using Pipelines and Notebooks, working with Delta Tables, metadata monitoring, and why not, some english for documentation purposes.

## Project Overview

The result of this end-to-end project is a report about Natural Population Growth, which is the difference between the birth ans death rates. Futhermore, this data is cross-referenced with healthcare coverage in each region, enhancing the decision-making capacity os institutional authorities and/or the private sector.

The data used was obtained using python, from a public library called PySUS, which requires the data from DATASUS - a database of Unified Health System (SUS) - and delivers in pandas dataframe or parquet files.

Git integration was set between my local machine, GitHub repository, and Microsoft Fabric using multiple sources:

- pyFabricOps library: a **Python wrapper library for Microsoft Fabric (and Power BI) operations, providing a simple interface to the official Fabric REST APIs. Falls back to Power BI REST APIs where needed. Designed to run in Python notebooks, pure Python scripts or integrated into YAML-based workflows for CI/CD** and can be accessed on [GitHub](https://github.com/alisonpezzott/pyfabricops)
- GitHub Actions: a continuous integration and continuous delivery (CI/CD) platform that allows you to automate your build, test, and deployment pipeline, creating workflows that build and test every pull request to your repository, or deploy merged pull requests to production.
- Microsoft Fabric Git integration: enables developers to integrate their development processes, tools, and best practices straight into the Fabric platform and allows developers to collaborate with others or working alone using Git branches.

Inside Microsoft Fabric, Notebooks were used for Extraction, Load and data Transformations (ELT) and the data extracted was stored at a single Lakehouse.

For this project, the data orchestration defined following the Medallion Architecture model, which consists in split the path that the data travels in stages, commonly called Bronze, Silver and Gold stages, but in this project we added one more stage at the beginning:

| Stage | Description |
|:---|:---|
| **Landing** | Where the data is extracted as-is from DATASUS and stored at Lakehouse in parquet files |
| **Bronze** | The data inside the files extracted are converted as-is to Delta Tables using Notebooks |
| **Silver** | Applies nomenclature best practices, treat nulls, changes schemas and do other transformations |
| **Gold** | Create Dimensional Model and aggregate data in order to reduce the size of the model for better performance and answer bussiness questions |

## Tech stack:

- Microsoft Fabric
- PySpark
- Python
- PySUS and pyFabricOps libraries
- GitHub
- GitHub Actions
- VS Code
- Figma
- Power BI
- Direct Lake Model
  
## License

[MIT](LICENSE) © 2026 Jair Campelo