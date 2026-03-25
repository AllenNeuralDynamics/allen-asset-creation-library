# aind-automation-capsule-library


[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
![Code Style](https://img.shields.io/badge/code%20style-black-black)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)
![Interrogate](https://img.shields.io/badge/interrogate-100.0%25-brightgreen)
![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)
![Python](https://img.shields.io/badge/python->=3.10-blue?logo=python)

## Getting Started

This is a template capsule that pipeline maintainers can use to automatically
capture results as a data asset when a pipeline run is finished.

### Usage

Use this template repo to create your own capsule. You may want to customize 
the following methods in the core.job.CaptureResultsJob:

- `_capture_results`: if you want to customize the mount, tags, or 
permissions, they are configured here
- `_send_notification`: as default, the template capsule simply logs an error 
message, but this can be modified to send an alert, etc.
- Please add a .env file in the code folder and set the following environment 
variables:
  - DESTINATION_BUCKET
  - DOCDB_HOST
  - CODEOCEAN_DOMAIN
  - ASSET_PERMISSIONS
- Once the capsule is created and configured, it can now be used when a 
pipeline finishes by going to the pipeline UI settings and clicking the 
Automation tab
- When modifying the capsule, please don't log tokens, secrets, or 
credentials. Since Code Ocean stores some of this info in env vars, please 
don't log the full set of environment variables either.
- Currently, the only information passed into the Automation Capsule is the
source pipeline id and source pipeline exit code. These are set by Code Ocean
as environment variables.

### Requirements

- The pipeline maintainer needs the codeocean-power-user role
- The pipeline maintainer should set their Code Ocean Token as a secret
- If the pipeline maintainer wants to automatically trigger their pipeline 
via the transfer service, then please reach out to Scientific Computing


### Local Development

- Make sure the pyproject.toml dependency list is synced with the Dockerfile.
- Create a conda environment. From the code folder, run 
`pip install -e . --group dev`
- For testing, run `coverage run -m unittest discover && coverage report`

### Level of Support
 - [x] Supported:
We are releasing this code to the public as a tool we expect others to use. 
Issues are welcomed, and we expect to address them promptly; pull requests 
will be vetted by our staff before inclusion.
