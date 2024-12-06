# Google Workspace Drives Backupper

This project is a simple Python script that backs up all Google Drives in a Google Workspace domain. It uses domain-wide delegation to impersonate a super admin account and access all drives.

## Features

- Backup all Google Drives in a Google Workspace domain (personal and shared drives)
- Converts Google Apps files (Docs, Sheets, Slides) to Microsoft Office format
- Exports all files to a local directory
- Saves metadata about the files to a JSON file (currently md5Checksum)
- Links are converted to .txt files with path to the original file

# Usage

## GCP Project

1. Create a new project in the [Google Cloud Console](https://console.cloud.google.com/) and enable the [Google Drive API](https://console.cloud.google.com/marketplace/product/google/drive.googleapis.com), as well as [Admin SDK API](https://console.cloud.google.com/marketplace/product/google/admin.googleapis.com)

2. (Optional) If your domain has a lot of users and files, you may need to request a quota increase for the Google Drive API. You can do this by going to the [quotas page](https://console.cloud.google.com/iam-admin/quotas).

3. Create a new service account in the [Google Cloud Console](https://console.cloud.google.com/iam-admin/serviceaccounts) and download the JSON key file. This key file will be used to authenticate the script. Note the Client ID of the service account (Unique ID in Console).

## Google Workspace

1. Go to the [Google Admin Console](https://admin.google.com) and navigate to `Security` -> `Access and data control` -> `API Controls`

2. Click on `Manage Domain Wide Delegation` and click on `Add new`

3. Enter the Client ID of the service account you created earlier and the following scopes:
   - `https://www.googleapis.com/auth/admin.directory.user.readonly`
   - `https://www.googleapis.com/auth/drive.readonly`

4. Obtain the `Customer ID` of your Google Workspace domain. This can be found in the Google Admin Console under `Account` -> `Account Settings`

## Enviroment variables

| Name                     | Required | Purpose                                                                    | Type   | Default                  |
|--------------------------|----------|----------------------------------------------------------------------------|--------|--------------------------|
| `DELEGATED_ADMIN_EMAIL`    | Yes      | E-mail address of the superadmin account                                   | string |                            |
| `WORKSPACE_CUSTOMER_ID`    | Yes      | Customer ID from Google Admin Console                                      | string |                            |
| `SERVICE_ACCOUNT_FILE`     | Yes      | Path to service account .json key                                          | string | `service-account-key.json` |
| `MAX_QUERY_THREADS`        | No       | Number of threads used to list files (always one thread per Google Drive)  | int    | `5`                        |
| `MAX_DOWNLOAD_PROCESSES`   | No       | Number of max processes used to download files from Google Drive           | int    | `cpu_count()`              |
| `FILES_PER_DOWNLOAD_BATCH` | No       | How many files should a process download                                   | int    | `1`                        |
| `COMPRESS_DRIVES`          | No       | Compress the exported drives to a .zip file                                 | bool   | `false`                   |


# Roadmap

- [x] File compression
- [ ] Configurable algorithm for file compression
- [ ] Configurable export formats
- [ ] Configurable metadata fields
- [ ] Configurable links behaviour
- [ ] Workload Identity support

# Good to know

- Files without `md5Checksum` are are non-binary files (e.g. Folders, Google Docs, Sheets, Slides, Forms, etc.)