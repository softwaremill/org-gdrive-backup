# Google Workspace Drives Backupper

This project is a simple Python script that backs up all Google Drives in a Google Workspace domain. It uses domain-wide delegation to impersonate a super admin account and access all drives.

## Features

- Backups all Google Drives in a Google Workspace domain (personal and shared drives) to S3
- Converts Google Apps files (Docs, Sheets, Slides) to Microsoft Office format
- Saves metadata about the files to a JSON file (`files.json`) (id, name, md5Checksum, path, permissions)
- Handles duplicate files (same name, path) by appending file ID to the name
- Links are converted to .txt files with path to the original file
- Whitelist & blacklist of drives
- Multi-process (per drive) and multi-threaded (per file) downloading
- `pigz` or `lz4` compression of the exported drives

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

| Name                     | Required | Purpose                                                                                                                              | Type   | Default                    |
| ------------------------ | -------- | ------------------------------------------------------------------------------------------------------------------------------------ | ------ | -------------------------- |
| `DELEGATED_ADMIN_EMAIL`  | Yes      | E-mail address of the superadmin account                                                                                             | string |                            |
| `WORKSPACE_CUSTOMER_ID`  | Yes      | Customer ID from Google Admin Console                                                                                                | string |                            |
| `SERVICE_ACCOUNT_FILE`   | Perhaps  | Path to service account .json key. If file won't exist, `SERVICE_ACCOUNT_JSON` will be used to create the file                       | string | `service-account-key.json` |
| `SERVICE_ACCOUNT_JSON`   | Perhaps  | Service account JSON file **encoded in BASE64**                                                                                      | string |                            |
| `S3_BUCKET_NAME`         | Yes      | Name of the S3 bucket to upload the backup to                                                                                        | string |                            |
| `S3_ROLE_BASED_ACCESS`   | Perhaps  | Use role-based access to S3 bucket. If enabled, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are not required                     | bool   | `false`                    |
| `AWS_ACCESS_KEY_ID`      | Perhaps  | AWS Access Key ID                                                                                                                    | string |                            |
| `AWS_SECRET_ACCESS_KEY`  | Perhaps  | AWS Secret Access Key                                                                                                                | string |                            |
| `MAX_DOWNLOAD_THREADS`   | No       | How many threads (**per single drive**) are used to download files                                                                   | int    | `20`                       |
| `MAX_DRIVE_PROCESSES`    | No       | Each drive gets it's own process. This specifies how many drives can be handled concurrently.                                        | int    | `4`                        |
| `COMPRESS_DRIVES`        | No       | Compress the exported drives to a .zip file                                                                                          | bool   | `false`                    |
| `COMPRESSION_PROCESSES`  | No       | How many processes are used to compress the drives (if supported by algorithm)                                                       | int    | `cpu_count()`              |
| `DRIVE_WHITELIST`        | No       | Comma-separated list of drive IDs to backup (e.g. `user@domain.tld,0AE1OlXvu8lCKUk9PVA`)                                             | string |                            |
| `DRIVE_BLACKLSIT`        | No       | Comma-separated list of drive IDs to exclude from backup (same as `DRIVE_WHITELIST`).                                                | string |                            |
| `AUTO_CLEANUP`           | No       | Automatically delete the files after the backup is complete                                                                          | bool   | `true`                     |
| `INCLUDE_SHARED_WITH_ME` | No       | Include 'shared with me' files. Applies to user drives only.                                                                         | bool   | `true`                     |
| `JIT_S3_UPLOAD`          | No       | Upload files to S3 as soon as they are downloaded. Useful when local disk space is limited. `COMPRESS_DRIVES` must be set to `False` | bool   | `false`                    |

# Roadmap

- [x] Drive compression
- [x] Configurable algorithm for file compression
- [ ] Configurable metadata fields
- [ ] Configurable links behaviour
- [x] AWS S3 role-based access
- [x] Drive whitelist
- [x] Drive blacklist

# Good to know

- Files without `md5Checksum` are are non-binary files (e.g. Folders, Google Docs, Sheets, Slides, Forms, etc.)
- If a file (or a folder) is shared with multiple users and `INCLUDE_SHARED_WITH_ME` is enabled, it will be downloaded multiple times (once per user)
- Requires `MAX_DRIVE_PROCESSES` \* largest Google Drive size in GB of free disk space
- `COMPRESS_DRIVES` doubles the disk space requirements
- If short on disk space, enable `JIT_S3_UPLOAD` to upload files to S3 as soon as they are downloaded. At most `MAX_DOWNLOAD_THREADS` \* `MAX_DRIVE_PROCESSES` files will be stored locally at any given time.
