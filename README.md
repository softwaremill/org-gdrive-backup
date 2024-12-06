# Google Workspace Drives Backupper

This project is a simple Python script that backs up all Google Drives in a Google Workspace domain. It uses domain-wide delegation to impersonate a super admin account and access all drives.

## Features

- Backup all Google Drives in a Google Workspace domain (personal and shared drives)
- Converts Google Apps files (Docs, Sheets, Slides) to Microsoft Office format
- Exports all files to a local directory
- Saves metadata about the files to a JSON file (currently md5Checksum)
- Links are converted to .txt files with path to the original file

# Configuration

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

# Roadmap

- [ ] (Configurable) compression
- [ ] Configurable export formats
- [ ] Configurable metadata fields
- [ ] Configurable links behaviour

# Good to know

- Files without `md5Checksum` are are non-binary files (e.g. Folders, Google Docs, Sheets, Slides, Forms, etc.)