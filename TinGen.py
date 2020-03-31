# Copyright 2020 eXhumer

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from googleapiclient.discovery import build as google_api_build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google.auth.exceptions import TransportError
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.http import MediaFileUpload
from pathlib import Path
from CryptoHelpers import encrypt_file
from concurrent.futures.thread import ThreadPoolExecutor
import socket, json, argparse, urllib.parse, time, re, asyncio, tqdm

sem = None

class GDrive:
    def __init__(self, token_path, credentials_path):
        credentials = self._get_creds(credentials=str(credentials_path), token=str(token_path))
        self.drive_service = google_api_build("drive", "v3", credentials=credentials)

    def _cred_to_json(self, cred_to_pass):
        cred_json = {
            'access_token': cred_to_pass.token,
            'refresh_token': cred_to_pass.refresh_token
        }
        return cred_json

    def _json_to_cred(self, json_to_pass, client_id, client_secret):
        cred_json = json.load(json_to_pass)
        creds = Credentials(
            cred_json['access_token'],
            refresh_token=cred_json['refresh_token'],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret
        )
        return creds

    def _get_creds(self, credentials="credentials.json", token="gdrive.token", scopes=['https://www.googleapis.com/auth/drive']):
        if Path(credentials).is_file():
            with open(credentials, "r") as c:
                cred_json = json.load(c)
            creds = None
            if Path(token).is_file():
                with open(token, "r") as t:
                    creds = self._json_to_cred(t, cred_json["installed"]["client_id"], cred_json["installed"]["client_secret"])
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(credentials, scopes)
                    creds = flow.run_local_server(port=0)
                with open(token, "w") as t:
                    json.dump(self._cred_to_json(creds), t, indent=2)
            return creds

    def _apicall(self, request, maximum_backoff=32):
        sleep_exponent_count = 0
        while True:
            success = True
            retry = False
            try:
                response = request.execute()
            except HttpError as error:
                success = False
                try:
                    error_details = json.loads(error.content.decode("utf-8"))["error"]
                except json.decoder.JSONDecodeError as error:
                    retry = True
                else:
                    if "errors" in error_details:
                        if error_details["errors"][0]["reason"] in ("dailyLimitExceeded", "userRateLimitExceeded", "rateLimitExceeded", "backendError", "sharingRateLimitExceeded", "failedPrecondition", "internalError", "domainPolicy", "insufficientFilePermissions", "appNotAuthorizedToFile"): # IF REQUEST IS RETRYABLE
                            retry = True
                    else:
                        raise error
            except (TransportError, socket.error, socket.timeout) as error:
                success = False
                retry = True
            if success:
                break
            if retry:
                sleep_time = 2^sleep_exponent_count
                if sleep_time < maximum_backoff:
                    time.sleep(sleep_time)
                    sleep_exponent_count += 1
                    continue
                else:
                    raise Exception("Maximum Backoff Limit Exceeded.")
            else:
                raise Exception("Unretryable Error")
        return response

    def _ls(self, folder_id, fields="files(id,name,size,permissionIds),nextPageToken", searchTerms=""):
        files = []
        resp = {"nextPageToken": None}
        while "nextPageToken" in resp:
            resp = self._apicall(self.drive_service.files().list(
                q = " and ".join(["\"%s\" in parents" % folder_id] + [searchTerms] + ["trashed = false"]),
                fields = fields,
                pageSize = 1000,
                supportsAllDrives = True,
                includeItemsFromAllDrives = True,
                pageToken = resp["nextPageToken"]
            ))
            files += resp["files"]
        return files

    def _lsd(self, folder_id):
        return self._ls(
            folder_id,
            searchTerms="mimeType contains \"application/vnd.google-apps.folder\""
        )

    async def _lsf(self, folder_id, fields="files(id,name,size,permissionIds),nextPageToken"):
        return self._ls(
            folder_id,
            fields=fields,
            searchTerms="not mimeType contains \"application/vnd.google-apps.folder\""
        )
        sem.release()

    def check_file_shared(self, file_to_check):
        shared = False
        if "permissionIds" in file_to_check:
            for permissionId in file_to_check["permissionIds"]:
                if permissionId[-1] == "k" and permissionId[:-1].isnumeric():
                    self.delete_file_permission(file_to_check["id"], permissionId)
                if permissionId == "anyoneWithLink":
                    shared = True
        return shared

    def delete_file_permission(self, file_id, permission_id):
        self._apicall(self.drive_service.permissions().delete(fileId=file_id, permissionId=permission_id, supportsAllDrives=True))

    def get_all_folders_in_folder(self, folder_id):
        folder_ids = []

        for folder in self._lsd(folder_id):
            folder_ids.append(folder["id"])
            folder_ids.extend(self.get_all_folders_in_folder(folder["id"]))

        return folder_ids

    async def get_all_files_in_folder(self, folder_id, recursion=True, files_pbar=None):
        folders_to_scan = []

        if recursion:
            folders_to_scan.extend(self.get_all_folders_in_folder(folder_id))

        folders_to_scan.append(folder_id)

        tasks = []

        for folder_id in folders_to_scan:
            await sem.acquire()
            tasks.append(asyncio.create_task(self._lsf(folder_id)))

        files = {}

        for task in tasks:
            await task
            part_files = task.result()
            if isinstance(files_pbar, tqdm.std.tqdm):
                files_pbar.update(len(part_files))
            for gdrive_file in part_files:
                if "size" in gdrive_file:
                    files.update({gdrive_file["id"]: {"size": gdrive_file["size"], "name": gdrive_file["name"], "shared": self.check_file_shared(gdrive_file)}})

        return files

    def share_file(self, file_id_to_share):
        self._apicall(self.drive_service.permissions().create(fileId=file_id_to_share, supportsAllDrives=True, body={
            "role": "reader",
            "type": "anyone"
        }))

    def upload_file(self, file_path, dest_folder_id=None):
        media = MediaFileUpload(file_path)
        if dest_folder_id is None:
            response = self._apicall(self.drive_service.files().create(media_body=media, body={"name": Path(file_path).name}, supportsAllDrives=True))
        else:
            response = self._apicall(self.drive_service.files().create(media_body=media, body={"name": Path(file_path).name, "parents": [dest_folder_id]}, supportsAllDrives=True))

        if "id" in response:
            self.share_file(response["id"])
            print("Add the following to tinfoil: gdrive:/{file_id}#{file_name}".format(file_id=response["id"], file_name=response["name"]))


class Generator:
    DEFAULT_INDEX_FILE = "index.tfl"

    def __init__(self, args):
        if isinstance(args, argparse.Namespace):
            self.folder_ids = args.folder_ids if args.folder_ids and type(args.folder_ids) == list else []
            self.files = []
            self.gdrive_service = None
            self.success = args.success if args.success and type(args.success) == str else None
            self.index_path = Path(args.index_file if args.index_file and type(args.index_file) == str else DEFAULT_INDEX_FILE)
            self.recursion = args.recursion if args.recursion and type(args.recursion) == bool else True
            self.allow_without_title_id = args.allow_without_title_id if args.allow_without_title_id and type(args.allow_without_title_id) == bool else False
            if args.keep_old_files_in_index:
                self.read_index_file()

    def index_folders(self):
        raise NotImplementedError("This is an abstract method.")

    def read_index_file(self):
        if self.index_path.is_file():
            with open(self.index_path, "r") as index_json:
                try:
                    index_json = json.loads(index_json.read())
                    if "files" in index_json:
                        self.files += index_json["files"]
                except json.JSONDecodeError:
                    raise Exception("Error while trying to read the index json file. Make sure that it is a valid JSON file.")

    def write_index_to_folders(self):
        Path(self.index_path).parent.resolve().mkdir(parents=True, exist_ok=True)
        to_write = {"files": self.files}
        if self.success is not None:
            to_write.update({"success": self.success})
        with open(self.index_path, "w") as output_file:
            json.dump(to_write, output_file, indent=2)

    @staticmethod
    def is_file_name_with_title_id(file_name):
        return True if isinstance(file_name, str) and re.match(r"\[[0-9A-Fa-f]{16}\]", file_name) else False


class TinGen(Generator):
    DEFAULT_CREDENTIALS = "credentials.json"
    DEFAULT_TOKEN = "gdrive.token"

    def __init__(self, args):
        if isinstance(args, argparse.Namespace):
            super().__init__(args)
            self.credentials_path = Path(args.credentials if args.credentials and type(args.credentials) == str else DEFAULT_CREDENTIALS)
            self.token_path = Path(args.token if args.token and type(args.token) == str else DEFAULT_TOKEN)
            self.share_files = args.share_files if args.share_files and type(args.share_files) == bool else False
            self.gdrive_service = GDrive(self.token_path, self.credentials_path)

    async def index_folders(self):
        files_pbar = tqdm.tqdm(desc="Files Scanned", unit="file", unit_scale=True)
        list_tasks = []
        files = {}

        if self.share_files:
            to_share = []

        for folder_id in self.folder_ids:
            list_tasks.append(asyncio.create_task(self.gdrive_service.get_all_files_in_folder(folder_id, self.recursion, files_pbar)))

        for task in list_tasks:
            await task
            files.update(task.result())

        files_pbar.close()

        for (file_id, file_details) in files.items():
            file_name = urllib.parse.quote(file_details["name"], safe="")
            if self.allow_without_title_id or Generator.is_file_name_with_title_id(file_name):
                to_add = {"url": "gdrive:{file_id}#{file_name}".format(file_id=file_id, file_name=file_name), "size": int(file_details["size"])}

                if to_add not in self.files:
                    self.files.append(to_add)

                if file_details["shared"]:
                    continue
                elif self.share_files:
                    to_share.append(file_id)

        if self.share_files:
            if len(to_share) > 0:
                for file_id_to_share in tqdm.tqdm(to_share, desc="File Share Progress"):
                    self.gdrive_service.share_file(file_id_to_share)

        self.write_index_to_folders()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script that will allow you to easily generate an index file with Google Drive file links for use with Tinfoil.")
    parser.add_argument(nargs="*", metavar="FOLDER_ID_TO_SCAN", dest="folder_ids", help="Folder ID of Google Drive folders to scan. Can use more than 1 folder IDs at a time.")
    parser.add_argument("--max-list-file-tasks", default=10, type=int, help="Number of maximum list file tasks to perform concurrently.")
    parser.add_argument("--index-file", metavar="INDEX_FILE_PATH", default="index.tfl", help="File Path for unencrypted index file to update.")
    parser.add_argument("--encrypt", nargs="?", metavar="ENC_INDEX_FILE_PATH", const="enc_index.tfl", help="Use this flag is you want to encrypt the resulting index file.")
    parser.add_argument("--public-key", metavar="PUBLIC_KEY_FILE_PATH", default="public.key", help="File Path for Public Key to encrypt with.")
    parser.add_argument("--success", metavar="SUCCESS_MESSAGE", help="Success Message to add to index.")
    parser.add_argument("--unauthenticated", action="store_true", help="Use this flag if you want to generate the file from public folders without requiring authentication. This may not work in the future.")
    parser.add_argument("--keep-old-files-in-index", action="store_true", help="Use this flag if you want to keep files already in the index file. By deafult, the script will overwrite the index file even it has files indexed in the index file.")
    parser.add_argument("--allow-without-title_id", action="store_true", help="Use this flag if you want to keep files already in the index file. By deafult, the script will overwrite the index file even it has files indexed in the index file.")
    parser.add_argument("--disable-recursion", dest="recursion", action="store_false", help="Use this flag to stop folder IDs entered from being recusively scanned. (It basically means if you use this flag, the script will only add the files at the root of each folder ID passed, without going through the sub-folders in it.")
    auth_options = parser.add_argument_group("auth_options", "Options available if script is authenticated")
    auth_options.add_argument("--upload-to-folder-id", metavar="UPLOAD_FOLDER_ID", dest="upload_folder_id", help="Upload resulting index to folder id supplied.")
    auth_options.add_argument("--upload-to-my-drive", action="store_true", help="Upload resulting index to My Drive")
    auth_options.add_argument("--share-files", action="store_true", help="Use this flag if you want to share files that gets newly added to your index file.")
    auth_options.add_argument("--credentials", default="credentials.json", metavar="CREDENTIALS_FILE_NAME", help="Obtainable from https://developers.google.com/drive/api/v3/quickstart/python. Make sure to select the correct account before downloading the credentails file.")
    auth_options.add_argument("--token", default="gdrive.token", metavar="TOKEN_FILE_PATH", help="File Path of a Google Token file.")

    args = parser.parse_args()

    sem = asyncio.BoundedSemaphore(args.max_list_file_tasks)

    if not args.unauthenticated:
        generator = TinGen(args)

    asyncio.run(generator.index_folders())

    if args.encrypt:
        encrypt_file(args.index_file, args.encrypt, public_key=args.public_key)

    if not args.unauthenticated:
        if args.upload_folder_id:
            generator.gdrive_service.upload_file(args.encrypt if args.encrypt else args.index_file, args.upload_folder_id)
        if args.upload_to_my_drive:
            generator.gdrive_service.upload_file(args.encrypt if args.encrypt else args.index_file)