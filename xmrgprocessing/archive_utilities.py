import logging
import os
import glob
import string
import requests
from datetime import datetime, timedelta
import pytz

from dateutil.relativedelta import relativedelta

from xmrgprocessing.xmrg_utilities import build_filename, get_collection_date_from_filename
from xmrgprocessing.xmrg_utilities import http_download_file

class xmrg_archive_utilities:
    def __init__(self, archive_directory):
        self._logger = logging.getLogger()
        self._parent_directory = archive_directory
        self._data_path_template = string.Template("$year/$month")

    def build_file_list_for_date_range(self, start_date, end_date, file_ext):
        date_time_list = []
        date_time = start_date
        while date_time < end_date:
            file_name = build_filename(date_time, file_ext)
            date_time += timedelta(hours=1)
            date_time_list.append(file_name)
        return date_time_list

    def file_list(self, year, month_abbreviation):
        '''
        Given the year and month, return a directory listing of the files there.
        :param year:
        :param month_abbreviation:
        :return:
        '''
        path_to_check = self._data_path_template.substitute(year=year, month=month_abbreviation)
        path_to_check = os.path.join(self._parent_directory, path_to_check)
        file_ext = "gz"
        file_filter = os.path.join(path_to_check, f"*.{file_ext}")
        file_list = glob.glob(file_filter)
        #We might not have the .gz files, so let's search just for files.
        if len(file_list) == 0:
            file_ext = ""
            file_filter = os.path.join(path_to_check, "*")
            file_list = glob.glob(file_filter)
        return file_list

    def scan_for_missing_data(self, from_date, to_date):
        '''

        :param from_date:
        :param to_date:
        :return:
        '''
        results = {}
        date_time = from_date
        #Build a list of the files we should have for a given date range.
        complete_file_list = self.build_file_list_for_date_range(from_date, to_date, "")
        complete_file_set = set(complete_file_list)
        while date_time < to_date:
            year = date_time.year
            month_str = date_time.strftime("%b")
            #Get all the files available for the given year/month
            file_list = self.file_list(year, month_str)
            #Get file names only
            file_name_list = []
            for file in file_list:
                file_dir, file_name = os.path.split(file)
                #We just want the name of the file with no extensions.
                file_name, exten = os.path.splitext(file_name)
                file_name_list.append(file_name)
            end_date_time = date_time + relativedelta(months=1)
            #end_date_time = date_time + relativedelta(hours=1)

            archive_file_set = set(file_name_list)
            missing_files = complete_file_set.difference(archive_file_set)
            if len(missing_files):
                if year not in results:
                    results[year] = {}
                if month_str not in results[year]:
                    results[year][month_str] = []
                results[year][month_str].extend(list(missing_files))
            date_time = end_date_time
        return results

    def download_files(self, base_url: str, file_list: [], delete_if_exists: bool):
        '''

        :param base_url: The url we use to build the download URL for the data file.
        :param download_directory: Location to store the downloaded data file.
        :param file_list: List of files to download.
        :param delete_if_exists: If the file we want to download already exists, we delete it before downloading.
        :return:
        '''

        for xmrg_file in file_list:
            file_datetime = datetime.strptime(get_collection_date_from_filename(xmrg_file), "%Y-%m-%dT%H:00:00")

            year = file_datetime.year
            month_abbr = file_datetime.strftime("%b")

            download_path = self._data_path_template.substitute(year=year, month=month_abbr)
            download_path = os.path.join(self._parent_directory, download_path)
            if not os.path.exists(download_path):
                self._logger.info(f"Directory: {download_path} does not exist, creating it.")
                try:
                    os.makedirs(download_path, exist_ok=False)
                except FileExistsError as e:
                    self._logger.error(f"Directory {download_path} already exists, skipping. {e}")
            file_ext = "gz"
            #Check if the path we want to download to exists. Data is stored in a /year/month
            #directory structure.
            dl_xmrg_filename = f"{xmrg_file}.{file_ext}"
            self._logger.info(f'Downloading xmrg file: {dl_xmrg_filename}')
            #If the file exists, let's delete it and redownload.
            existing_file_name = os.path.join(download_path, dl_xmrg_filename)
            if delete_if_exists and os.path.exists(existing_file_name):
                self._logger.info(f"Deleting existing file: {existing_file_name}")
                try:
                    os.remove(existing_file_name)
                except Exception as e:
                    self._logger.error(f"Failed to delete existing file: {existing_file_name}. {e}")

            xmrg_file = http_download_file(base_url, dl_xmrg_filename, download_path)
            if xmrg_file is None:
                self._logger.error(f'Failed to download xmrg file: {dl_xmrg_filename}')
            else:
                self._logger.info(f'Successfully downloaded xmrg file: {dl_xmrg_filename}')
        return

    def check_file_timestamps(self, base_url, from_date, to_date, repository_data_duration_hours):
        #Get a list of the files we have.
        date_time = from_date
        #We do not need to check the remote repository for any times older than this one.
        oldest_date_at_repository = datetime.now() - timedelta(hours=repository_data_duration_hours)
        local_tz = pytz.timezone('America/New_York')
        gmt_tz = pytz.timezone('GMT')
        self._logger.info(f"Checking updated files for {from_date.strftime('%Y-%m-%d %H:%M:%S')}"
                          f" to {to_date.strftime('%Y-%m-%d %H:%M:%S')}")
        while date_time < to_date:
            year = date_time.year
            month_abbreviation = date_time.strftime("%b")
            current_file_list = self.file_list(year, month_abbreviation)
            files_to_download = []
            for current_file in current_file_list:
                file_directory, file_name = os.path.split(current_file)
                current_file_datetime = datetime.strptime(get_collection_date_from_filename(file_name), "%Y-%m-%dT%H:00:00")
                if current_file_datetime > oldest_date_at_repository:
                    mtime = os.path.getmtime(current_file)
                    local_mod_time = datetime.fromtimestamp(mtime, local_tz)
                    try:
                        directory, file_name = os.path.split(current_file)
                        file_name, file_ext = os.path.splitext(file_name)
                        remote_file_name = f"{file_name}.gz"
                        remote_filename_url = os.path.join(base_url, remote_file_name)
                        remote_file_info = requests.head(remote_filename_url)
                        if remote_file_info.status_code == 200:
                            remote_file_info.raise_for_status()  # Raise an exception if the request fails
                            header_param = None
                            if 'Last-Modified' in remote_file_info.headers:
                                header_param = 'Last-Modified'
                            elif 'Date' in remote_file_info.headers:
                                header_param = 'Date'
                            last_modified = remote_file_info.headers[header_param]
                            remote_timestamp = datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S %Z').astimezone(gmt_tz)
                            if remote_timestamp > local_mod_time:
                                files_to_download.append(file_name)
                                self._logger.info(f"Remote file: {remote_file_name} "
                                                  f"{local_mod_time.strftime('%Y-%m-%d %H:%M:%S')} more recent "
                                                  f"time stamp than remote file: "
                                                  f"{remote_timestamp.strftime('%Y-%m-%d %H:%M:%S')} adding to "
                                                  f"re-download.")

                        else:
                            self._logger.info(f"Remote file: {remote_file_name} no longer on remote server. HTML status "
                                              f"code: {remote_file_info.status_code} Reason: {remote_file_info.reason}")
                    except Exception as e:
                        self._logger.exception(e)

            #Now we hit the endpoint where the remote system has the files stored and check their last modified time
            #XMRg files get periodically updated due to QAQC during the day, so we try and get the latest version.
            end_date_time = date_time + relativedelta(months=1)

            date_time = end_date_time
            if len(files_to_download) > 0:
                self.download_files(base_url, files_to_download, True)
            self._logger.info(f"Finished checking updated files for {from_date} to {to_date}")


    def get_file_location(self, filename: str):
        '''
        Given the filename, this will return the fullpath to it's location in the archive.
        :param filename:
        :return:
        '''
        file_datetime = datetime.strptime(get_collection_date_from_filename(filename), "%Y-%m-%dT%H:00:00")
        month_abbreviation = file_datetime.strftime("%b")

        full_filepath = self._data_path_template.substitute(year=file_datetime.year, month=month_abbreviation)
        full_filepath = os.path.join(self._parent_directory, full_filepath)
        full_filename = os.path.join(full_filepath, filename)
        if os.path.exists(full_filename):
            return full_filename
        return None
    def copy_file(self, filename: str, destination_directory: str):
        return