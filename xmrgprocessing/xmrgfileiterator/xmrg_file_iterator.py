import os
import logging.config
from datetime import timedelta
from pathlib import Path
from string import Template

from ..xmrg_utilities import file_list_from_date_range, build_filename

DEFAULT_XMRG_PATH = "{base_path}/{year}/{month}"
class xmrg_file_iterator:
    '''
    This class serves as an iterator for the xmrg files we want to process.
    The default behavior is to build the list from
    '''
    def __init__(self, **kwargs):
        self._logger = logging.getLogger('xmrg_file_iterator')
        self._file_list = []
        #We can provide the full path to where all the XMRG files would be.
        self._full_xmrg_path = kwargs.get('full_xmrg_path', None)
        #If we are using the /year/month template for the xmrg files, the
        #base_xmrg_path is the parent directory where those sub-directories begin.
        self._base_xmrg_path = kwargs.get('base_xmrg_path', None)

        self._start_date = kwargs.get('start_date', None)
        self._end_date = kwargs.get('end_date', None)
        self._current_iterate_date = self._start_date

    def __iter__(self):
        return self

    def __next__(self):
        full_filepath = None
        try:
            file_name = build_filename(self._current_iterate_date, "gz")
        except Exception as e:
            self._logger.exception(e)
        else:
            if self._current_iterate_date < self._end_date:
                if self._full_xmrg_path is None:
                    full_filepath = self.get_path(self._current_iterate_date,
                                                   file_name,
                                                   self._base_xmrg_path,
                                                   DEFAULT_XMRG_PATH)
                else:
                    full_filepath = os.path.join(self._full_xmrg_path, file_name)
            else:
                raise StopIteration
            #The data files are hourly, so increment are iterate date by an hour.
            self._current_iterate_date += timedelta(hours=1)
        return full_filepath

    def get_path(self, file_date, file_name, base_path, path_template):
        '''

        :param file_date: The date used to build the filename.
        :param file_name:  The xmrg file name.
        :param base_path:  The base path used to build the full file path.
        :param path_template:  The template of the full file path.
        :return:
        '''
        file_year = file_date.year
        file_month = file_date.strftime('%b')

        xmrg_path = Path(path_template.format(base_path=base_path,
                                              year=file_year,
                                              month=file_month))
        xmrg_path = os.path.join(xmrg_path, file_name)
        return xmrg_path

    def  setup_iterator(self, **kwargs):
        self._full_xmrg_path = kwargs.get('full_xmrg_path', None)
        self._base_xmrg_path = kwargs.get('base_xmrg_path', None)

        self._start_date = kwargs['start_date']
        self._end_date = kwargs['end_date']
        self._current_iterate_date = self._start_date

