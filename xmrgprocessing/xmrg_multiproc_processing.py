import os
import logging
import threading
from multiprocessing import Process, Queue, current_process
import time
from pathlib import Path
from queue import Empty

import pandas as pd
import geopandas as gpd
import shutil

from xmrgprocessing.xmrg_results import xmrg_results
from xmrgprocessing.geoXmrg import geoXmrg, LatLong
from xmrgprocessing.xmrg_utilities import get_collection_date_from_filename



def file_queue_builder(**kwargs):
    '''
    This function is a thread worker that creates the list of XMRG files we're going to process.
    We use a worker thread to do this since it's possible we could have 1000s of files and it would
    be memory costly to add all the files into the queue first before we started processing.
    :param kwargs:
    :return:
    '''
    logger = logging.getLogger()
    try:
        input_queue = kwargs['input_queue']
        file_list_iterator = kwargs['file_list_iterator']
        local_copy_directory = kwargs['local_copy_directory']
        unique_id = kwargs['unique_id']
        worker_count = kwargs['worker_count']
        logger.info(f"{unique_id} file_queue_builder starting.")

        file_count = 0
        for xmrg_file in file_list_iterator:
            logger.info(f"{unique_id} queueing file: {xmrg_file}")
            file_to_process = xmrg_file
            if os.path.isfile(xmrg_file):
                # Copy the file to our local working directory
                if local_copy_directory is not None:
                    try:
                        xmrg_src_dir, xmrg_src_filename = os.path.split(xmrg_file)
                        source_full_filepath = os.path.join(local_copy_directory, xmrg_src_filename)
                        logger.info(f"{unique_id} copying to local file: {source_full_filepath}")
                        shutil.copy2(xmrg_file, source_full_filepath)
                        file_to_process = source_full_filepath
                    except Exception as e:
                        logger.exception(f"{unique_id} {e}")
                        file_to_process = None
            else:
                file_to_process = None

            if file_to_process is not None:
                input_queue.put(file_to_process)
                file_count += 1
    except Exception as e:
        logger.exception(f"{unique_id} {e}")

    #Add the stop indicator for each worker.
    for cnt in range(worker_count):
        input_queue.put("STOP")
    logger.info(f"{unique_id} Finished iterating {file_count} files.")
    return

def process_xmrg_file_geopandas(**kwargs):
    '''
    This is a Process worker which pulls XMRG filenames from the input_queue and with the boundaries
    provided, calculates the weighted rainfall average for the boundary.
    The results are stored in an xmrg_result object and added to the results_queue which
    the parent process handles.
    :param kwargs:
    :return:
    '''
    try:
        try:
            processing_start_time = time.time()

            gp_results = None

            xmrg_file_count = 1
            logger = None
            process_name = current_process().name

            #Each worker will get its own log file.
            base_log_output_directory = kwargs.get('base_log_output_directory',
                                                   'process_xmrg_file_geopandas.log')
            log_output_filename = os.path.join(base_log_output_directory,
                                               f"process_xmrg_file_geopandas-{process_name}.log")
            error_log_output_filename = os.path.join(base_log_output_directory,
                                               f"process_xmrg_file_geopandas_errors-{process_name}.log")
            debug_dir = kwargs['debug_files_directory']
            input_queue = kwargs['input_queue']
            results_queue = kwargs['results_queue']
            save_all_precip_vals = kwargs['save_all_precip_vals']
            delete_source_file = kwargs['delete_source_file']
            delete_compressed_source_file = kwargs['delete_compressed_source_file']
            # A course bounding box that restricts us to our area of interest.
            minLatLong = None
            maxLatLong = None
            if 'min_lat_lon' in kwargs and 'max_lat_lon' in kwargs:
                minLatLong = LatLong(kwargs['min_lat_lon'][0], kwargs['min_lat_lon'][1])
                maxLatLong = LatLong(kwargs['max_lat_lon'][0], kwargs['max_lat_lon'][1])

            # Boundaries we are creating the weighted averages for.
            boundaries = kwargs['boundaries']

            logger = logging.getLogger(process_name)
            logger.setLevel(logging.DEBUG)
            formatter = logging.Formatter("%(asctime)s,%(levelname)s,%(funcName)s,%(lineno)d,%(message)s")
            fh = logging.handlers.RotatingFileHandler(log_output_filename)
            error_fh = logging.handlers.RotatingFileHandler(error_log_output_filename)
            ch = logging.StreamHandler()
            fh.setLevel(logging.DEBUG)
            error_fh.setLevel(logging.ERROR)
            ch.setLevel(logging.DEBUG)
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)
            logger.addHandler(fh)
            logger.addHandler(ch)


            logger.info(f"{process_name} starting process_xmrg_file_geopandas.")


            save_boundary_grid_cells = True
            save_boundary_grids_one_pass = True
            write_percentages_grids_one_pass = True

        except Exception as e:
            logger.error(f"{process_name} {e}")
            #logger.exception(e)

        else:
            # Build boundary dataframes
            logger.info(f"{process_name} begin processing boundaries.")
            boundary_frames = []
            for boundary in boundaries:
                logger.info(f"{process_name} adding boundary {boundary[0]}")
                df = pd.DataFrame([[boundary[0], boundary[1]]], columns=['Name', 'Boundaries'])
                boundary_df = gpd.GeoDataFrame(df, geometry=df.Boundaries)
                boundary_df = boundary_df.drop(columns=['Boundaries'])
                boundary_df.set_crs(epsg=4326, inplace=True)
                try:
                    #Write out a geojson file we can use to visualize the boundaries if needed.
                    #Write it before we change the CRS to 3857.
                    boundaries_outfile = os.path.join(debug_dir,
                                                      f"{boundary_df['Name'][0].replace(' ', '_')}_boundary.json")
                    if not os.path.exists(boundaries_outfile):
                        boundary_df.to_file(boundaries_outfile, driver="GeoJSON")
                except Exception as e:
                    logger.exception(e)

                #Convert to a projected CRS.
                boundary_df.to_crs(epsg=3857, inplace=True)
                boundary_frames.append(boundary_df)

            tot_file_time_start = time.time()
            logger.info(f"{process_name} begin processing queue.")
            for xmrg_filename in iter(input_queue.get, 'STOP'):
                logger.info(f"{process_name} processing file: {xmrg_filename}")

                gpXmrg = geoXmrg(minLatLong, maxLatLong, 0.01)
                try:
                    gpXmrg.openFile(xmrg_filename)
                except Exception as e:
                    logger.exception(f"{process_name} Failed to open file: {xmrg_filename}. {e}")
                else:

                    # This is the database insert datetime.
                    # Parse the filename to get the data time.
                    (directory, filetime) = os.path.split(gpXmrg.fileName)
                    xmrg_filename = filetime
                    (filetime, ext) = os.path.splitext(filetime)
                    filetime = get_collection_date_from_filename(filetime)

                    try:
                        if gpXmrg.readFileHeader():
                            read_rows_start = time.time()
                            gpXmrg.readAllRows()
                            if logger:
                                logger.info(f"{process_name}({time.time() - read_rows_start} secs)"
                                            f" to read all rows in file: {xmrg_filename}")

                            gp_results = xmrg_results()
                            gp_results.datetime = filetime

                            for index, boundary_row in enumerate(boundary_frames):
                                file_start_time = time.time()
                                xmrg_projected = gpXmrg.geo_data_frame.to_crs(epsg=3857, inplace=False)
                                overlayed = gpd.overlay(boundary_row, xmrg_projected, how="intersection",
                                                        keep_geom_type=False)

                                # Here we create our percentage column by applying the function in the map(). This applies to
                                # each area.
                                overlayed['percent'] = overlayed.area.map(
                                    lambda area: float(area) / float(boundary_row.area.iloc[0]))
                                overlayed['weighted average'] = (overlayed['Precipitation']) * (overlayed['percent'])

                                wghtd_avg_val = sum(overlayed['weighted average'])
                                gp_results.add_boundary_result(boundary_row['Name'][0], 'weighted_average',
                                                               wghtd_avg_val)
                                logger.info(f"{process_name} File: {xmrg_filename} "
                                            f"Processed boundary: {boundary_row.Name[0]} WgtdAvg: {wghtd_avg_val}"
                                            f" in {time.time() - file_start_time} seconds.")
                                xmrg_file_count += 1

                                if save_boundary_grid_cells or write_percentages_grids_one_pass:
                                    #We want EPSG 4326 for our output debug files.
                                    overlayed_4326 = overlayed.to_crs(epsg=4326, inplace=False)
                                    if save_boundary_grid_cells:
                                        for ndx, row in overlayed_4326.iterrows():
                                            gp_results.add_grid(row.Name, (row.geometry, row.Precipitation))

                                    if write_percentages_grids_one_pass:
                                        try:
                                            percentage_file = os.path.join(debug_dir,
                                                f"{overlayed['Name'][0].replace(' ', '_')}_percentage.json")
                                            if not os.path.exists(percentage_file):
                                                overlayed_4326.to_file(percentage_file, driver="GeoJSON")
                                            #Once we've written out each boundary, we can stop.
                                            if index == len(boundary_frames) - 1:
                                                write_percentages_grids_one_pass = False
                                        except Exception as e:
                                            logger.exception(e)
                                if save_boundary_grids_one_pass:
                                    try:
                                        full_data_grid = os.path.join(debug_dir,
                                                                      "%s_%s_fullgrid_.json" % (
                                                                      filetime.replace(':', '_'),
                                                                      boundary_row.Name[0].replace(' ', '_')))
                                        gpXmrg._geo_data_frame.to_file(full_data_grid, driver="GeoJSON")
                                        save_boundary_grids_one_pass = False
                                    except Exception as e:
                                        logger.exception(e)

                            results_queue.put(gp_results)
                            try:
                                gpXmrg.cleanUp(delete_source_file, delete_compressed_source_file)
                            except Exception as e:
                                logger.exception(e)
                        else:
                            logger.error(f"{process_name} Failed to process file: {xmrg_filename}")
                    except Exception as e:
                        logger.exception(f"{process_name} Failed to process file: {xmrg_filename}. {e}")
            logger.info(f"{process_name} process finished. Processed in: "
                         f"{time.time() - processing_start_time} seconds")
    except Exception as e:
        logger.exception(e)

    return


class xmrg_processing_geopandas:
    def __init__(self):
        self._logger = None
        self._min_latitude_longitude = None
        self._max_latitude_longitude = None
        self._save_all_precip_values = False
        self._boundaries = []
        self._source_file_working_directory = None
        self._delete_source_file = False
        self._delete_compressed_source_file = False
        self._kml_output_directory = None
        self._callback_function = None
        self._logging_config = None
        self._base_log_output_directory = ""
        self._worker_process_count = 4
        self._unique_id = ""
    def setup(self, **kwargs):

        self._unique_id = kwargs.get("unique_id", "")
        self._logger = logging.getLogger(f"xmrg_task_{self._unique_id}")

        #Number of Processes to spawn.
        self._worker_process_count = kwargs.get("worker_process_count", 4)

        #The overall bounding box to trim the XMRG data to.
        self._min_latitude_longitude = kwargs.get("min_latitude_longitude", None)
        self._max_latitude_longitude = kwargs.get("max_latitude_longitude", None)

        #Save all the preciptation values, not just > 0 ones.
        self._save_all_precip_values = kwargs.get("save_all_precip_values", False)

        #The list of boundaries to process rain data for.
        self._boundaries = kwargs.get("boundaries", None)

        #These next parameters deal with where we process the data files. We might be grabbing files
        #from an archive, so we want to copy them to a working directory.
        #If set, copy the XMRG files to this directory for processing.
        self._source_file_working_directory = kwargs.get("source_file_working_directory", None)
        if self._source_file_working_directory is not None:
            self._source_file_working_directory = Path(self._source_file_working_directory)
            self._source_file_working_directory.mkdir(parents=True, exist_ok=True)

        #Delete the source file when it has been processed.
        self._delete_source_file = kwargs.get("delete_source_file", False)
        #Delete the compressed file after processing
        self._delete_compressed_source_file = kwargs.get("delete_compressed_source_file", False)

        #The directory to output the KML file we use for debugging.
        self._kml_output_directory = kwargs.get("kml_output_directory", None)
        if self._kml_output_directory is not None:
            self._kml_output_directory = Path(self._kml_output_directory)
            self._kml_output_directory.mkdir(parents=True, exist_ok=True)

        #Callback function used when we have a result.
        self._callback_function = kwargs.get("callback_function", None)

        #Directory where logfiles are written.
        self._base_log_output_directory = kwargs.get("base_log_output_directory", "")
        if self._base_log_output_directory is not None:
            self._base_log_output_directory = Path(self._base_log_output_directory)
            self._base_log_output_directory.mkdir(parents=True, exist_ok=True)

    def import_files(self, file_list_iterator):
        start_import_files_time = time.time()

        self._logger.info("Start import_files")

        current_process().daemon = False

        input_queue = Queue()
        results_queue = Queue()
        #Start the file list populator thread.
        thrd_args = {
            'input_queue': input_queue,
            'file_list_iterator': file_list_iterator,
            'local_copy_directory': self._source_file_working_directory,
            'unique_id': self._unique_id,
            'worker_count': self._worker_process_count
        }
        file_queue_build_thread = threading.Thread(target=file_queue_builder, kwargs=thrd_args)
        file_queue_build_thread.start()
        try:
            processes = []
            #Create a multiprocessing Process() for each worker.
            for workerNum in range(self._worker_process_count ):
                args = {
                    'input_queue': input_queue,
                    'results_queue': results_queue,
                    'min_lat_lon': self._min_latitude_longitude,
                    'max_lat_lon': self._max_latitude_longitude,
                    'save_all_precip_vals': self._save_all_precip_values,
                    'boundaries': self._boundaries,
                    'delete_source_file': self._delete_source_file,
                    'delete_compressed_source_file': self._delete_compressed_source_file,
                    'debug_files_directory': self._kml_output_directory,
                    'base_log_output_directory': self._base_log_output_directory
                }
                p = Process(target=process_xmrg_file_geopandas, kwargs=args)
                self._logger.info(f"{self._unique_id} Starting process: %s" % (p._name))
                p.start()
                processes.append(p)

            rec_count = 0
            while any([(checkJob is not None and checkJob.is_alive()) for checkJob in processes]):
                #if not results_queue.empty():
                try:
                    self.process_result(results_queue.get(block=False))
                    rec_count += 1
                    if (rec_count % 10) == 0:
                        self._logger.info(f"{self._unique_id} Processed {rec_count} results")
                except Empty:
                    if (rec_count % 100) == 0:
                        msg = ['Q Empty']
                        for p in processes:
                            msg.append(f"{p._name} {p .is_alive()}")
                        self._logger.debug(msg)
                except ValueError as e:
                    self._logger.exception(e)

        finally:
            # Wait for the process to finish.
            self._logger.info(f"{self._unique_id} waiting for {self._worker_process_count} processes to finish.")
            for p in processes:
                if p.is_alive():
                    p.terminate()
                    p.join()
                if hasattr(p, 'close'):
                    p.close()

            #Wait for the file builder queue to finish.
            file_queue_build_thread.join()

            self._logger.info(f"{self._unique_id} builder thread and xmrg processes finished.")


            # Poll the queue once more to get any remaining records.
            while not results_queue.empty():
                self._logger.info(f"{self._unique_id} Pulling records from resultsQueue.")
                self.process_result(results_queue.get())
                rec_count += 1

            results_queue.close()
            input_queue.close()

        self._logger.info(f"{self._unique_id} Finished. Imported: {rec_count} records in: "
                          f"{time.time() - start_import_files_time} seconds")

        return

    def process_result(self, xmrg_results_data):
        if self._callback_function is not None:
            self._callback_function(xmrg_results_data)
        return