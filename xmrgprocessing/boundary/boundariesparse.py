import csv
import os
from shapely import from_wkt, to_geojson, from_geojson
from shapely.geometry import MultiPolygon
import geojson
import geopandas as gpd
import logging
from lxml import objectify


class QueryBoundary:
    def __init__(self):
        self._name = None
        self._boundary = None

    def build_boundary(self, name, bndry):
        self._name = name
        self._boundary = bndry

    @property
    def name(self):
        return self._name

    @property
    def boundary(self):
        return self._boundary

class Boundary:
    def __init__(self, unique_id):
        self._id = unique_id
        self._logger = logging.getLogger()
        self._boundaries = []

    @property
    def boundaries(self):
        return self._boundaries

    def determine_boundaries_filetype(self, file: str):
        type = None
        filepath, filename = os.path.split(file)
        filename, filext = os.path.splitext(file)
        if ".shp" in filext:
            type = 'shapefile'
            filename = os.path.join(filepath, file)
        elif ".csv" in filext:
            type = 'csv'
            filename = os.path.join(filepath, file)
        elif ".json" in filext:
            type = 'json'
            filename = os.path.join(filepath, file)
        '''
        files = os.listdir(filepath)
        self._logger.info(f"{self._id} files: {files}")
        for file in files:
            filename, filext = os.path.splitext(file)
            if ".shp" in filext:
                type = 'shapefile'
                filename = os.path.join(filepath, file)
                break
            elif ".csv" in filext:
                type = 'csv'
                filename = os.path.join(filepath, file)
                break
            elif ".json" in filext:
                type = 'json'
                filename = os.path.join(filepath, file)
                break
        '''
        return type,filename

    def get_parser(self, file_type: str):
        if file_type == 'csv':
            return CSVBoundaryParser
        elif file_type == 'json':
            return JSONBoundaryParser
        elif file_type == 'shapefile':
            return SHPBoundaryParser
        return None

    def parse_boundaries_file(self, filepath):
        self._logger.info(f"{self._id} parse_boundaries_file checking: {filepath}")
        #If the filepath is a directory, then we'll list the files in it. This is probably going to
        #be a unzipped shapefile.
        files = os.listdir(filepath)
        self._logger.info(f"{self._id} files: {files}")
        for file in files:
            fullpath = os.path.join(filepath, file)
            file_type,filename = self.determine_boundaries_filetype(fullpath)
            if file_type != None:
                self._logger.info(f"{self._id} parse_boundaries_file checking: {filepath} is type: {file_type}")
                parser_class = self.get_parser(file_type)
                self._logger.info(f"{self._id} parse_boundaries_file parser is: {parser_class}")
                try:
                    bnd_parser = parser_class(unique_id=self._id)
                    self._logger.info(f"{self._id} parse_boundaries_file parsing file: {filepath}")
                    boundary = bnd_parser.parse(filepath=filename)
                    for bound in boundary:
                        self._boundaries.append(bound)
                except Exception as e:
                    raise e
        if len(self._boundaries) > 0:
            return True
        '''
        file_type,filename = self.determine_boundaries_filetype(filepath)
        self._logger.info(f"{self._id} parse_boundaries_file checking: {filepath} is type: {file_type}")
        parser_class = self.get_parser(file_type)
        self._logger.info(f"{self._id} parse_boundaries_file parser is: {parser_class}")
        try:
            bnd_parser = parser_class(unique_id=self._id)
            self._logger.info(f"{self._id} parse_boundaries_file parsing file: {filepath}")
            self._boundaries = bnd_parser.parse(filepath=filename)
            return True
        except Exception as e:
            raise e
            #self._logger.exception(f"{self._id} parse_boundaries_file exception: {e}")
        '''
        return False


class BoundaryParser:
    def __init__(self, unique_id):
        self._id = unique_id
        self._logger = logging.getLogger()
    def parse(self, **kwargs):
        boundaries = None
        self._logger.info(f"{self._id} parse started filepath: {kwargs['filepath']}")
        try:
            boundaries = self._do_parsing(**kwargs)
        except Exception as e:
            raise e
            #self._logger.exception(f"{self._id} parse exception: {e}")
        self._logger.info(f"{self._id} parse finished.")
        return boundaries

    def _do_parsing(self, **kwargs):
        pass
class CSVBoundaryParser(BoundaryParser):
    def _do_parsing(self, **kwargs):
        '''
        Parses a CSV file that has the Name,WKT format.
        :param filename: str that is the full path to the CSV file.
        :return:
        '''
        filename = kwargs.get('filepath', None)
        logger = logging.getLogger()
        boundaries_tuples = []
        try:
            header = ['Name', 'WKT']
            with open(filename, "r") as boundaries_csv_file:
                csv_reader = csv.DictReader(boundaries_csv_file, fieldnames=header)
                for row in csv_reader:
                    polygon = geojson.loads(to_geojson(from_wkt(row['WKT'])))
                    boundaries_tuples.append((row['Name'], polygon))
        except Exception as e:
            logger.exception(e)

        return boundaries_tuples

class SHPBoundaryParser(BoundaryParser):
    def _do_parsing(self, **kwargs):
        shp_filepath = kwargs.get('filepath', None)
        boundaries_tuples = []
        if shp_filepath is not None:
            shp_dataframe = gpd.read_file(shp_filepath, engine='fiona')
            if shp_dataframe.crs.srs != 'EPSG:4326':
                shp_dataframe.to_crs(epsg=4326, inplace=True)
            for ndx, row in shp_dataframe.iterrows():
                bnd_json = geojson.loads(to_geojson(row['geometry']))
                if 'Name' in row:
                    boundaries_tuples.append((row['Name'], bnd_json))
                else:
                    self._logger.error(f"{self._id} File: {shp_filepath} has no Name field, default to filename.")
                    directory, filename = os.path.split(shp_filepath)
                    filename, exten = os.path.splitext(filename)
                    boundaries_tuples.append((f"{filename}_{ndx}", bnd_json))

        return boundaries_tuples

class JSONBoundaryParser(BoundaryParser):
    def _do_parsing(self, **kwargs):
        filename = kwargs.get('filepath', None)

        boundaries_tuples = []
        try:
            json_dataframe = gpd.read_file(filename, engine='fiona')
            '''
            for ndx, row in json_dataframe.iterrows():
                bnd_json = geojson.loads(to_geojson(row['geometry']))
                boundaries_tuples.append((row['Name'], bnd_json))
            '''
        except Exception as e:
            self._logger.exception(e)
        return boundaries_tuples


def find_bbox_from_boundaries(boundaries: [], buffer_percent: float):
    '''
    Computes the total extent of boundaries provided. If the buffer_percent is provided, the bbox is increased
    by that percentage.
    :param boundaries:
    :param buffer_percent:
    :return:
    '''
    bbox = None
    poly_list = []
    for boundary in boundaries:
        polygon = from_geojson(geojson.dumps(boundary[1]))
        poly_list.append(polygon)
    multi_polys = MultiPolygon(poly_list)
    buffer_poly = multi_polys.buffer(buffer_percent)
    minx, miny, maxx, maxy = buffer_poly.bounds
    '''
    combined_polygons = unary_union(poly_list)
    minx, miny, maxx, maxy = combined_polygons.bounds
    '''
    #
    '''
    if buffer_percent is not None:
        width = maxx - minx
        height = maxy - miny

        minx = minx - (width * buffer_percent)
        miny = miny - (height * buffer_percent)
        maxx = maxx + (width * buffer_percent)
        maxy = maxy + (height * buffer_percent)
    '''
    bbox = [(miny, minx), (maxy, maxx)]
    return bbox
