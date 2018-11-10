from files import S3File, TXTFile, TARFile, Download
import os, logging, sys
from .sub_type_config import SubTypeConfig

logger = logging.getLogger(__name__)

class DataTypeConfig(object):

    def __init__(self, data_type, submission_system_data):
        self.data_type = data_type
        self.submission_system_data = submission_system_data

        # TODO These will be set by the config YAML.
        self.commit_size = 2500
        self.batch_size = 10000

        self.list_of_subtype_objects = []

        logger.info(data_type)
        logger.info(submission_system_data)

    def get_data(self):
        # Grab the data (TODO validate) and create sub_type objects.
        # Some of this algorithm is temporary.
        # Files from the submission system will arrive without the need for unzipping, etc.

        path = 'tmp'

        for downloadable_item in self.submission_system_data:
            if downloadable_item[1] is not None:
                if downloadable_item[1].startswith('http'):
                    download_filename = os.path.basename(downloadable_item[2])
                    download_object = Download(path, downloadable_item[1], download_filename) # savepath, urlToRetieve, filenameToSave
                    download_object.get_downloaded_data()
                else:
                    S3File(downloadable_item[1], path).download()
                    if downloadable_item[1].endswith('tar.gz'):
                        tar_object = TARFile(path, downloadable_item[1])
                        tar_object.extract_all()
            else: 
                logger.warn('No download path specified, assuming download is not required.')

            try:
                sub_type = SubTypeConfig(downloadable_item[0], path + '/' + downloadable_item[2])
            except TypeError:
                sub_type = SubTypeConfig(downloadable_item[0], None) # If we don't have a path.
            self.list_of_subtype_objects.append(sub_type)

    def running_etl(self):
        return True

    def get_neo4j_commit_size(self):
        return self.commit_size

    def get_generator_batch_size(self):
        return self.get_generator_batch_size    

    def check_for_single(self):
        if len(self.list_of_subtype_objects) > 1:
            logger.critical('Called for single item in object containing multiple children.')
            logger.critical('Please check the function calling for this single item.')
            sys.exit(-1)
        else:
            pass

    def get_single_filepath(self):
        self.check_for_single()
        return self.list_of_subtype_objects[0].get_filepath()

    def get_sub_type_objects(self):
        return self.list_of_subtype_objects