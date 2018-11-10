from files import S3File, TXTFile, TARFile, Download
import os, logging

logger = logging.getLogger(__name__)

class DataTypeConfig(object):

    def __init__(self, data_type, data_type_content, submission_system_data):
        self.data_type = data_type
        self.data_type_content = data_type_content
        self.submission_system_data = submission_system_data

        # TODO These will be set by the config YAML.
        self.commit_size = 2500
        self.batch_size = 10000

        logger.info(data_type)
        logger.info(data_type_content)
        logger.info(submission_system_data)

    def get_data(self):
        path = 'tmp'

        # Some of this algorithm is temporary.
        # Files from the submission system will arrive without the need for unzipping, etc.

        for downloadable_item in self.submission_system_data:
            if downloadable_item[1] is not None:
                if downloadable_item[1].startswith('http'):
                    download_filename = os.path.basename(downloadable_item[1])
                    download_object = Download(path, downloadable_item[1], download_filename) # savepath, urlToRetieve, filenameToSave
                    download_object.get_downloaded_data()
                else:
                    S3File(downloadable_item[1], path).download()
                    if downloadable_item[1].endswith('tar.gz'):
                        tar_object = TARFile(path, downloadable_item[1])
                        tar_object.extract_all()
                # return TXTFile(path + "/" + path_to_file).get_data()
            else: 
                logger.warn('No download path specified, assuming download is not required.')

    def running_etl(self):
        return True

    def get_neo4j_commit_size(self):
        return self.commit_size

    def get_generator_batch_size(self):
        return self.get_generator_batch_size    

    def get_configs(self):
        return self.submission_system_data