from extractors import *
import xmltodict, json, urllib
from files import S3File, TARFile, JSONFile, Download
from etl.helpers import ETLHelper
from services import RetrieveGeoXrefService
from test import TestObject
import gzip, time
import csv
import os, json
import logging

logger = logging.getLogger(__name__)

class MOD(object):

    def __init__(self, batch_size, species):
        self.batch_size = batch_size
        self.species = species

    def load_wt_expression_objects_mod(self, expressionFileName, loadFile):
        data = WTExpressionExt().get_wt_expression_data(loadFile, expressionFileName, 10000, self.testObject)
        return data
