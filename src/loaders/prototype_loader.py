from loaders import *
from annotators import *
from files import *
from mods import *
import gc
import time
import os
from py2neo import Graph, authenticate

class PrototypeAggregateLoader:

	def __init__(self):
		graph = Graph('http://neo4j:neo4j@172.17.0.2:7474/db/data')

	def load_from_mods(self):
		mods = [FlyBase()]
		print("Gathering genes from each MOD.")
		for mod in mods:
			genes = mod.load_genes_prototype

	def index_data(self):
		print("Hello!")