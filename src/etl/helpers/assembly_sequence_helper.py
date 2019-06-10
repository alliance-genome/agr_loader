import logging, os, time
from pyfaidx import Fasta
from common import ContextInfo

logger = logging.getLogger(__name__)

class AssemblySequenceHelper(object):
    def __init__(self, assembly, data_manager):
        sub_type_string = assembly.replace('.', '').replace('_', '')
        assembly_to_subtype_map = {"R6.27": "FlyBaseAssemblyR6.27",
                                   "WBcel235": "WormBaseAssemblyWBcel235",
                                   "GRCz11": "ZFinAssemblyGRCz11",
                                   "GRCm38": "MGIAssemblyGRCm38",
                                   "Rnor_6.0": "RGDAssemblyRnor60"}
        sub_type = assembly_to_subtype_map[assembly]
        fasta_config = data_manager.get_config('FASTA')
        for sub_type_config in fasta_config.get_sub_type_objects():
            if not sub_type == sub_type_config.get_sub_data_type():
                continue
            else:
                filepath = sub_type_config.get_filepath()
                break
        self.assembly = assembly
        self.filepath = filepath
        fa = Fasta(filepath)
        while (len(fa.keys()) == 0):
            time.sleep(6)
            os.remove(filepath + ".fai")
            fa = Fasta(filepath)
        self.fa = fa

    def getAssembly(self):
        return self.assembly

    def getFilepath(self):
        return self.filepath

    def getChromosomes(self):
        return self.fa.keys()

    def getSequence(self, chromosome, first, second):
        if first > second:
           start = second
           end = first
        else:
           start = first
           end = second

        start = start - 1
        if chromosome.startswith("chr"):
            chromosome_str = chromosome[3:]
        else:
            chromosome_str = chromosome
        if chromosome_str in self.fa:
            return self.fa[chromosome_str][start:end].seq
        else:
            logger.info("Chr " + chromosome_str + " not in assembly " + self.assembly )
