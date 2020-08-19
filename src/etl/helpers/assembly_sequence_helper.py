"""Assembly Sequence Helper"""

import logging
import os
import sys
import time

from pyfaidx import Fasta


class AssemblySequenceHelper():
    """Assembly Sequence Helper"""

    logger = logging.getLogger(__name__)

    def __init__(self, assembly, data_manager):
        sub_type = assembly.replace('.', '').replace('_', '')
        if sub_type.startswith('R6'):
            sub_type = 'R627'
        fasta_config = data_manager.get_config('FASTA')

        for sub_type_config in fasta_config.get_sub_type_objects():
            if not sub_type == sub_type_config.get_sub_data_type():
                self.debug.info(sub_type_config.get_sub_data_type())
                continue
            filepath = sub_type_config.get_filepath()
            self.debug.info(filepath)
            break

        if filepath is None:
            self.logger.warning("Can't find Assembly filepath for %s", assembly)
            sys.exit(3)

        self.assembly = assembly
        self.filepath = filepath
        fasta_data = Fasta(filepath)
        while len(fasta_data.keys()) == 0:
            time.sleep(6)
            os.remove(filepath + ".fai")
            fasta_data = Fasta(filepath)
        self.fasta_data = fasta_data

    def get_assembly(self):
        """Assembly"""

        return self.assembly

    def get_filepath(self):
        """Filepath"""

        return self.filepath

    def get_chromosomes(self):
        """Chromosomes"""

        return self.fasta_data.keys()

    def get_sequence(self, chromosome, first, second):
        """Sequence"""

        if first > second:
            start = second
            end = first
        else:
            start = first
            end = second

        start = start - 1
        if chromosome in self.fasta_data:
            return self.fasta_data[chromosome][start:end].seq

        self.logger.warning("Chromosome %s not in assembly %s", chromosome, self.assembly)
