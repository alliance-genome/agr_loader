'''CSV Transactor'''

from contextlib import ExitStack
import csv
import logging
import os


class CSVTransactor():
    '''CSV Transactor'''

    logger = logging.getLogger(__name__)

    @staticmethod
    def save_file_static(generator, generator_file_list):
        '''Save File Static'''

        with ExitStack() as stack:
            # Open all necessary CSV files at once.
            open_files = [stack.enter_context(open(os.path.join('tmp', file_name),
                                                   'w',
                                                   encoding='utf-8'))
                          for [query, file_name] in generator_file_list]


            # Create a list with 'None' placeholder entries.
            csv_file_writer = [None] * len(open_files)
            for generator_entry in generator:
                for index, individual_list in enumerate(generator_entry):
                    current_filename = open_files[index].name  # Our current CSV output file.

                    # Remove None's from list which cause the write rows to crash
                    individual_list = [x for x in individual_list if x is not None]


                    if len(individual_list) == 0:
                        CSVTransactor.logger.debug("No data found when writing to csv! %s: %s",
                                                   'Skipping output file',
                                                   current_filename)
                        continue

                    if csv_file_writer[index] is None:  # If we haven't yet created a DictWriter
                        # for this particular file.
                        CSVTransactor.logger.debug("Saving data to output file: %s",
                                                   current_filename)
                        try:
                            csv_file_writer[index] = csv.DictWriter(\
                                                          open_files[index],
                                                          fieldnames=list(individual_list[0]),
                                                          quoting=csv.QUOTE_NONNUMERIC)
                            csv_file_writer[index].writeheader()
                        except csv.Error as error:
                            CSVTransactor.logger.critical("Couldn't write to file: %s ",
                                                          current_filename)
                            CSVTransactor.logger.critical(error)

                    # Write the remainder of the list
                    csv_file_writer[index].writerows(individual_list)
                    # content for this iteration.
                    # logger.info("%s: Finished Writting %s entries to file: %s",
                    #             self._get_name(),
                    #             len(individual_list),
                    #             current_filename)
