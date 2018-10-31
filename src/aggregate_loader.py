import logging, coloredlogs
from loaders import *
from transactions import *

coloredlogs.install(level=logging.INFO,
    fmt='%(asctime)s %(levelname)s: %(name)s:%(lineno)d: %(message)s',
    field_styles={
        'asctime': {'color': 'green'}, 
        'hostname': {'color': 'magenta'}, 
        'levelname': {'color': 'white', 'bold': True}, 
        'name': {'color': 'blue'}, 
        'programname': {'color': 'cyan'}
    })

# This has to be done because the OntoBio module does not use DEBUG it uses INFO which spews output.
# So we have to set the default to WARN in order to "turn off" OntoBio and then "turn on" by setting 
# to DEBUG the modules we want to see output for.
logger = logging.getLogger(__name__)

class AggregateLoader(object):

    def run_loader(self):

        thread_pool = []

        for n in range(0, 4):
            trans_runner = Transaction()
            trans_runner.threadid = n
            trans_runner.daemon = True
            trans_runner.start()
            thread_pool.append(trans_runner)
        
        # The following order is REQUIRED for proper loading.
        logger.info("Creating indices.")
        Indicies().create_indices()

        [self.do_dataset, self.go_dataset] = OntologyLoader().run_loader()
        logger.info("OntologyLoader: Waiting for Queue to clear")
        Transaction.queue.join()

        modloader = ModLoader()
        modloader.run_bgi_loader()
        logger.info("ModLoader.run_bgi_loader: Waiting for Queue to clear")
        Transaction.queue.join()

        modloader.run_other_loaders(self.do_dataset, self.go_dataset)
        #modloader.run_other_loaders(None, None)
        logger.info("ModLoader.run_other_loaders: Waiting for Queue to clear")
        Transaction.queue.join()

        OtherDataLoader().run_loader()
        logger.info("OtherDataLoader: Waiting for Queue to clear")
        Transaction.queue.join()

if __name__ == '__main__':
    AggregateLoader().run_loader()
