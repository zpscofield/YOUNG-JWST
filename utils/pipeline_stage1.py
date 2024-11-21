import os
# import json
import yaml
import numpy as np
from jwst.pipeline import Detector1Pipeline
from tqdm.auto import tqdm
import logging
import sys
from multiprocessing import current_process
import argparse

with open('config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)

os.environ['CRDS_PATH'] = config['crds_path']
os.environ['CRDS_SERVER_URL'] = config['crds_server_url']

log_file_path = "pipeline.log"

if current_process().name == "MainProcess":
    with open(log_file_path, 'a') as log_file:
        log_file.write("------------------\n")
        log_file.write("Stage 1 Processing\n")
        log_file.write("------------------\n\n")

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
file_handler = logging.FileHandler(log_file_path, mode='a')  # Append mode
file_handler.setFormatter(formatter)
log.addHandler(file_handler)

crds_log = logging.getLogger("CRDS")
crds_log.setLevel(logging.INFO)
crds_handler = logging.FileHandler(log_file_path, mode='a')
crds_handler.setFormatter(formatter)
crds_log.addHandler(crds_handler)

stpipe_log = logging.getLogger("stpipe")
stpipe_log.setLevel(logging.INFO)
stpipe_handler = logging.FileHandler(log_file_path, mode='a')
stpipe_handler.setFormatter(formatter)
stpipe_log.addHandler(stpipe_handler)

for logger in [log, crds_log, stpipe_log]:
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            logger.removeHandler(handler)

def main(img, output_dir):

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    result = Detector1Pipeline.call(
        img, 
        steps={
            'ramp_fit': {'maximum_cores': config['ramp_fit_cores']}, 
            'jump': {'maximum_cores': config['jump_cores']}
        }, 
        output_dir=output_dir, 
        save_results=True
    )

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Stage 1 of the JWST data reduction pipeline.')
    parser.add_argument('--output_dir', type=str, help='Directory where output will be written')
    args = parser.parse_args()

    path = config['uncal_path']
    file_list1 = os.listdir(path)
    uncal_list = [file for file in file_list1 if file.endswith('uncal.fits')]
    uncal_list = np.sort(uncal_list)
    for i, img in enumerate(tqdm(uncal_list, file=sys.stderr)):
        main(os.path.join(path, img), args.output_dir)


    