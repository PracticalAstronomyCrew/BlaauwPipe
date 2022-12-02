from core.observation import Observation

from datetime import datetime
import json
import os
import numpy as np
import csv

import core.constants as cst
import core.printstatus as ps
from core.pluginsystem import Plugin

import logging

logger = logging.getLogger(__name__)
from blaauwpipe import BlaauwPipe

class Pending(Plugin):
    def __init__(self):
        super().__init__()
        self.title = "The Pending-Mechanism"
        self.call_level = 999
        self.command = "-p"
        self.command_full = "--pending"
        self.description = """ Core plugin that generated the master calibration frames from the cst.raw_dir
                               and stores them in cst.cor_dir. Loops over every possible binning (and filter 
                               if applicable) and created the master files for each cluster of frames.
                           """
        self.description_short = "Save raw calibration frames locally"

    def on_run(self, obs, plp):
        self.rerun_pending(obs, plp)

    @staticmethod
    def append_pending_log(pending_json):
        pd_log = cst.pending_log
        
        data = None
        if os.path.exists(pd_log):
            with open(pd_log) as json_file:
                try:
                    data = json.load(json_file)
                except:
                    data = {
                        "penders": []
                    }
        else:
            data = {
                "penders": []
            }
            
        if pending_json not in data["penders"]:
            data["penders"].append(pending_json)
        
        with open(pd_log, 'w') as outfile:
            json.dump(data, outfile, indent=4)
            
    #    # Create the pending log if it doesn't exist
    #     if not os.path.exists(pd_log):
    #         # Append a header
    #         clmns = np.array(["Date", "Frame type", "Binning", "Filter", "BIAS-AGE", "DARK-AGE", "FLAT-AGE", "Expires", "Path"])
    #         np.savetxt(pd_log, clmns.reshape(1, clmns.shape[0]), delimiter=", ", fmt="%s")
    #     # Append the new line to the pending log
    #     with open(pd_log, "ab") as f:
    #         np.savetxt(f, new_line.reshape(1, new_line.shape[0]), delimiter=", ", fmt="%s")

    def read_pending_log(self):
        pd_log = cst.pending_log
        
        data = None
        if os.path.exists(pd_log):
            with open(pd_log) as json_file:
                data = json.load(json_file)
                
    #     lines = []
    #     # Loop over each line and append its content to the list
    #     with open(pd_log, "r") as f:
    #         reader = csv.reader(f, delimiter=",")
    #         for i, line in enumerate(reader):
    #             lines.append(line)

        # Note that the first element is the header! This line is purposefully returned
        return data

    # def rerun_pending(self, obs, plp):
    #     pd_log = cst.pending_log
        
    #     # Read the current pending log
    #     data_json = self.read_pending_log()
    #     folder_date = obs.foldername.replace(cst.base_path, '').split(os.sep)[1]
    #     folder_datetime = datetime.strptime(folder_date, '%y%m%d')
        
    #     if not data_json:
    #         ps.warning("Pending log empty!")
    #         return
        
    # #     with open(pd_log, 'w') as outfile:
    # #         json.dump(data, outfile, indent=4)
        
    #     # Overwrite the pending log
    # #     with open(pd_log, "wb") as f:
    # #         # Re-append the header
    # #         np.savetxt(f, lines[0].reshape(1, lines[0].shape[0]), delimiter=",", fmt="%s")
        
    #     # Loop over every old line and check if it needs to be re-run 
    #     for pending_json in data_json["penders"]:
    #         # Unpack data and rerun the reduction process
    # #         date, frame_type, binning, fltr, bias_off, dark_off, flat_off, expiry, file_path = line
            
    #         # Reinitialise the observation object and working dir
    #         line_obs = Observation(pending_json["path"])
    #         line_working_dir = (os.path.split(pending_json["path"])[0]).replace(cst.tele_path, cst.base_path)
    #         tele_data_dir = line_working_dir.replace(cst.base_path, cst.tele_path)
            
    #         # Check its expiry date if is has one
    #         if not pending_json["expires"]:
    #             expiry_date = datetime.strptime(pending_json["expires"], "%d-%m-%Y")
    #             if expiry_date.date() < folder_datetime.date():
    #                 # We cannot hope to find a better version, like ever. So, we can 
    #                 # safely continue with new actions/plugins.
    #                 BlaauwPipe.run_plugins_single(obs, plp, pending_json.path, "pending")
            
    #         # Re-reduce a light file
    #         if pending_json["Frame type"] == "Light file":
    #             max_days_off = 365
    #             # Cut on calculation time if possible
    #             try:
    #                 max_days_off = abs(max(int(pending_json["BIAS-AGE"]), int(pending_json["DARK-AGE"]), int(pending_json["FLAT-AGE"])))
    #             except ValueError: pass
    #             # Re-reduce light file. Will add a new entry in the pending log, but hopefully with a
    #             # lower max_days_off. When the time's there, it will expire.
    #             Reduction.reduce_img(obs, plp, pending_json["Path"], max_days_off)
            
    #         # Re-reduce dark frame
    #         elif pending_json["Frame type"] == "Dark file":
    #             line_working_dir = (os.path.split(os.path.split(pending_json["Path"])[0])[0]).replace(cst.tele_path, cst.base_path)
    #             Calibration.recreate_mdark(obs, plp, pending_json["Path"], pending_json["Binning"], pending_json["Filter"])
    #         # Re-reduce flat field
    #         elif pending_json["Frame type"] == "Flat file":
    #             line_working_dir = (os.path.split(os.path.split(pending_json["Path"])[0])[0]).replace(cst.tele_path, cst.base_path)
    #             Calibration.recreate_mflat(obs, plp, pending_json["Path"], pending_json["Binning"], pending_json["Filter"])