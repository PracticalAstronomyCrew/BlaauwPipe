import os

#-----------------------------------------#
#                General                  #
#-----------------------------------------#
version = "0.4.0"
date = "16-09-2022"
logo = """
 ____   _                                        _              
| __ ) | |  __ _   __ _  _   _ __      __ _ __  (_) _ __    ___ 
|  _ \ | | / _` | / _` || | | |\ \ /\ / /| '_ \ | || '_ \  / _ \\
| |_) || || (_| || (_| || |_| | \ V  V / | |_) || || |_) ||  __/
|____/ |_| \__,_| \__,_| \__,_|  \_/\_/  | .__/ |_|| .__/  \___|
                                         |_|       |_|          
"""
authors = ["Fabian Gunnink"]
cr = f"Version {version} ({date}), Copyright 2022 © {', '.join(authors)}"

#-----------------------------------------#
#          Telescope directories          #
#-----------------------------------------#

# Path constants associated with the telescope datadirs
tele_path = os.path.normpath("/net/vega/data/users/observatory/images/")
tele_subdirs = ("ST-7", "STL-6303E")
tele_subsubdirs = ("g", "i")


#-----------------------------------------#
#        Save directories/files           #
#-----------------------------------------#

# User-specific path constants
#base_path = os.path.normpath("/Users/users/gunnink/PAC/Data/")
base_path = os.path.normpath("/net/dataserver3/data/users/noelstorr/blaauwpipe/")
raw_dir = "Raw"
reduced_dir = "Reduced"
correction_dir = "Correction"
pending_log = "pending_log.json"
logfile = "logfile.txt"


#-----------------------------------------#
#            Astrometry Config            #
#-----------------------------------------#

api_key = "spkngclyydujeuke"


#-----------------------------------------#
#           New header keywords           #
#-----------------------------------------#

# Path to the original raw file as stored on the telescope dataserver
HKW_traw = "BP-TRAW"
# Path to the raw file as stored on the pipeline dataserver
HKW_praw = "BP-PRAW"
# Path to the reduced file as stored on the pipeline dataserver
HKW_pred = "BP-PRED"

# Specifies the number of files used in combining process
# to create a master correction frame
HKW_nsource = "BP-SRCN"
# An integer n should be appended to the following keyword,
# which then gives the path to the n'th used source.
HKW_source = "BP-SRC"

# Gives the path to the used master bias frame
HKW_mbias = "BP-MBIAS"
# Gives the relative age of the master bias in days.
HKW_mbias_age = "BP-MBAGE"

# Gives the path to the used master dark frame
HKW_mdark = "BP-MDARK"
# Gives the relative age of the master dark in days.
HKW_mdark_age = "BP-MDAGE"

# Gives the path to the used master flat frame
HKW_mflat = "BP-MFLAT"
# Gives the relative age of the master flat in days.
HKW_mflat_age = "BP-MFAGE"