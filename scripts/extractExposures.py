import json, glob, os
import numpy as np
from astropy.io import fits
import argparse

parser = argparse.ArgumentParser(description='extract exposure data')
parser.add_argument("--indir", required=True, type=str, help="input directory")
parser.add_argument("--outfile", required=True, type=str, help="output json")
args = parser.parse_args()

outlist = []


fileIter = glob.iglob(os.path.join(args.indir,'?,?_nImage.fits'))
for f in fileIter :
    tract = None
    patch = None
    band=None
    band_median=None
    band_mean=None
    band_min=None
    band_max=None
    logfile=f
    f_split=f.split('/')
    filename = f_split[-1]
    tract = f_split[-2]
    band = f_split[-3]
    patch = filename.split('_')[0]
    image_data = fits.getdata(f)
    band_min = np.min(image_data).item()
    band_max = np.max(image_data).item()
    band_mean = np.mean(image_data).item()
    band_median = np.median(image_data).item()


    outdict={
        "file": logfile,
        "tract":tract, 
        "patch":patch, 
        "band": band,
        "min": band_min,
        "max": band_max,
        "mean": band_mean,
        "median": band_median
        }
    outlist.append(outdict)



with open(args.outfile, "w") as outfile: 
    json.dump(outlist, outfile)                     




