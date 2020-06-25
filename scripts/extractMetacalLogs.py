import json, re, glob, os
import argparse

parser = argparse.ArgumentParser(description='extract metacal data')
parser.add_argument("--indir", required=True, type=str, help="input directory")
parser.add_argument("--outfile", required=True, type=str, help="output json")
args = parser.parse_args()

datePattern= re.compile(r"\* Started on:")
metacalMaxPattern=re.compile(r"processDeblendedCoaddsMetacalMax INFO: index: 000000\/")
metacalMaxTimePattern=re.compile("processDeblendedCoaddsMetacalMax INFO: time:")
metacalMaxTimePerPattern=re.compile("processDeblendedCoaddsMetacalMax INFO: time per:")
metacalMaxSuccessPattern=re.compile("successful execution of processDeblendedCoaddsMetacalMax.py")
ngmixMaxPattern = re.compile(r"processDeblendedCoaddsNGMixMax INFO: index: 000000\/")
ngmixMaxTimePattern = re.compile("processDeblendedCoaddsNGMixMax INFO: time:")
ngmixMaxTimePerPattern = re.compile("processDeblendedCoaddsNGMixMax INFO: time per:")
ngmixMaxSuccessPattern=re.compile("successful execution of processDeblendedCoaddsNGMixMax.py")
patchPattern= re.compile("PATCH=")
slotPattern = re.compile("NSLOTS=")
streamPathPattern = re.compile("PIPELINE_STREAMPATH=")
tractPattern = re.compile("TRACT=")
cpuPattern = re.compile(r"\*   CPU time:")
vmemPattern = re.compile(r"\*   vmem:")
maxvmemPattern = re.compile(r"\*   maxvmem:")
maxrssPattern = re.compile(r"\*   maxrss:")
gbPattern = re.compile(r"\wB")
appendixPattern = re.compile(r"\(\w\)")
maxfevPattern = re.compile("Number of calls to function has reached maxfev =")
skipPattern = re.compile("Skipping tract")
secPattern= re.compile(r"\w+ seconds")
justsecPattern = re.compile(" seconds")
secParenPattern = re.compile(r"\(\w+ seconds\)")
naPattern = re.compile("N/A")

outlist = []


fileIter = glob.iglob(os.path.join(args.indir,'logFile.txt'))
for f in fileIter :
    with open (f, 'rt') as myfile:
        for _ in range(60):
            next(myfile)
        logfile=f
        date=None
        patch=None
        deblendedsources=None
        metacalMax_detections=None
        metacalMax_success=False
        metacalMax_time=None
        metacalMax_timeunits="min"
        metacalMax_timeper=None
        metacalMax_timeperunits="sec"
        ngmixMax_detections=None
        ngmixMax_success=False
        ngmixMax_time=None
        ngmixMax_timeunits="min"
        ngmixMax_timeper=None
        ngmixMax_timeperunits="sec"
        slots=None
        streamPath=None
        tract=None
        cputime=None
        cpuseconds=None
        vmem=None
        maxvmem=None
        maxrss = None
        maxrssunits = None
        maxvmemunits = None
        vmemunits = None
        skiptract = False
        skiptractstr = None
        maxfev = False
        maxfevstr = None

        foundfirstcpu = False

        for myline in myfile:
            curline=myline.strip()
            if (patch == None) and patchPattern.search(curline) != None:
                patch=curline.lstrip("PATCH=")
                continue
            elif (tract==None) and tractPattern.search(curline) != None:
                tract=curline.lstrip("TRACT=")
                continue
            elif (slots==None) and slotPattern.search(curline) != None:
                slots=curline.lstrip("NSLOTS=")
                continue
            elif (skiptract==False) and skipPattern.search(curline) != None:
                skiptract = True
                skiptractstr = curline.strip()
                continue
            elif (maxfev==False) and maxfevPattern.search(curline) != None:
                maxfev = True
                maxfevstr = curline.strip()
                continue
            elif (metacalMax_success==False) and metacalMaxSuccessPattern.search(curline) != None:
                metacalMax_success = True
                continue
            elif (metacalMax_detections==None) and metacalMaxPattern.search(curline) != None:
                metacalMax_detections= re.sub(metacalMaxPattern, '', curline)
                continue
            elif (metacalMax_time==None) and metacalMaxTimePattern.search(curline) != None:
                metacalMax_time= re.sub(metacalMaxTimePattern, '', curline).strip().split(" min")[0]
                continue
            elif (metacalMax_timeper==None) and metacalMaxTimePerPattern.search(curline) != None:
                metacalMax_timeper= re.sub(metacalMaxTimePerPattern, '', curline).strip().split(" sec")[0]
                continue
            elif (ngmixMax_success==False) and ngmixMaxSuccessPattern.search(curline) != None:
                ngmixMax_success = True
                continue
            elif (ngmixMax_detections==None) and ngmixMaxPattern.search(curline) != None:
                ngmixMax_detections= re.sub(ngmixMaxPattern, '', curline)
                continue
            elif (ngmixMax_time==None) and ngmixMaxTimePattern.search(curline) != None:
                ngmixMax_time= re.sub(ngmixMaxTimePattern, '', curline).strip().split(" min")[0]
                continue
            elif (ngmixMax_timeper==None) and ngmixMaxTimePerPattern.search(curline) != None:
                ngmixMax_timeper= re.sub(ngmixMaxTimePerPattern, '', curline).strip().split(" sec")[0]
                continue
            elif (streamPath==None) and streamPathPattern.search(curline) != None:
                streamPath=curline.lstrip("PIPELINE_STREAMPATH=")
                continue
            elif (date==None) and datePattern.search(curline) != None:
                dateTemp = curline.lstrip("* Started on:")
                dateTemp = dateTemp.strip("*")
                date=dateTemp.strip()
                continue
            elif (cputime==None) and cpuPattern.search(curline) != None:
                if (not foundfirstcpu):
                    foundfirstcpu = True
                    continue
                cputimeTemp = re.sub(cpuPattern, '', curline).strip().rstrip("*").rstrip()
                cpusecondsTemp = secPattern.findall(cputimeTemp)[0]
                cpusecondsTemp2 = cpusecondsTemp
                cpuseconds = re.sub(justsecPattern, '', cpusecondsTemp2).strip()
                cputime = re.sub(secParenPattern, '', cputimeTemp).strip()
                continue
            elif (vmem==None) and vmemPattern.search(curline) != None:
                # Check for N/A
                if naPattern.search(curline):
                    continue
                vmemTemp= re.sub(vmemPattern, '', curline).strip().rstrip("*").rstrip()
                vmemTemp2 = re.sub(appendixPattern, '', vmemTemp).strip()
                vmemunits = gbPattern.findall(vmemTemp2)[0]
                vmem = re.sub(gbPattern, '', vmemTemp2).strip()
                continue
            elif (maxvmem==None) and maxvmemPattern.search(curline) != None:
                if naPattern.search(curline):
                    continue
                maxvmemTemp= re.sub(maxvmemPattern, '', curline).strip().rstrip("*").rstrip()
                maxvmemTemp2 = re.sub(appendixPattern, '', maxvmemTemp).strip()
                maxvmemunits = gbPattern.findall(maxvmemTemp2)[0].strip()
                maxvmem = re.sub(gbPattern, '', maxvmemTemp2).strip()
                continue
            elif (maxrss==None) and maxrssPattern.search(curline) != None:
                if naPattern.search(curline):
                    continue
                maxrssTemp= re.sub(maxrssPattern, '', curline).strip().rstrip("*").rstrip()
                maxrssTemp2 = re.sub(appendixPattern, '', maxrssTemp).strip()
                maxrssunits = gbPattern.findall(maxrssTemp2)[0].strip()
                maxrss = re.sub(gbPattern, '', maxrssTemp2).strip()
                continue

        # Done with this file

        # Find number of deblended sources
        if (metacalMax_detections != None) and (ngmixMax_detections != None):
            if (int(metacalMax_detections) == int(ngmixMax_detections)):
                deblendedsources = int(metacalMax_detections)
            else:
                print("Number of deblended sources mismatch\n")
        elif (metacalMax_detections == None):
            print("no metacal deblended sources logged")
        elif (ngmixMax_detections == None):
            print("no ngmix deblended sources logged")

        outdict={
                "logfile": logfile,
                "date": date, 
                "deblendedsources":deblendedsources,
                "metacalmax_success": metacalMax_success,
                "metacalmax_detections": metacalMax_detections,
                "metacalmax_time": metacalMax_time,
                "metacalmax_time_units": metacalMax_timeunits,
                "metacalmax_timeper": metacalMax_timeper,
                "metacalmax_timeper_units": metacalMax_timeperunits,
                "ngmixmax_success": ngmixMax_success,
                "ngmixmax_detections": ngmixMax_detections,
                "ngmixmax_time": ngmixMax_time,
                "ngmixmax_time_units": ngmixMax_timeunits,
                "ngmixmax_timeper": ngmixMax_timeper,
                "ngmixmax_timeper_units": ngmixMax_timeperunits,
                "maxfev": maxfev,
                "maxfevstr": maxfevstr,
                "tract":tract, 
                "patch":patch, 
                "slots":slots, 
                "streampath":streamPath,
                "skiptract":skiptract,
                "skipttractstr":skiptractstr,
                "cputime":cputime,
                "cpuseconds":cpuseconds,
                "vmem":vmem,
                "vmemunits":vmemunits,
                "maxvmem":maxvmem,
                "maxvmemunits":maxvmemunits,
                "maxrss":maxrss,
                "maxrssunits":maxrssunits
                }
        outlist.append(outdict)



with open(args.outfile, "w") as outfile: 
    json.dump(outlist, outfile)                     




