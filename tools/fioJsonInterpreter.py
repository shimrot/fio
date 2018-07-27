#!/usr/bin/env python3

"""
class to import and allow examination or conversion of fio json output to a normal or terse like result.
"""

import io, re, sys
import json
from pathlib import Path
from collections import OrderedDict

__version__ = 0.2


class FioJsonFile():
    """
    Class to import an fioJson file to allow examination of values, and also allow conversion to a
    "normal", or "terse" like output.
    """
    def __init__(self, jsonpath):
        self.fpath = Path(jsonpath)
        with self.fpath.open() as f:
            jsonStream, self.normalStream, self.fileContainsNormalResults = self._splitJsonNormal(f)
            self.data = json.load(jsonStream, object_pairs_hook=OrderedDict)


    @property
    def bothJsonAndNormal(self):
        """
        Return whether the input file contains both json data and normal status results.
        If the file contains "normal" results, then can use the "normal" method to get the
        non-json text lines.  Otherwise, can use "normal" to get the possible lines which are
        not json, but would need to use the "normalize" method to get "normal like" lines.
        """
        return self.fileContainsNormalResults


    @property
    def objs(self):
        """
        return dict and list heirarchy from json and json+ output
        The structure of the returned is similar to this example::
            {   'fio version': 'fio-3.5-56-g9634-dirty',
                'timestamp': 1526325132,
                'timestamp_ms': 1526325132117,
                'time': 'Mon May 14 14:12:12 2018',
                'global options': {   'bs': '32KB',
                                      'direct': '1',
                                      ...
                                },
                'jobs': [   {   'jobname': '32KB RandWr QD=8',
                                'groupid': 0,
                                'error': 0,
                                'eta': 0,
                                'elapsed': 61,
                                'job options': {   'name': '32KB RandWr QD=8',
                                                   ...
                                               },
                                'mixed': {   'io_bytes': 49613012992,  # or read,write or trim
                                             'io_kbytes': 48450208,
                                             'bw_bytes': 826869768,
                                             'bw': 807490,
                                             'iops': 25234.062766,
                                             'runtime': 60001,
                                             'total_ios': 1514069,
                                             'short_ios': 0,
                                             'drop_ios': 0,
                                             'slat_ns': {   'min': 5014,
                                                            'max': 39515,
                                                            'mean': 8336.447085,
                                                            'stddev': 2613.125673
                                                        },
                                             'clat_ns': {   'min': 190662,
                                                            'max': 12726083,
                                                            'mean': 306308.172564,
                                                            'stddev': 215443.677113,
                                                            'percentile': {   '1.000000': 250880,
                                                                              '5.000000': 276480,
                                                                               ...,
                                                                              '99.990000': 10682368},
                                                            'bins': {   '191488': 1,
                                                                        ...
                                                                        '12779520': 1
                                                                    }
                                                        },
                                             'lat_ns': {   'min': 208196,
                                                           'max': 12731972,
                                                           'mean': 314910.265908,
                                                           'stddev': 215395.586333
                                                       },
                                             'bw_min': 767040,
                                             'bw_max': 851136,
                                             'bw_agg': 100.0,
                                             'bw_mean': 807506.613445,
                                             'bw_dev': 13787.314203,
                                             'bw_samples': 119,
                                             'iops_min': 23970,
                                             'iops_max': 26598,
                                             'iops_mean': 25234.605042,
                                             'iops_stddev': 430.841573,
                                             'iops_samples': 119
                                         },
                                'usr_cpu': 12.113333,
                                'sys_cpu': 29.941667,
                                'ctx': 1340458,
                                'majf': 0,
                                'minf': 35,
                                'iodepth_level': {   '1': 0.1,
                                                     '2': 0.1,
                                                     ...
                                                     '>=64': 0.0
                                                 },
                                'latency_ns': {   '2': 0.0,
                                                  ...
                                                  '1000': 0.0},
                                'latency_us': {   '2': 0.0,
                                                  ...
                                                  '1000': 0.013672},
                                'latency_ms': {   '2': 0.01,
                                                  ...
                                                  '>=2000': 0.0},
                                'latency_depth': 8,
                                'latency_target': 0,
                                'latency_percentile': 100.0,
                                'latency_window': 0
                            }
                        ],  # end of jobs
                'disk_util': [   {   'name': 'sdb',
                                     'read_ios': 123,
                                     'write_ios': 1511355,
                                     'read_merges': 0,
                                     'write_merges': 0,
                                     'read_ticks': 11,
                                     'write_ticks': 454510,
                                     'in_queue': 453905,
                                     'util': 99.906614
                                 }
                             ]
            }
        """
        return self.data


    @property
    def avgClat(self):
        """
        Return average Clat (ms) across all jobs (scaled based on total IOs performed)
        """
        return self._avgMetric('clat_ns') / 1000000.0


    @property
    def avgLat(self):
        """
        Return average lat (ms) across all jobs (scaled based on total IOs performed)
        """
        return self._avgMetric('lat_ns') / 1000000.0


    @property
    def iops(self):
        """
        Return totol iops across all jobs and directions
        """
        return self._sumMetric('iops')


    @property
    def mibps(self):
        """
        Return totol MiBps (binary) across all jobs and directions
        """
        return self._sumMetric('bw_bytes') / 1048576.0


    def normalLines(self):
        """
        return the file lines which are NOT json.
        Be sure to check method "bothJsonAndNormal" if unsure whether the results contain
        both Json and Normal results (Json is always assumed).
        You may want to still get the non-json lines for output even if there is no "normal"
        status results.  In which case, you can use the "normalized" method to get simulated normal
        results.
        """
        return self.normalStream.getvalue()


    def normalized(self):
        """
        Return a string simulating the fio "normal" output from the json data.
        Unfortunately, the json data does not contain all the data provided in the normal output.
        Specifically:

           * The 'jobs=' number which is in the status output.  Thus, cannot discover the
             actual number of jobs which were run from status (I think)
           * The 'pid' number for the job, which you likely do not need.
           * the IO submit/complete percentage lines cannot be created (I think)
           * The "run status" group lines:
               -- I might be able to generate these, but am not currently.
           * The disk stats lines:
               -- This data exists in the json data, but I'm not currently generating them.
        """
        out = io.StringIO()

        #self._jobStartLines(out)  Let's skip creating these - not normally interesting
        print(self.data["fio version"], file=out)
        print(file=out)
        if 'jobs' in self.data:
            for dJob in self.data['jobs']:
                self._normalJobStats(out, dJob)
            self._normalRunStatus(out, self.data['jobs'])
            self._normalDiskStats(out, self.data['disk_util'])

        elif 'client_stats' in self.data:
            for dJob in self.data['client_stats']:
                self._normalJobStats(out, dJob, clients=True)
                print("<{}>".format(dJob['hostname']), file=out)
            self._normalRunStatus(out, self.data['client_stats'])
            self._normalDiskStats(out, self.data['disk_util'])

        else:
            raise RuntimeError("Missing 'jobs' or 'client_stats' key in json results for file '{}'".format(str(self.fpath)))

        contents = out.getvalue()
        out.close()
        return contents


    def terseList(self):
        """
        Not Really Tested much - don't really have need for it, but a complete implementation
        should include this output format.

        Note: this only works as long as --unified_rw_reporting = 0
        Output format would need be able to vary if results are mixed.

        Example Terse Format Output:
            3;fio-3.6;16MB MultiStream-SeqRd QD=2;0;0;269074432;2242137;136;120008;1533;2041;1792.424773;59.708645;2025;749588;12819.214959;9365.443748;1.000000%=10158;5.000000%=10682;10.000000%=10944;20.000000%=11337;30.000000%=11599;40.000000%=11730;50.000000%=11993;60.000000%=12124;70.000000%=12386;80.000000%=12779;90.000000%=13434;95.000000%=15663;99.000000%=36438;99.500000%=42729;99.900000%=60030;99.950000%=103284;99.990000%=742391;0%=0;0%=0;0%=0;3791;751211;14612.207651;9362.285456;98304;2523136;99.986777%;2241840.525000;225700.336782;0;0;0;0;0;0;0.000000;0.000000;0;0;0.000000;0.000000;1.000000%=0;5.000000%=0;10.000000%=0;20.000000%=0;30.000000%=0;40.000000%=0;50.000000%=0;60.000000%=0;70.000000%=0;80.000000%=0;90.000000%=0;95.000000%=0;99.000000%=0;99.500000%=0;99.900000%=0;99.950000%=0;99.990000%=0;0%=0;0%=0;0%=0;0;0;0.000000;0.000000;0;0;0.000000%;0.000000;0.000000;0.067496%;24.595232%;16364;0;1641;0.1%;112.5%;0.0%;0.0%;0.0%;0.0%;0.0%;0.00%;0.00%;0.00%;0.00%;0.00%;0.00%;0.00%;0.00%;0.00%;0.00%;0.00%;0.04%;0.82%;95.68%;3.22%;0.19%;0.04%;0.00%;0.01%;0.00%;0.00%;0.00%;sdb;1181879;0;0;0;4003675;0;4000980;99.96%;00001-1: RA-WT-IOD-WCD
        """
        # dict of header names to json path to data or function to calculate
        dTerse = OrderedDict([
            ('fio_version', 'fio version')
            ])

        # list of entries under 'jobs'
        dJobs = OrderedDict([
             ('jobname',         'jobname')
            ,('groupid',        'groupid')
            ,('error',          'error')
            ])

        dDirections = OrderedDict([
             ('_kb',            'io_kbytes')
            ,('_bandwidth',     'bw')  # in KiBs
            ,('_iops',          'iops')
            ,('_runtime_ms',    'runtime')  # in ms
            ,('_slat_min',      'slat_ns/min')
            ,('_slat_max',      'slat_ns/max')
            ,('_slat_mean',     'slat_ns/mean')
            ,('_slat_dev',      'slat_ns/stddev')
            ,('_clat_min',      'clat_ns/min')
            ,('_clat_max',      'clat_ns/max')
            ,('_clat_mean',     'clat_ns/mean')
            ,('_clat_dev',      'clat_ns/stddev')
            ,('_clat_pct',      self._getDirectionPercentiles)
            ,('_lat_min',       'lat_ns/min')
            ,('_lat_max',       'lat_ns/max')
            ,('_lat_mean',      'lat_ns/mean')
            ,('_lat_dev',       'lat_ns/stddev')
            ,('_bw_min',        'bw_min')
            ,('_bw_max',        'bw_max')
            ,('_bw_agg_pct',    'bw_agg')
            ,('_bw_mean',       'bw_mean')
            ,('_bw_dev',        'bw_dev')
            ])

        dJobRest = OrderedDict([
             ('cpu_user',           'usr_cpu')
            ,('cpu_sys',            'sys_cpu')
            ,('cpu_csw',            'ctx')
            ,('cpu_mjf',            'majf')
            ,('cpu_minf',           'minf')
            ,('iodepth_1',          'iodepth_level/1')
            ,('iodepth_2',          'iodepth_level/2')
            ,('iodepth_4',          'iodepth_level/4')
            ,('iodepth_8',          'iodepth_level/8')
            ,('iodepth_16',         'iodepth_level/16')
            ,('iodepth_32',         'iodepth_level/32')
            ,('iodepth_64',         'iodepth_level/>=64')
            ,('lat_2us',            'latency_us/2')
            ,('lat_4us',            'latency_us/4')
            ,('lat_10us',           'latency_us/10')
            ,('lat_20us',           'latency_us/20')
            ,('lat_50us',           'latency_us/50')
            ,('lat_100us',          'latency_us/100')
            ,('lat_250us',          'latency_us/250')
            ,('lat_500us',          'latency_us/500')
            ,('lat_750us',          'latency_us/750')
            ,('lat_1000us',         'latency_us/1000')
            ,('lat_2ms',            'latency_ms/2')
            ,('lat_4ms',            'latency_ms/4')
            ,('lat_10ms',           'latency_ms/10')
            ,('lat_20ms',           'latency_ms/20')
            ,('lat_50ms',           'latency_ms/50')
            ,('lat_100ms',          'latency_ms/100')
            ,('lat_250ms',          'latency_ms/250')
            ,('lat_500ms',          'latency_ms/500')
            ,('lat_750ms',          'latency_ms/750')
            ,('lat_1000ms',         'latency_ms/1000')
            ,('lat_2000ms',         'latency_ms/2000')
            ,('lat_over_2000ms',    'latency_ms/>=2000')
            ])

        # list of entries under "disk_util"
        dDiskUtil = OrderedDict([
             ('disk_name',          'name')
            ,('disk_read_iops',     'read_ios')
            ,('disk_write_iops',    'write_ios')
            ,('disk_read_merges',   'read_merges')
            ,('disk_write_merges',  'write_merges')
            ,('disk_read_ticks',    'read_ticks')
            ,('write_ticks',        'write_ticks')
            ,('disk_queue_time',    'in_queue')
            ,('disk_util',          'util')
            ])


        obj = self.objs
        lJobs = obj['jobs']
        lDisks = obj['disk_util']

        outList = []
        for jobObj in lJobs:
            dOut = OrderedDict([('terse_version', '??')])
            for hdr,path in dTerse.items():
                dOut[hdr] = self._getObjValue(obj, path)

            for hdr,path in dJobs.items():
                dOut[hdr] = self._getObjValue(jobObj, path)

            dAvailDirs = OrderedDict()
            if 'read' in jobObj: dAvailDirs['read'] = jobObj['read']
            if 'write' in jobObj: dAvailDirs['write'] = jobObj['write']
            if 'mixed' in jobObj: dAvailDirs['mixed'] = jobObj['mixed']
            for dirName,dirObj in dAvailDirs.items():
                for hdr,path in dDirections.items():
                    metricName = dirName+hdr
                    if path == self._getDirectionPercentiles:
                        path(dOut, metricName, dirObj['clat_ns']['percentile'])
                    else:
                        dOut[metricName] = self._getObjValue(dirObj, path)

            for hdr,path in dJobRest.items():
                dOut[hdr] = self._getObjValue(jobObj, path)

            for diskObj in lDisks:
                for hdr,path in dDiskUtil.items():
                    dOut[hdr] = self._getObjValue(diskObj, path)

            outList.append(dOut)

        return outList


    def terse(self):
        outStrg = ''
        for dOut in self.terseList():
            # we'll assume all the entries are the same for all jobs (incl percentiles and such)
            # (which may not be a safe assumption)
            if outStrg == '':
                outStrg += ';'.join(dOut.keys())

            outStrg += '\n'
            outStrg += ';'.join([str(v) for v in dOut.values()])

        return outStrg


    def _getObjValue(self, dObj, path):
        out = dObj
        nodes = path.split('/')
        for n in nodes:
            out = out[n]

        if "_ns" in path or "lat" in path and isinstance(out, int):
            out = out / 1000.0

        return out


    def _getDirectionPercentiles(self, dOut, metricName, dObj):
        for pct,val in dObj.items():
            pct = pct.rstrip('.0')
            dOut[metricName+"_"+pct] = val / 1000.0  # convert to us


    def _splitJsonNormal(self,f):
        """
        From file, identify json and other data and split into two strings of data,
        While the file may also contain lines which are not json data,
        Return (json lines, normal lines)
        """
        reStatusLine = re.compile(r': \(groupid=')
        containsStatusLines = False
        jsonStream = io.StringIO()
        normalStream = io.StringIO()
        curStream = normalStream
        for line in f.readlines():
            if line == "{\n":
                curStream = jsonStream
                jsonStream.write(line)
            elif line == "}\n":
                jsonStream.write(line)
                curStream = normalStream
            elif curStream is normalStream and reStatusLine.search(line):
                containsStatusLines = True
                curStream.write(line)
            else:
                curStream.write(line)

        if len(jsonStream.getvalue()) == 0:
            raise RuntimeError("File '{}' contains no json data".format(str(self.fpath)))

        jsonStream.seek(0)
        normalStream.seek(0)
        return (jsonStream,normalStream,containsStatusLines)


    def _getMetricAndIOs(self, dirDict, metric, submetric=None):
        """
        from "dirDict" object, get the requested metric and the total number of IOs for
        scaling purposes
        """
        if submetric:
            ret = (dirDict[metric][submetric], dirDict['total_ios'])
        else:
            ret = (dirDict[metric], dirDict['total_ios'])
        return ret


    def _getMetricResultsList(self, metric, submetric=None, dirFilter=None):
        """
        Arguments:
            dirFilter (str):  indicates which direction to return (mixed, read, write, trim, or None (meaning all)
            metric    (str):  name of metric to return

        Returns:
            list of tuples of (value, total_ios) for scaling purposes
        """
        ret = []
        jobs = []
        if 'jobs' in self.data: jobs = self.data['jobs']
        elif 'client_stats' in self.data: jobs = self.data['client_stats']

        for job in jobs:
            if job['jobname'].lower() == 'all clients': continue
            if 'mixed' in job and (dirFilter == None or dirFilter == 'mixed'):
                ret.append(self._getMetricAndIOs(job['mixed'], metric, submetric))
            if 'read' in job and (dirFilter ==  None or dirFilter == 'read'):
                ret.append(self._getMetricAndIOs(job['read'], metric, submetric))
            if 'write' in job and (dirFilter ==  None or dirFilter == 'write'):
                ret.append(self._getMetricAndIOs(job['write'], metric, submetric))
            if 'trim' in job and (dirFilter ==  None or dirFilter == 'trim'):
                ret.append(self._getMetricAndIOs(job['trim'], metric, submetric))
        return ret


    def _avgMetric(self, metric, dirFilter=None):
        """
        Calc scaled averge of 'metric' name across jobs and directional results
        """
        aResults = self._getMetricResultsList(metric, 'mean', dirFilter)
        totalIos = 0
        sumAvgs = 0.0
        for result in aResults:
            sumAvgs += result[0] * result[1]
            totalIos += result[1]
        scaledAvgs = 0.0
        if totalIos > 0:
            scaledAvgs = sumAvgs / totalIos
        return scaledAvgs


    def _sumMetric(self, metric, dirFilter=None):
        """
        Calc sum of 'metric' name across jobs and directional results
        """
        aResults = self._getMetricResultsList(metric, dirFilter)
        sumResults = 0
        for result in aResults:
            sumResults += result[0]
        return sumResults


    def _normalJobStats(self, out, dJob, clients=False):
        """
        generate normalized status for given job dictionary
        """
        dJob['time'] = self.data['time']
        dJob['numjobs'] = '??'  # json doesn't tell us - most cases it's one, but not all
        strg = "{jobname}: (groupid={groupid}, jobs={numjobs}): err={error:>2}: pid=??: {time}".format_map(dJob)
        print(strg, file=out)

        if 'desc' in dJob:
            print("  Description  : [{desc}]".format_map(dJob), file=out)

        totals = [0,0,0,0]
        shorts = [0,0,0,0]
        dropped = [0,0,0,0]
        lDirs = ['mixed','read','write','trim','sync']
        for direction in lDirs:
            if direction in dJob and dJob[direction]['total_ios'] > 0:
                self._normalDirection(out, direction, dJob[direction])
                idx = lDirs.index(direction)
                if idx > 0: idx -= 1  # mixed and read end up using same index
                totals[idx] += dJob[direction]['total_ios']
                shorts[idx] += dJob[direction]['short_ios']
                dropped[idx] += dJob[direction]['drop_ios']

        self._normalLatBuckets(out, dJob)
        self._normalCpu(out, dJob)
        self._normalIODepths(out, dJob)
        self._normalIssued(out, totals, shorts, dropped)
        self._normalLatency(out, dJob)


    def _normalLatency(self, out, dJob):
        strg = "     latency    : target={latency_target}, window={latency_window}, percentile={latency_percentile:.2f}%, depth={latency_depth}".format_map(dJob)
        print(strg, file=out)

    def _normalIssued(self, out, totals, shorts, dropped):
        totals = [str(x) for x in totals]
        shorts = [str(x) for x in shorts]
        dropped = [str(x) for x in dropped]
        strg = "     issued rwts: total={} short={} dropped={}".format(
            ','.join(totals), ','.join(shorts), ','.join(dropped))
        print(strg, file=out)


    def _normalIODepths(self, out, dJob):
        strg = "  IO depths     : "
        depths = []
        for d,pct in dJob['iodepth_level'].items():
            depths.append("{d}={pct:.1f}%".format(d=d, pct=pct))
        print(strg + ", ".join(depths), file=out)


    def _normalLatBuckets(self, out, dJob):
        for scale in ['ns', 'us', 'ms', 's']:
            key = 'latency_'+scale
            if key in dJob:
                strg = "    lat ({scale:>2}ec)  : ".format(scale=scale)
                lBkts = []
                for bkt,pct in dJob[key].items():
                    if pct > 0.0:
                        lBkts.append("{bkt}={pct:.2f}%".format(bkt=bkt, pct=pct))
                if len(lBkts) > 0:
                    print(strg + ", ".join(lBkts), file=out)


    def _normalCpu(self, out, dJob):
        strg = "  cpu           : usr={usr_cpu:.2f}%, sys={sys_cpu:.2f}%, ctx={ctx}, majf={majf}, minf={minf}".format_map(dJob)
        print(strg, file=out)


    def _normalDirection(self, out, direction, dDir):
        dDir['direction'] = direction
        dDir['bwKB'] = dDir['bw_bytes'] / 1000
        strg = "  {direction}: IOPS={iops}, BW={bw}KiB/s ({bwKB}KB/s)({io_kbytes}KiB/{runtime}ms)".format_map(dDir)
        print(strg, file=out)

        self._normalXLat(out, 'slat', dDir)
        self._normalXLat(out, 'clat', dDir)
        self._normalXLat(out, 'lat', dDir)
        self._normalCLatPercents(out, dDir['clat_ns']['percentile'])
        self._normalBw(out, dDir)
        self._normalIops(out, dDir)


    def _normalIops(self, out, dDir):
        strg = "    iops        : min={min}, max={max}, avg={mean:>.2f}, stdev={dev:>.2f}, samples={samples}".format(
            min=dDir['iops_min'], max=dDir['iops_max'], mean=dDir['iops_mean'],
            dev=dDir['iops_stddev'], samples=dDir['iops_samples'])
        print(strg, file=out)


    def _normalBw(self, out, dDir):
        strg = "    bw (  KiB/s): min={min}, max={max}, per={per:>.2f}%, avg={mean:>.2f}, stdev={dev:>.2f}, samples={samples}".format(
            min=dDir['bw_min'], max=dDir['bw_max'], per=dDir['bw_agg'], mean=dDir['bw_mean'],
            dev=dDir['bw_dev'], samples=dDir['bw_samples'])
        print(strg, file=out)


    def _normalCLatPercents(self, out, dPcts):
        print('    clat percentiles  (nsec):', file=out)
        pctlst = []
        for pct,val in dPcts.items():
            pctlst.append("{pct:>9}th=[{val:>10}]".format(pct=pct, val=val))

        strg = ''
        for lChunk in self._genChunks(pctlst, 3):
            strg += "     | " + ', '.join(lChunk) + ',\n'
        strg = strg[0:-2]
        print(strg, file=out)


    def _genChunks(self, l, n):
        """
        yield list elements in n-sized chunks
        """
        for idx in range(0, len(l), n):
            yield l[idx:idx+n]


    def _normalXLat(self, out, metric, dDir):
        for scale in ['ns', 'us', 'ms', 's']:
            key = "{metric}_{scale}".format(metric=metric, scale=scale)
            dDir['metric'] = metric
            dDir['scale'] = scale
            if key in dDir:
                dDir[key]['metric'] = metric
                dDir[key]['scale'] = scale
                strg = "    {metric:>4} ({scale:>2}ec): min={min}, max={max}, avg={mean}, stddev={stddev}".format_map(dDir[key])
                print(strg, file=out)


    def _normalRunStatus(self, out, aJobs):
        """
        generate RunStatus entries for given list of job dicts
        """
        pass


    def _normalDiskStats(self, out, dUtils):
        """
        generate Disk Utilization entries
        """
        pass


if __name__ == "__main__":
    import argparse
    import pprint

    parser = argparse.ArgumentParser(description="Convert fio json output file to terse or normal like results",
                                     epilog="*** Version {}".format(__version__))

    parser.add_argument('jsonfile',
                        help="File containing fio json output")
    parser.add_argument('-o', '--format',
                        default='terse',
                        choices=['terse', 'normal', 'json'],
                        help="output format ('terse', 'normal', or imported 'json' prettyprinted")
    args = parser.parse_args()


    jFileObj = FioJsonFile(args.jsonfile)
    if args.format == 'terse':
        print(jFileObj.terse())
    elif args.format == 'normal':
        print(jFileObj.normalized())
    elif args.format == 'json':
        pp = pprint.PrettyPrinter(indent=4, width=160, compact=True)
        print(pp.pformat(jFileObj.objs))



            