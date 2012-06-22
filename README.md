classad-xrootd-mapping
======================

Custom ClassAd functions for mapping Xrootd files to locations

Sample test case usage:

```
[bbockelm@brian-test classad-xrootd-mapping]$ XRD_DEBUGLEVEL=0 ./src/classad_xrootd_mapping_tester src/libclassad_xrootd_mapping.so src/classad_sample.txt 
Resulting ClassAd:

    [
        sites = files_to_sites("xrootd-itb.unl.edu:1094","/store/mc/JobRobot/RelValProdTTbar/GEN-SIM-DIGI-RECO/MC_3XY_V24_JobRobot-v1/0001/56E18353-982C-DF11-B217-00304879FA4A.root")
    ]

Value of 'sites' attribute: 
   {
      "cithep160.ultralight.org",
      "cithep172.ultralight.org",
      "cithep230.ultralight.org",
      "cithep251.ultralight.org",
      "cmsdbs.rcac.purdue.edu",
      "cmssrv32.fnal.gov",
      "crabserver.rcac.purdue.edu",
      "gridftp-16-23.ultralight.org",
      "s17n01.hep.wisc.edu",
      "se2.accre.vanderbilt.edu",
      "srm.unl.edu",
      "xrootd.cmsaf.mit.edu",
      "xrootd.rcac.purdue.edu",
      "xrootd.t2.ucsd.edu",
      "xrootd1.ihepa.ufl.edu",
      "xrootd2.ihepa.ufl.edu"
   }
```

More thorough usage requires integration with Condor.

