<html>
<head>
      <meta http-equiv="refresh" content="30">
<title>Hadoop job_201412071502_308344 on horton-jt</title>
</head>
<body>
<h1>Hadoop job_201412071502_308344 on <a href="jobtracker.jsp">horton-jt</a></h1>

<b>User:</b> guazhu<br>
<b>Job Name:</b> SPEAR - Calculating RvrAccId 2014/12/14<br>
<b>Job File:</b> <a href="jobconf.jsp?jobid=job_201412071502_308344">hdfs://a.b.c.com/user/danickzhu/.staging/job_201412071502_308344/job.xml</a><br>
<b>Submit Host:</b> localhost.com<br>
<b>Submit Host Address:</b> 10.196.192.24<br>
<b>Job-ACLs: All users are allowed</b><br>
<b>Job Setup:</b><a href="jobtasks.jsp?jobid=job_201412071502_308344&type=setup&pagenum=1&state=completed"> Successful</a><br>
<b>Status:</b> Running<br>
<b>Started at:</b> Sun Dec 28 22:37:38 PST 2014<br>
<b>Running for:</b> 31hrs, 10mins, 38sec<br>
<b>Job Cleanup:</b> Pending<br>
<hr>
<table border=2 cellpadding="5" cellspacing="2"><tr><th>Kind</th><th>% Complete</th><th>Num Tasks</th><th>Pending</th><th>Running</th><th>Complete</th><th>Killed</th><th><a href="jobfailures.jsp?jobid=job_201412071502_308344">Failed/Killed<br>Task Attempts</a></th></tr>
<tr><th><a href="jobtasks.jsp?jobid=job_201412071502_308344&type=map&pagenum=1">map</a></th><td align="right">100.00%<table border="1px" width="80px"><tr><td cellspacing="0" class="perc_filled" width="100%"></td></tr></table></td><td align="right">846</td><td align="right">0</td><td align="right">0</td><td align="right"><a href="jobtasks.jsp?jobid=job_201412071502_308344&type=map&pagenum=1&state=completed">846</a></td><td align="right">0</td><td align="right">0 / <a href="jobfailures.jsp?jobid=job_201412071502_308344&kind=map&cause=killed">39</a></td></tr>
<tr><th><a href="jobtasks.jsp?jobid=job_201412071502_308344&type=reduce&pagenum=1">reduce</a></th><td align="right">99.96%<table border="1px" width="80px"><tr><td cellspacing="0" class="perc_filled" width="99%"></td><td cellspacing="0" class="perc_nonfilled" width="1%"></td></tr></table></td><td align="right">113</td><td align="right">0</td><td align="right"><a href="jobtasks.jsp?jobid=job_201412071502_308344&type=reduce&pagenum=1&state=running">1</a></td><td align="right"><a href="jobtasks.jsp?jobid=job_201412071502_308344&type=reduce&pagenum=1&state=completed">112</a></td><td align="right">0</td><td align="right">0 / 0</td></tr>
</table>

    <p/>
    <table border=2 cellpadding="5" cellspacing="2">
    <tr>
      <th><br/></th>
      <th>Counter</th>
      <th>Map</th>
      <th>Reduce</th>
      <th>Total</th>
    </tr>
    
        <tr>
          
            <td rowspan="6">
            Job Counters </td>
            
          <td>SLOTS_MILLIS_MAPS</td>
          <td align="right">0</td>
          <td align="right">0</td>
          <td align="right">289,066,110</td>
        </tr>
        
        <tr>
          
          <td>Launched reduce tasks</td>
          <td align="right">0</td>
          <td align="right">0</td>
          <td align="right">113</td>
        </tr>
        
        <tr>
          
          <td>Rack-local map tasks</td>
          <td align="right">0</td>
          <td align="right">0</td>
          <td align="right">12</td>
        </tr>
        
        <tr>
          
          <td>Launched map tasks</td>
          <td align="right">0</td>
          <td align="right">0</td>
          <td align="right">885</td>
        </tr>
        
        <tr>
          
          <td>Data-local map tasks</td>
          <td align="right">0</td>
          <td align="right">0</td>
          <td align="right">845</td>
        </tr>
        
        <tr>
          
          <td>SLOTS_MILLIS_REDUCES</td>
          <td align="right">0</td>
          <td align="right">0</td>
          <td align="right">428,277,339</td>
        </tr>
        
        <tr>
          
            <td rowspan="4">
            FileSystemCounters</td>
            
          <td>FILE_BYTES_READ</td>
          <td align="right">260,849,306,886</td>
          <td align="right">320,628,453,701</td>
          <td align="right">581,477,760,587</td>
        </tr>
        
        <tr>
          
          <td>HDFS_BYTES_READ</td>
          <td align="right">113,911,446,472</td>
          <td align="right">0</td>
          <td align="right">113,911,446,472</td>
        </tr>
        
        <tr>
          
          <td>FILE_BYTES_WRITTEN</td>
          <td align="right">474,783,984,834</td>
          <td align="right">334,091,767,313</td>
          <td align="right">808,875,752,147</td>
        </tr>
        
        <tr>
          
          <td>HDFS_BYTES_WRITTEN</td>
          <td align="right">0</td>
          <td align="right">4,773,992,777</td>
          <td align="right">4,773,992,777</td>
        </tr>
        
        <tr>
          
            <td rowspan="15">
            Map-Reduce Framework</td>
            
          <td>Map input records</td>
          <td align="right">1,852,080,373</td>
          <td align="right">0</td>
          <td align="right">1,852,080,373</td>
        </tr>
        
        <tr>
          
          <td>Reduce shuffle bytes</td>
          <td align="right">0</td>
          <td align="right">235,393,113,591</td>
          <td align="right">235,393,113,591</td>
        </tr>
        
        <tr>
          
          <td>Spilled Records</td>
          <td align="right">7,408,321,492</td>
          <td align="right">3,598,763,972</td>
          <td align="right">11,007,085,464</td>
        </tr>
        
        <tr>
          
          <td>Map output bytes</td>
          <td align="right">654,140,659,767</td>
          <td align="right">0</td>
          <td align="right">654,140,659,767</td>
        </tr>
        
        <tr>
          
          <td>CPU time spent (ms)</td>
          <td align="right">361,198,210</td>
          <td align="right">590,568,060</td>
          <td align="right">951,766,270</td>
        </tr>
        
        <tr>
          
          <td>Total committed heap usage (bytes)</td>
          <td align="right">401,356,918,784</td>
          <td align="right">120,824,397,824</td>
          <td align="right">522,181,316,608</td>
        </tr>
        
        <tr>
          
          <td>Combine input records</td>
          <td align="right">0</td>
          <td align="right">0</td>
          <td align="right">0</td>
        </tr>
        
        <tr>
          
          <td>SPLIT_RAW_BYTES</td>
          <td align="right">139,590</td>
          <td align="right">0</td>
          <td align="right">139,590</td>
        </tr>
        
        <tr>
          
          <td>Reduce input records</td>
          <td align="right">0</td>
          <td align="right">3,489,400,277</td>
          <td align="right">3,489,400,277</td>
        </tr>
        
        <tr>
          
          <td>Reduce input groups</td>
          <td align="right">0</td>
          <td align="right">26,133,382</td>
          <td align="right">26,133,382</td>
        </tr>
        
        <tr>
          
          <td>Combine output records</td>
          <td align="right">0</td>
          <td align="right">0</td>
          <td align="right">0</td>
        </tr>
        
        <tr>
          
          <td>Physical memory (bytes) snapshot</td>
          <td align="right">443,911,569,408</td>
          <td align="right">138,561,306,624</td>
          <td align="right">582,472,876,032</td>
        </tr>
        
        <tr>
          
          <td>Reduce output records</td>
          <td align="right">0</td>
          <td align="right">26,133,381</td>
          <td align="right">26,133,381</td>
        </tr>
        
        <tr>
          
          <td>Virtual memory (bytes) snapshot</td>
          <td align="right">1,402,063,368,192</td>
          <td align="right">199,599,611,904</td>
          <td align="right">1,601,662,980,096</td>
        </tr>
        
        <tr>
          
          <td>Map output records</td>
          <td align="right">3,704,160,746</td>
          <td align="right">0</td>
          <td align="right">3,704,160,746</td>
        </tr>
        
    </table>

<hr>Map Completion Graph - 
 
<a href="jobdetails.jsp?jobid=job_201412071502_308344&refresh=30&map.graph=off" > close </a>
<br><embed src="taskgraph?type=map&jobid=job_201412071502_308344"
       width="760" 
       height="260"
       style="width:100%" type="image/svg+xml" pluginspage="svg/viewer/install/" />



<hr>Reduce Completion Graph -
 
<a href="jobdetails.jsp?jobid=job_201412071502_308344&refresh=30&reduce.graph=off" > close </a>
 
 <br><embed src="taskgraph?type=reduce&jobid=job_201412071502_308344"
       width="760" 
       height="260" 
       style="width:100%" type="image/svg+xml" pluginspage="svg/viewer/install/" />
</body></html>

