<html>
<head>
<title>horton-jt Hadoop Map/Reduce Administration</title>
<link rel="stylesheet" type="text/css" href="static/hadoop.css?F5CH=C">
<link rel="icon" type="image/vnd.microsoft.icon" href="static/images/favicon.ico" />
<script>try{F5_preScript(document)}catch(e){}</script><script type="text/javascript" src="static/jobtracker.js"></script><script>try{F5_postScript(document);}catch(e){}</script>
<script>try{F5_preScript(document)}catch(e){}</script><script type='text/javascript' src='static/sorttable.js'></script><script>try{F5_postScript(document);}catch(e){}</script>
</head>
<body>
<h1>horton-jt Hadoop Map/Reduce Administration</h1>

<div id="quicklinks">
  <a href="#quicklinks" >Quick Links</a>
  <ul id="quicklinks-list">
    <li><a href="#scheduling_info">Scheduling Info</a></li>
    <li><a href="#running_jobs">Running Jobs</a></li>
    <li><a href="#retired_jobs">Retired Jobs</a></li>
    <li><a href="#local_logs">Local Logs</a></li>
  </ul>
</div>

<b>State:</b> RUNNING<br>
<b>Started:</b> Sun Dec 07 15:02:03 PST 2014<br>
<b>Version:</b> 0.20.2-cdh3u4a,
                7c53448eea62f65b92e66f02ba44e41f62ca6b01<br>
<b>Compiled:</b> Mon Apr  1 13:51:37 PDT 2013 by 
                 jenkins from
                 Unknown<br>
<b>Identifier:</b> 201412071502<br>                 
                   
<hr>
<h2>Cluster Summary (Heap Size is 21.58 GB/62.72 GB)</h2>
<table border="1" cellpadding="5" cellspacing="0">
<tr><th>Running Map Tasks</th><th>Running Reduce Tasks</th><th>Total Submissions</th><th>Nodes</th><th>Occupied Map Slots</th><th>Occupied Reduce Slots</th><th>Reserved Map Slots</th><th>Reserved Reduce Slots</th><th>Map Task Capacity</th><th>Reduce Task Capacity</th><th>Avg. Tasks/Node</th><th>Blacklisted Nodes</th><th>Excluded Nodes</th></tr>
<tr><td>2078</td><td>122</td><td>31625</td><td><a href="machines.jsp?type=active">734</a></td><td>2078</td><td>122</td><td>0</td><td>0</td><td>9542</td><td>3670</td><td>18.00</td><td><a href="machines.jsp?type=blacklisted">1</a></td><td><a href="machines.jsp?type=excluded">4</a></td></tr></table>
<br>
<hr>
<h2 id="scheduling_info">Scheduling Information</h2>
<table border="2" cellpadding="5" cellspacing="2" class="sortable">
<thead style="font-weight: bold">
<tr>
<td> Queue Name </td>
<td> State </td>
<td> Scheduling Information</td>
</tr>
</thead>
<tbody>

<tr>
<td><a href="jobqueue_details.jsp?queueName=etl">etl</a></td>
<td>running</td>
<td>N/A
</td>
</tr>

<tr>
<td><a href="jobqueue_details.jsp?queueName=default">default</a></td>
<td>running</td>
<td>N/A
</td>
</tr>

</tbody>
</table>
<hr/>
<b>Filter (Jobid, Priority, User, Name)</b> <input type="text" id="filter"> <br>
<span class="small">Example: 'user:smith 3200' will filter by 'smith' only in the user field and '3200' in all fields</span>
<hr>

<h2 id="running_jobs">Running Jobs</h2>
<table class="datatable">
<thead><tr><th><b>Jobid</b></th><th><b>Priority</b></th><th><b>User</b></th><th><b>Name</b></th><th><b>Map % Complete</b></th><th><b>Map Total</b></th><th><b>Maps Completed</b></th><th><b>Reduce % Complete</b></th><th><b>Reduce Total</b></th><th><b>Reduces Completed</b></th><th><b>Job Scheduling Information</b></th><td><b>Diagnostic Info </b></td></tr>
</thead><tbody><tr><td id="job_0"><a href="jobdetails.jsp?jobid=job_201412071502_29177&refresh=30">job_201412071502_29177</a></td><td id="priority_0">NORMAL</td><td id="user_0">user3</td><td id="name_0">PigLatin:user_count</td><td>100.00%<table border="1px" width="80px"><tr><td cellspacing="0" class="perc_filled" width="100%"></td></tr></table></td><td>3346</td><td>3346</td><td>99.64%<table border="1px" width="80px"><tr><td cellspacing="0" class="perc_filled" width="99%"></td><td cellspacing="0" class="perc_nonfilled" width="1%"></td></tr></table></td><td>425</td><td> 400</td><td>NA</td><td>NA</td></tr>
<tr><td id="job_1"><a href="jobdetails.jsp?jobid=job_201412071502_31948&refresh=30">job_201412071502_31948</a></td><td id="priority_1">NORMAL</td><td id="user_1">user2</td><td id="name_1">PigLatin:world_count.pig</td><td>0.00%<table border="1px" width="80px"><tr><td cellspacing="0" class="perc_nonfilled" width="100%"></td></tr></table></td><td>225</td><td>0</td><td>0.00%<table border="1px" width="80px"><tr><td cellspacing="0" class="perc_nonfilled" width="100%"></td></tr></table></td><td>0</td><td> 0</td><td>NA</td><td>NA</td></tr>
</tbody></table>

<hr>

<h2 id="completed_jobs">Completed Jobs</h2><table class="datatable">
<thead><tr><th><b>Jobid</b></th><th><b>Priority</b></th><th><b>User</b></th><th><b>Name</b></th><th><b>Map % Complete</b></th><th><b>Map Total</b></th><th><b>Maps Completed</b></th><th><b>Reduce % Complete</b></th><th><b>Reduce Total</b></th><th><b>Reduces Completed</b></th><th><b>Job Scheduling Information</b></th><td><b>Diagnostic Info </b></td></tr>
</thead><tbody><tr><td id="job_28"><a href="jobdetails.jsp?jobid=job_201412071502_30137&refresh=0">job_201412071502_30137</a></td><td id="priority_28">NORMAL</td><td id="user_28">user1</td><td id="name_28">PigLatin:world_count.pig</td><td>100.00%<table border="1px" width="80px"><tr><td cellspacing="0" class="perc_filled" width="100%"></td></tr></table></td><td>799</td><td>799</td><td>100.00%<table border="1px" width="80px"><tr><td cellspacing="0" class="perc_filled" width="100%"></td></tr></table></td><td>1500</td><td> 1500</td><td>NA</td><td>NA</td></tr>
<tr><td id="job_29"><a href="jobdetails.jsp?jobid=job_201412071502_31002&refresh=0">job_201412071502_31002</a></td><td id="priority_29">NORMAL</td><td id="user_29">ops</td><td id="name_29">PigLatin:customer_profiler.pig</td><td>100.00%<table border="1px" width="80px"><tr><td cellspacing="0" class="perc_filled" width="100%"></td></tr></table></td><td>5263</td><td>5263</td><td>100.00%<table border="1px" width="80px"><tr><td cellspacing="0" class="perc_filled" width="100%"></td></tr></table></td><td>999</td><td> 999</td><td>NA</td><td>NA</td></tr>
</tbody></table>
<hr>

<h2 id="failed_jobs">Failed Jobs</h2><table class="datatable">
<thead><tr><th><b>Jobid</b></th><th><b>Priority</b></th><th><b>User</b></th><th><b>Name</b></th><th><b>Map % Complete</b></th><th><b>Map Total</b></th><th><b>Maps Completed</b></th><th><b>Reduce % Complete</b></th><th><b>Reduce Total</b></th><th><b>Reduces Completed</b></th><th><b>Job Scheduling Information</b></th><td><b>Diagnostic Info </b></td></tr>
</thead><tbody><tr><td id="job_91"><a href="jobdetails.jsp?jobid=job_201412071502_31731&refresh=0">job_201412071502_31731</a></td><td id="priority_91">NORMAL</td><td id="user_91">user1</td><td id="name_91">PigLatin:regen1.pig</td><td>100.00%<table border="1px" width="80px"><tr><td cellspacing="0" class="perc_filled" width="100%"></td></tr></table></td><td>225</td><td>0</td><td>100.00%<table border="1px" width="80px"><tr><td cellspacing="0" class="perc_filled" width="100%"></td></tr></table></td><td>0</td><td> 0</td><td>NA</td><td>NA</td></tr>
<tr><td id="job_98"><a href="jobdetails.jsp?jobid=job_201412071502_31981&refresh=0">job_201412071502_31981</a></td><td id="priority_98">NORMAL</td><td id="user_98">ops</td><td id="name_98">PigLatin:regen.pig</td><td>100.00%<table border="1px" width="80px"><tr><td cellspacing="0" class="perc_filled" width="100%"></td></tr></table></td><td>293</td><td>0</td><td>100.00%<table border="1px" width="80px"><tr><td cellspacing="0" class="perc_filled" width="100%"></td></tr></table></td><td>0</td><td> 0</td><td>NA</td><td>NA</td></tr>
</tbody></table>
<hr>

<h2 id="retired_jobs">Retired Jobs</h2>
<table border="1" cellpadding="5" cellspacing="0" class="sortable">
<tr><td><b>Jobid</b></td><td><b>Priority</b></td><td><b>User</b></td><td><b>Name</b></td><td><b>State</b></td><td><b>Start Time</b></td><td><b>Finish Time</b></td><td><b>Map % Complete</b></td><td><b>Reduce % Complete</b></td><td><b>Job Scheduling Information</b></td><td><b>Diagnostic Info </b></td></tr>
<tr><td id="job_99"><a href="jobdetailshistory.jsp?logFile=abc.com%2Fjthistory%2Fabc.com_1417993323479_%2F2014%2F05%2F04%2F003146%2Fjob_201412071502_31464_1418173369800">job_201412071502_31464</a></td><td id="priority_99">NORMAL</td><td id="user_99">xzhuang1</td><td id="name_99">PigLatin:worldcount100.pig</td><td>SUCCEEDED</td><td>Tue Dec 09 17:02:49 PST 2014</td><td>Tue Dec 09 17:04:56 PST 2014</td><td>100.00%<table border="1px" width="80px"><tr><td cellspacing="0" class="perc_filled" width="100%"></td></tr></table></td><td>100.00%<table border="1px" width="80px"><tr><td cellspacing="0" class="perc_filled" width="100%"></td></tr></table></td><td>NA</td><td>NA</td></tr>
</table>

<hr>

<h2 id="local_logs">Local Logs</h2>
<a href="logs/">Log</a> directory, <a href="jobhistory.jsp">
Job Tracker History</a>
</body></html>

