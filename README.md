#CommandLine Hadoop Job Tracker#

##Usage##
This tool is help you to view the Hadoop job information in command line without looking at the web pages.
And the tool provide a more elegant display than the provided query api.

![Sample job display](/src/test/resources/hjt_job_display.png)

* `hjt -u [user.name]    `: display user jobs
* `hjt -q queue.name`     : display the jobs in queue.name
* `hjt -q`                : display all the queue information
* `hjt -j application_id` : display the information for application_id


##Installation##
1. use `git clone` to clone this repository to local disk.
2. user `maven clean package` to pack the project and you will get tar file
3. copy the tar file to Hadoop cli machine and extract
4. user bin/hjt command, and make sure `HADOOP_HOME` is correctly configured
