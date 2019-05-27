# Datadog - Application of Pageviews

## Possible Errors Encoutered
* Spark - Error “A master URL must be set in your configuration” using Intellij IDEA
  
  It looks like the parameter is not passed somehow. E.g. the spark is initialized somewhere earlier. Try with the VM option -Dspark.master=local[*]. 
  In the IntelliJ it's in list of run config -> Edit Configurations... -> VM Options
