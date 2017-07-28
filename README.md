# yelp-challenge

According to the task it consists of two parts:

   1. Parsing to tabular format
   2. Querying over data

Json to Tabular conversion is done using spark job which just simply read the input json and save it in parquet columnar format.

The system which does the job consist of 2 docker images:

   1. Cluster container - hadoop/spark container on which job is being submitted
   2. Client container - container containing hadoop/spark client, all the user data needed for job submission.

The basic flow of parsing is the following:

   1. Build/Start cluster container
   
   2. Build/Start client container, which among other steps involves
 
      a. Setting up hadoop/spark clients
  
      b. Copying yelp dataset
      
      c. Copying scripts/job files necessary for job execution

3. From the client container start the job script with yelp dataset as an input. 
   
      It will perform the following steps:
      
      a. Extract dataset tar file
      
      b. Extracted files will be transferred to HDFS (this is to achieve parallelism in spark job) . this step will return the filenames of transferred files
      
      c. Having the result from previous step as in input spark job will be called, which will read json as an input and save it in parquet format.

Having this done we can already start querying.


-----HOW TO START PARSING PROCESS-----

Let’s see which commands needs to be executed in sequence:
    Note: in order to trigger the process as a prerequirement we need to have yelp_dataset.tar in docker/clinet directory
    
    For building and starting cotnainers we simple need to run the following from parent direcotry
      
      sh deployment.sh
    
    At the end of the execution we will be in client terminal. From there we can do the following
    
      1. cd /client
      
      2. sh json_to_parquet.sh /yelp-dataset/yelp_dataset_challenge_round9.tar
      
      It will trigger a spark job on cluster machine, which will take a couple of minutes to finsih.
        
       
 ------HOW TO QUERY------
 
 After successfull job execution it's time for querying. There are a couple of queries under /client/queries directory
       
       for running each and every query simply run from /client dir
       
       
       
         1. spark-shell -i  queries/top_nightlife_businesses_in_LasVegas.scala
         
         2. spark-shell -i  queries/top_users_for_starbucks_by_reviews.scala 
         
         3. spark-shell -i queries/top_statescites_by_reviews.scala
         
         4. spark-shell -i  queries/top_rated.scala

Results will be printed in the terminal.

After each and every query run we need to exit frm scala terminal to execute the next one


Enjoy!
