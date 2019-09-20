# Developers Guide: Networks & Servers 


# Layout 
The development of the networks an servers for Apache-HFT is an ongoing process that needs all the help it can get. If you are interest in
helping with any of the following, this place is for you. If not, make sure to check out other areas that could possibly interest you. 


# Server Structure
The current setup of the server environment is cloud based, using GCP as our main provider. Everything is hosted on either compute engine instances or on k8 container instances, making use of Dataflow / Dataproc for handling event streams along with an explicit subnet structure for mitgating the constantly changing VPC layout. There is currently support for both TCP and UDP broadcast streams with a couple of different options for filtering streams, otherwise you will want to check out tapping into the MongoDB replication log for watching changes to a cluster, certain database, or even a certain collection within a database. 
