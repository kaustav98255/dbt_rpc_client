# DBT RPC Client
DBT RPC client and airflow DAGs to execute DBT cli commands through the client on the RPC server.

The `docker-componse.yml` creates a DBT RPC server, an airflow metadata db and the airflow scheduler and webserver.
Launch the containers using `docker-compose up`.

Once the server is up and running, it can be accessed at `http://dbt-rpc-server:8580/jsonrpc` from a docker container or from `http://localhost:8580/jsonrpc` when you are using the IDE to debug. The Client takes care of choosing which URL to use.

To run / test the repo you will need to:

1. clone the repo
2. should have docker and docker-compose installed
3. in the repo folder execute `docker-compose up`
4. go to `http://localhost:8080` to access the airflow webserver
5. turn the `dbt_dag` ON and trigger the dag
6. check the logs for each task

The client currently supports the following DBT CLI commands:
- dbt test
- dbt compile
- dbt run
- dbt docs.generate

When a task is executed the client checks the status of the task every 2 seconds and prints the logs to stdout.
Here the rpc end points `ps` and `poll` are being used.

The rpc commands are executed from the demo dag in the `/dags/` folder. It consists of three tasks chained together to run tests, create models and generate docs.

The repo contains the sample DBT jaffle_shop models and a sample profile.yml but you can adapt that to your models and profile.yml. I normally replace the profile.yml with my standard BigQuery config for testing.


