import uuid
import time
import pandas as pd
import json as js
import requests as req


class DBTRPCClient:
    def __init__(self):
        self.headers = {'content-type': 'application/json'}

        # dbt rpc URL - check if the docker connection works
        # else fall back to localhost when running from local machine
        try:
            # this url is required for container to container networking
            r = req.get(url="http://dbt-rpc-server:8580/jsonrpc", headers=self.headers)
            if r.status_code == 200:
                self.url = "http://dbt-rpc-server:8580/jsonrpc"
        except req.exceptions.ConnectionError:
            # this url works when connecting directly from terminal to the rpc server for testing
            self.url = "http://localhost:8580/jsonrpc"

        # baseline json spec for making rpc calls
        self.json_spec = {
           "jsonrpc": "2.0",
            "method": "",
            "id": "",
            "params": {}
        }

        # will hold the logs for the current running process
        self.logs = pd.DataFrame()

    @staticmethod
    def get_json_spec(method, uid, params={}):
        """

            Function takes the DBT ``method`` name as parameter, ``uid`` of the process
            and finally what parameters are going to be passed in the RPC call to create
            the json in the format expected by the dbt rpc server.

            :param method: (string) one of run / compile / test / ps / poll
            :param uid: (string) uuid
            :param params: (dict) dict of parameters expected in variious dbt rpc calls
            :return: dict in the json spec for dbt rpc server
            :rtype: dict

        """
        # remove empty values from the parameters
        if 'models' in params.keys() and len(params['models']) == 0:
            del params['models']
        if 'exclude' in params.keys() and len(params['exclude']) == 0:
            del params['exclude']

        json_spec = {
            "jsonrpc": "2.0",
            "method": method,
            "id": uid,
            "params": params
        }

        return json_spec

    @staticmethod
    def get_uuid():
        """
            Function returns string based random uuid

            :return: string uuid

        """
        return str(uuid.uuid4())

    @staticmethod
    def print_logs(logs, log_level="DEBUG"):
        """

            Convenience method to print the logs returned by dbt rpc server using the ``poll``
            function call.
            For log_level = DEBUG, it will print both DEBUG and INFO logs
            For log_level = INFO, it will only print INFO logs

            .. code-block: python

               #Logs example
               <class 'dict'>: {'timestamp': '2019-12-14T19:07:25.917722Z',
               'message': 'Found 254 models, 138 tests, 0 snapshots, 0 analyses, 253 macros',
               'channel': 'dbt', 'level': 11, 'levelname': 'INFO', 'thread_name': 'QueueFeederThread',
               'process': 18, 'extra': {'request_id': '65ba3889-93a3-4caa-b893-90e81e0de1b9',
               'method': 'compile', 'context':
               'request', 'addr': '172.29.0.1',
               'http_method': 'POST', 'run_state': 'internal'}, 'exc_info': None}

            :param logs: list of dicts for each log
            :param log_level: string - either DEBUG or INFO

            :return: None

        """
        # create the list of log levels we will need to print
        if log_level == "DEBUG":
            level_names = ["INFO", "DEBUG"]
        elif log_level == "INFO":
            level_names = ["INFO"]

        # filter all the logs to what needs to be printed and we take only the columns we need
        filtered_logs = list(filter(lambda x: x["levelname"] in level_names, logs))

        logs_df = pd.DataFrame(data=filtered_logs, columns=['message'])
        for idx, rows in logs_df.iterrows():
            print(rows['message'])

        return len(logs)

    def make_rpc_request(self, payload):
        """

            Makes a POST request to the rpc server endpoint with payload defined in
            dbt json rpc spec and returns the JSON response.
            Returns empty dict if an error occurs.

            :param payload: dict in format returned by self.get_json_spec() method

            :return: dict response form the rpc call

        """
        try:
            response = req.post(self.url, data=js.dumps(payload), headers=self.headers).json()
        except req.exceptions.HTTPError:
            response = {}

        return response

    def poll(self, request_token, log_start):
        """

            Makes a ``poll`` rpc call and returns the poll response containing all
            the running logs of the task_id defined by ``request_token``.
            We use ``log_start`` to only fetch those logs that have not been fetched yet by using a starting
            number that is based on the number of logs that was fetched in the previous run.

            :param request_token: task_id of the process for which we need the logs
            :param log_start: the starting position from where to get the logs from the rpc server
            :return: json dict response of the poll request

        """
        # set the request parameters and get the payload
        parameters = {
            "request_token": request_token,
            "logs": True,
            "logs_start": log_start
        }
        payload = self.get_json_spec(method='poll', uid=self.get_uuid(), params=parameters)

        # make rpc request and get json response
        poll_response = self.make_rpc_request(payload=payload)

        return poll_response

    def ps(self, request_token):
        """

            Heartbeat rpc call to get the current status of the task defined by ``request_token``.
            The ps call returns a list of all running and completed processes and we find
            the task matching our ``request_token`` to get the status of that process.

            :param request_token: task_id of the process for which we need the logs
            :return: json dict of the current status of the task_id == request_token

        """
        # set the request parameters and get the payload
        parameters = {
            "completed": True
        }
        payload = self.get_json_spec(method='ps', uid=self.get_uuid(), params=parameters)

        # make rpc request and get json response
        response = self.make_rpc_request(payload=payload)

        # find the task response that matches the request token
        task_responses = [r for r in response["result"]["rows"] if r['task_id'] == request_token]

        return task_responses[0]

    def execute(self, task='', models='', exclude='', data=True, schema=True):
        """

            Main function to execute dbt cli commands against the dbt rpc server.
            The function gets the prepared json and executes the cli command defined in the ``task``
            with ``models`` and ``exclude`` parameters. Those parameters are removed from the payload
            if they are empty.
            For ``dbt tests`` two additional parameters for running data and schema tests are taken.

            Once the command is issued, the method uses ``ps`` to check if the command is still running
            and keeps printing the logs using the ``poll`` method. It sleeps 5 seconds between subsequent
            checks of the process status.

            :param task: string - one of **run / test / compile**
            :param models: string - specification of which models to include e.g. "+abc"
            :param exclude: string - specification of which models to exclude e.g. "def-"
            :param data: boolean - True for running data tests, False for not running data tests
            :param schema: boolean - True for running schema tests, False for not running schema tests
            :return:
        """
        # create parameters for the compile cli request
        parameters = {
            'models': models,
            'exclude': exclude
        }
        if task == 'test':
            parameters['data'] = data
            parameters['schema'] = schema

        # get the json paylod
        payload = self.get_json_spec(method=task, uid=self.get_uuid(), params=parameters)

        # make the rpc request and get json response
        response_json = self.make_rpc_request(payload=payload)
        request_token = response_json["result"]["request_token"]

        # keep polling, fetch and print the logs until the request is completed
        task_completed = False
        n_logs = 0

        print("INFO: DBT CLI {} starting ...".format(task))
        print("\nRUN LOGS:")
        print("------------------------------------------------------")
        while not task_completed:
            # get the task status
            task_status = self.ps(request_token=request_token)
            # get the logs we haven't got before
            log = self.poll(request_token=request_token, log_start=n_logs)

            # print the logs and get the number of rows of logs printed
            # we will use this in the next iteration to only fetch logs that have not
            # been printed yet
            n_logs = self.print_logs(log["result"]["logs"], log_level="INFO")

            # state = 'running' for running process, state='success' for completed
            if task_status['state'] != 'running':
                task_completed = True

            # sleep 2 seconds before checking the status of the task again
            time.sleep(2)

        return None


if __name__ == "__main__":
    rpc_object = DBTRPCClient()
    rpc_object.execute(task='test')
