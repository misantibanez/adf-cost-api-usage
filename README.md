# Intro
Azure Data Factory Cost API Usage is a python script that calculate the cost of pipelines in a Data Factory (execution time = 10 min, you can change it).

# Scenario
The customer want to examine each pipeline determining its cost for a 10 min runs. Using ADF APIs, we can estimate the cost for each pipeline using the python script provided. 
We know that ADF APIs retrieve 50 items; so, in order to gather all the items, we are controlling pagination by using nextLink attribute.

This scripts only support specific types of activities. If you have other activities, I extreamly recommend to modify adding those activities required.

Before running, I suggest to read ADF Pricing concept first with some examples (reference link 1). As we know, in ADF exists different operations, so take a look at them (reference link 2).

# Script Flow
The script consume 6 APIs from ADF:
- Pipeline -> List by Factory
- Integration Runtimes -> List by Factory
- DataFlow -> List by Factory
- Dataset -> List by Factory
- LinkedServices -> List by Factory
- Pipeline run -> Query by Factory

First of all, we declare the following variables in order to make this reusable for others Data Factories:
```
tenantid = '***'
subscription = '***'
resource_group  = '***' 
datafactory_name = '****'
headers = {"Authorization": "Bearer **TOKEN**"}
```

To call an API, we have to consider the followin structure:
* For GET calls:
```
request_pipelines = f'https://management.azure.com/subscriptions/{subscription}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{datafactory_name}/pipelines?api-version=2018-06-01'
response_pipelines_prev = requests.get(request_pipelines,headers=headers).json()
```
* For POST calls:
```
request_pipeline_runs = f'https://management.azure.com/subscriptions/{subscription}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{datafactory_name}/queryPipelineRuns?api-version=2018-06-01'
response_pipeline_runs_prev = requests.post(request_pipeline_runs,headers=headers,json=body_runs).json()
```
To control pagination, we need to add a while clause in order to identify if the response include "nextLink" or, in pipeline run API, "continuationToken". 

* For nextLink pagination APIs:
```
c = 1
total_response_pipelines = []
total_response_pipelines.extend(response_pipelines_prev['value'])

while 'nextLink' in response_pipelines_prev:
    response_pipelines_prev = requests.get(response_pipelines_prev['nextLink'],headers=headers).json()
    total_response_pipelines.extend(response_pipelines_prev['value'])
    c+=1
```    
* For continuationToken pagination APIs:

First, we have to add a body request where we are going to set the continuationToken variable (it can start with null value) which it is going to be set then with the continuationToken value gather from the previous response. Remember that for "lastUpdatedBefore" value should be maximum the current date, cannot be future dates (it will return an error).
```
body_runs = {
    "lastUpdatedAfter": "2022-09-01T00:00:00.0000000Z",
    "lastUpdatedBefore": "2022-09-29T00:59:59.9999999Z",
    "continuationToken": ""
}

c = 1
total_response_pipeline_runs = []
total_response_pipeline_runs.extend(response_pipeline_runs_prev['value'])

while 'continuationToken' in response_pipeline_runs_prev:
    body_runs['continuationToken'] = response_pipeline_runs_prev['continuationToken']
    response_pipeline_runs_prev = requests.post(request_pipeline_runs,headers=headers,json=body_runs).json()
    total_response_pipeline_runs.extend(response_pipeline_runs_prev['value'])
    c+=1
```
Finally, we can consume the response of each API using indexing, for example, to get a name of the first pipeline listed: ```total_response_pipelines[0]['name'] ```.

Remember that is an estimate and a guideline, so if you find some difference you must modify the script considering your escenarios.

# Reference Link
- https://learn.microsoft.com/en-us/azure/data-factory/pricing-concepts
- https://azure.microsoft.com/en-us/pricing/details/data-factory/data-pipeline/
- https://learn.microsoft.com/en-us/rest/api/datafactory/pipelines/list-by-factory?tabs=HTTP
- https://learn.microsoft.com/en-us/rest/api/datafactory/integration-runtimes/list-by-factory?tabs=HTTP
- https://learn.microsoft.com/en-us/rest/api/datafactory/datasets/list-by-factory?tabs=HTTP
- https://learn.microsoft.com/en-us/rest/api/datafactory/linked-services/list-by-factory?tabs=HTTP
- https://learn.microsoft.com/en-us/rest/api/datafactory/pipeline-runs/query-by-factory?tabs=HTTP#code-try-0
- https://learn.microsoft.com/en-us/rest/api/datafactory/data-flows/list-by-factory?tabs=HTTP
