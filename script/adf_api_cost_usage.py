from decimal import ROUND_DOWN
import requests
import json
import math
#variables
tenantid = '***'
#print(tenantid)
subscription = '***'
#print(subscription)
resource_group  = '***' 
#print(resource_group)
datafactory_name = '****'
#print(datafactory_name)

headers = {"Authorization": "Bearer ***TOKEN***"}

#request pipelines
request_pipelines = f'https://management.azure.com/subscriptions/{subscription}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{datafactory_name}/pipelines?api-version=2018-06-01'
#print(request_pipelines)

params_pipelines = []
api_version = '2018-06-01'

params_pipelines.append({"api_version":f'{api_version}'})
#print(params_pipelines)
f_params_pipelines = str(params_pipelines).replace('[','').replace(']','')
#print(f_params_pipelines)

response_pipelines_prev = requests.get(request_pipelines,headers=headers).json()
#print(response_pipelines)

c = 1
total_response_pipelines = []
total_response_pipelines.extend(response_pipelines_prev['value'])
#print(len(total_response_pipelines))

while 'nextLink' in response_pipelines_prev:
    response_pipelines_prev = requests.get(response_pipelines_prev['nextLink'],headers=headers).json()
    total_response_pipelines.extend(response_pipelines_prev['value'])
    c+=1
    #print(c,len(total_response_pipelines))

#print(len(total_response_pipelines))

pipelines_count = len(total_response_pipelines)
print("se cuenta con "+ str(pipelines_count)+" pipelines en el ADF con nombre "+ datafactory_name)

#request integrationruntimes

request_integration_runtimes = f'https://management.azure.com/subscriptions/{subscription}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{datafactory_name}/integrationRuntimes?api-version=2018-06-01'
#print(request_integration_runtimes)

params_integration_runtimes = []
api_version = '2018-06-01'

params_integration_runtimes.append({"api_version":f'{api_version}'})
#print(params_integration_runtimes)
f_params_integration_runtimes = str(params_integration_runtimes).replace('[','').replace(']','')
#print(f_params_integration_runtimes)

response_integration_runtimes = requests.get(request_integration_runtimes,headers=headers).json()
#print(response_integration_runtimes)

default_integration_runtime = 	{
		"id": "/subscriptions/4eacb6e0-4d16-4ec2-9a81-facc83edff67/resourceGroups/RG-TDP-DWH-STPROC-DEV/providers/Microsoft.DataFactory/factories/dftdpdwhdev-afi01/integrationruntimes/AutoResolveIntegrationRuntime",
		"name": "AutoResolveIntegrationRuntime",
		"type": "Microsoft.DataFactory/factories/integrationruntimes",
		"properties": {
			"type": "Managed",
			"typeProperties": {
				"computeProperties": {
					"location": "AutoResolve",
					"dataFlowProperties": {
						"computeType": "General",
						"coreCount": 8,
						"timeToLive": 0
					}
				}
			}
		}
	}

response_integration_runtimes['value'].append(default_integration_runtime)
integration_runtimes_count = len(response_integration_runtimes['value'])
print("se cuenta con "+ str(integration_runtimes_count)+" integration runtimes en el ADF con nombre "+ datafactory_name)

#request dataflow

request_dataflow = f'https://management.azure.com/subscriptions/{subscription}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{datafactory_name}/dataflows?api-version=2018-06-01'
#print(request_dataflow)

params_dataflow = []
api_version = '2018-06-01'

params_dataflow.append({"api_version":f'{api_version}'})
#print(params_dataflow)
f_params_dataflow = str(params_dataflow).replace('[','').replace(']','')
#print(f_params_dataflow)

response_dataflow_prev = requests.get(request_dataflow,headers=headers).json()
#print(response_dataflow_prev)

c = 1
total_response_dataflow = []
total_response_dataflow.extend(response_dataflow_prev['value'])
#print(len(total_response_dataflow))

while 'nextLink' in response_dataflow_prev:
    response_dataflow_prev = requests.get(response_dataflow_prev['nextLink'],headers=headers).json()
    total_response_dataflow.extend(response_dataflow_prev['value'])
    c+=1
    #print(c,len(total_response_dataflow))

#print(len(total_response_dataflow))


dataflow_count = len(total_response_dataflow)
print("se cuenta con "+ str(dataflow_count)+" dataflow en el ADF con nombre "+ datafactory_name)

#request datasets

request_datasets = f'https://management.azure.com/subscriptions/{subscription}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{datafactory_name}/datasets?api-version=2018-06-01'
#print(request_datasets)

params_datasets = []
api_version = '2018-06-01'

params_datasets.append({"api_version":f'{api_version}'})
#print(params_datasets)
f_params_datasets = str(params_datasets).replace('[','').replace(']','')
#print(f_params_datasets)

response_datasets_prev = requests.get(request_datasets,headers=headers).json()
#print(response_datasets_prev)


c = 1
total_response_datasets = []
total_response_datasets.extend(response_datasets_prev['value'])
#print(len(total_response_datasets))

while 'nextLink' in response_datasets_prev:
    response_datasets_prev = requests.get(response_datasets_prev['nextLink'],headers=headers).json()
    total_response_datasets.extend(response_datasets_prev['value'])
    c+=1
    #print(c,len(total_response_datasets))

#print(len(total_response_datasets))


datasets_count = len(total_response_datasets)
print("se cuenta con "+ str(datasets_count)+" datasets en el ADF con nombre "+ datafactory_name)

#request linkedservices

request_ls = f'https://management.azure.com/subscriptions/{subscription}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{datafactory_name}/linkedservices?api-version=2018-06-01'
#print(request_ls)

params_ls = []
api_version = '2018-06-01'

params_ls.append({"api_version":f'{api_version}'})
#print(params_ls)
f_params_ls = str(params_ls).replace('[','').replace(']','')
#print(f_params_ls)

response_ls = requests.get(request_ls,headers=headers).json()
#print(response_ls)

ls_count = len(response_ls['value'])
print("se cuenta con "+ str(ls_count)+" linked services en el ADF con nombre "+ datafactory_name)

#request pipeline runs
request_pipeline_runs = f'https://management.azure.com/subscriptions/{subscription}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{datafactory_name}/queryPipelineRuns?api-version=2018-06-01'
#print(request_pipeline_runs)

params_pipeline_runs = []
api_version = '2018-06-01'

body_runs = {
    "lastUpdatedAfter": "2022-09-01T00:00:00.0000000Z",
    "lastUpdatedBefore": "2022-09-29T00:59:59.9999999Z",
    "continuationToken": ""
}

params_pipeline_runs.append({"api_version":f'{api_version}'})
#print(params_pipeline_runs)
f_params_pipeline_runs = str(params_pipeline_runs).replace('[','').replace(']','')
#print(f_params_pipeline_runs)

response_pipeline_runs_prev = requests.post(request_pipeline_runs,headers=headers,json=body_runs).json()
#print(response_pipeline_runs_prev)

c = 1
total_response_pipeline_runs = []
total_response_pipeline_runs.extend(response_pipeline_runs_prev['value'])
#print(len(total_response_pipeline_runs))

while 'continuationToken' in response_pipeline_runs_prev:
    body_runs['continuationToken'] = response_pipeline_runs_prev['continuationToken']
    response_pipeline_runs_prev = requests.post(request_pipeline_runs,headers=headers,json=body_runs).json()
    total_response_pipeline_runs.extend(response_pipeline_runs_prev['value'])
    c+=1
    #print(c,len(total_response_pipeline_runs))

#print(len(total_response_pipeline_runs))


pipeline_runs_count = len(total_response_pipeline_runs)
print("Cuenta con "+ str(pipeline_runs_count)+" pipeline runs.")



def cal_average(num):
    sum_num=0
    for t in num:
        sum_num = sum_num + t
    
    if (len(num)==0):
        #print("no tiene")
        avg=0
    else:
        avg = sum_num / len(num)
    
    return avg


#request activity runs
pipeline_run_id='2450f4b3-b47b-4557-90fb-b83197b40a29'
request_activity_runs = f'https://management.azure.com/subscriptions/{subscription}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{datafactory_name}/pipelineruns/{pipeline_run_id}/queryActivityruns?api-version=2018-06-01'

#print(request_activity_runs)

params_activity_runs = []
api_version = '2018-06-01'

params_activity_runs.append({"api_version":f'{api_version}'})
#print(params_activity_runs)
f_params_activity_runs = str(params_activity_runs).replace('[','').replace(']','')
#print(f_params_activity_runs)

#tipos y subtipos operaciones y costo por operacion
#subtipos CXO
st_read_write = 0.5/50000 
st_monitoring = 0.25/50000
st_activity_run = 1/1000
st_data_mov_Azure_IR = 0.25/60
st_ext_pipeline_act_exec = 0.00025/60
st_pipeline_activity = 0.005/60
st_dataflow_activity_general =0.274/60
st_dataflow_activity_memory =0.343/60
st_activity_run_sh = 1.5/1000
st_data_mov_SH_IR = 0.1/60
st_ext_pipeline_act_exec_SH = 0.0001/60
st_pipeline_activity_SH = 0.002/60

def round_down(n, decimals=0):
    multiplier = 10 ** decimals
    return math.floor(n * multiplier) / multiplier

def round_up(n, decimals=0):
    multiplier = 10 ** decimals
    return math.ceil(n * multiplier) / multiplier 

#Analisis
datafactory_activities_types = []
linked_services_types = []
#Tipos de actividades que tiene el data factory
for i in range(0, pipelines_count):
    dataflow_activities_count = 0
    for_each_activities_count = 0
    execute_pipeline_count = 0
    pipeline_name = total_response_pipelines[i]['name']
    #print("pipelines " + pipeline_name)
    pipeline_properties = total_response_pipelines[i]['properties']
    #print(pipeline_properties)
    pipeline_activities = total_response_pipelines[i]['properties']['activities']
    #print(pipeline_activities)
    pipeline_activities_count = len(pipeline_activities)
    #print("El pipeline "+ pipeline_name + " cuenta con "+str(pipeline_activities_count) + " activities.")
    for j in range(0, pipeline_activities_count):
        pipeline_activity_name = total_response_pipelines[i]['properties']['activities'][j]['name']
        pipeline_activity_type = total_response_pipelines[i]['properties']['activities'][j]['type']
        #print(pipeline_activity_type)
        if pipeline_activity_type not in datafactory_activities_types:
            datafactory_activities_types.append(pipeline_activity_type)
    for j in range(0,ls_count):
        pipeline_ls_type= response_ls['value'][j]['properties']['type']
        if pipeline_ls_type not in linked_services_types:
            linked_services_types.append(pipeline_ls_type)

print("El datafactory cuenta con los siguientes tipos de actividades: " + ', '.join(map(str,datafactory_activities_types))+'.')
print("El datafactory cuenta con los siguientes tipos de linked_services: " + ', '.join(map(str,linked_services_types))+'.')
print("\n")

for i in range(0, pipelines_count):
    dataflow_activities_count = 0 #listo
    dataflow_memory_activities_count = 0 #listo
    dataflow_general_activities_count = 0 #listo
    switch_count = 0 #listo
    copy_count = 0 #listo
    lookup_count = 0 #listo
    lookup_sh_count = 0
    setvariable_count = 0 #listo
    delete_count = 0 #listo
    delete_sh_count = 0
    script_count = 0 #listo
    script_sh_count = 0
    filter_count = 0 #listo
    ifcondition_count=0#listo
    for_each_activities_count = 0 #listo
    execute_pipeline_count = 0 #listo
    dataset_reference_count=0 #listo
    linked_services_count =0 #listo
    linked_service_sh_count = 0
    activity_run_sh_count = 0 
    copy_sh_count = 0
    activity_run_sh = 0
    
    #operaciones
    op_create_linked_service_unidades = 0
    op_create_dataset_unidades = 0
    op_create_pipeline_unidades = 0
    op_get_pipeline_unidades = 0
    op_run_pipeline_pipeline_unidades = 0
    op_run_pipeline_activity_unidades = 0
    op_copy_data_exec_time_unidades = 0
    op_monitor_pipeline_runs_unidades = 0
    op_execute_external_activity_unidades = 0
    op_execute_internal_activity_unidades = 0
    op_data_flow_activity_general_unidades = 0
    op_data_flow_activity_memory_unidades = 0
    op_run_pipeline_activity_sh_unidades = 0
    op_copy_data_exec_time_unidades_sh = 0
    op_execute_external_activity_unidades_sh = 0
    op_execute_internal_activity_unidades_sh = 0
    
    numero_ejecuciones = 1
    copy_DIU_IR=0
    pipeline_name = total_response_pipelines[i]['name']
    pipeline_properties = total_response_pipelines[i]['properties']
    pipeline_activities = total_response_pipelines[i]['properties']['activities']
    pipeline_activities_count = len(pipeline_activities)
    print("\n")
    print("El pipeline "+ pipeline_name + " cuenta con "+str(pipeline_activities_count) + " actividad(es).")
    for j in range(0, pipeline_activities_count):
        pipeline_activity_name = total_response_pipelines[i]['properties']['activities'][j]['name']
        pipeline_activity_type = total_response_pipelines[i]['properties']['activities'][j]['type']
        #print(pipeline_activity_type)
        #print("La actividad " + pipeline_activity_name + " es de tipo " + pipeline_activity_type)
        if (pipeline_activity_type == 'ExecuteDataFlow'):
            sources = []
            sinks = []
            sources_count=0
            sinks_count=0
            execution_time = 0
            dataflow_reference_name = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['dataflow']['referenceName']
            for k in range (0, dataflow_count):
                if(total_response_dataflow[k]['name'] == dataflow_reference_name):
                    sources_count=len(total_response_dataflow[k]['properties']['typeProperties']['sources'])
                    sinks_count=len(total_response_dataflow[k]['properties']['typeProperties']['sinks'])      
                    linked_services_count = sources_count + sinks_count
                    dataset_reference_count = sources_count + sinks_count
            try:
                dataflow_integration_runtime = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['integrationRuntime']['referenceName']
                for k in range(0,integration_runtimes_count):
                    if response_integration_runtimes['value'][k]['name'] == dataflow_integration_runtime:
                        integration_runtime_type = response_integration_runtimes['value'][k]['properties']['typeProperties']['computeProperties']['dataFlowProperties']['computeType']
                        integration_runtime_coreCount = response_integration_runtimes['value'][k]['properties']['typeProperties']['computeProperties']['dataFlowProperties']['coreCount']
                        integration_runtime_ttl = response_integration_runtimes['value'][k]['properties']['typeProperties']['computeProperties']['dataFlowProperties']['timeToLive']
                        if (integration_runtime_type == 'MemoryOptimized'):
                            execution_time = 10
                            op_data_flow_activity_memory_unidades  = execution_time*integration_runtime_coreCount
                            dataflow_memory_activities_count = dataflow_memory_activities_count+1
                        if(integration_runtime_type=='General'):
                            execution_time = 10
                            op_data_flow_activity_general_unidades  = execution_time*integration_runtime_coreCount
                            dataflow_general_activities_count = dataflow_general_activities_count +1
            except Exception as e:
                dataflow_integration_runtime = 'Custom'
                #ir_type_configure = 'compute'
                integration_runtime_type = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['compute']['computeType']
                integration_runtime_coreCount = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['compute']['coreCount']
                integration_runtime_ttl = 5
                if (integration_runtime_type == 'MemoryOptimized'): 
                    execution_time = 10
                    op_data_flow_activity_memory_unidades  = execution_time*integration_runtime_coreCount
                    dataflow_memory_activities_count = dataflow_memory_activities_count+1
                if(integration_runtime_type=='General'):
                    execution_time = 10
                    op_data_flow_activity_general_unidades  = execution_time*integration_runtime_coreCount
                    dataflow_general_activities_count = dataflow_general_activities_count +1
                #print(integration_runtime_type)
                #print(integration_runtime_coreCount)
                #print(integration_runtime_ttl)
                #print(op_data_flow_activity_memory_unidades)
                #print(op_data_flow_activity_general_unidades)
                
            print(dataflow_integration_runtime)
            print("La actividad "+ str(j+1) +"/" +str(pipeline_activities_count) + " con nombre " + pipeline_activity_name + " es de tipo " + pipeline_activity_type +" ejecuta el dataflow con nombre " + dataflow_reference_name + " en el IR "+dataflow_integration_runtime + " de tipo " + integration_runtime_type + " con una capacidad de " + str(integration_runtime_coreCount) + " y tiene un TTL de " + str(integration_runtime_ttl) + ". Además, cuenta con " + str(sources_count) + " datasets de input y " +  str(sinks_count) + " datasets de output." )
            print("Dataflow IR Capacity: "+ str(integration_runtime_coreCount))
            print("Dataflow IR TTL: "+ str(integration_runtime_ttl))
            execution_time = execution_time + integration_runtime_ttl
            dataflow_activities_count = dataflow_activities_count +1     
        elif (pipeline_activity_type == 'ForEach'):
            pipeline_activity_for_each = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['activities']
            pipeline_activity_for_each_count = len(pipeline_activity_for_each)
            print("La actividad "+ str(j+1) +"/" +str(pipeline_activities_count) + " con nombre " + pipeline_activity_name + " es de tipo " + pipeline_activity_type)
            for_each_activities_count = for_each_activities_count + 1
            for k in range(0,pipeline_activity_for_each_count):
                for_each_activity_name = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['activities'][k]['name']
                for_each_activity_type = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['activities'][k]['type']
                if(for_each_activity_type == 'ExecutePipeline'):
                    print("La subactividad del foreach es de tipo "+for_each_activity_type+ " y ejecuta el pipeline "+ for_each_activity_name)
                    execute_pipeline_count = execute_pipeline_count+1
                else:
                    print("La subactividad de tipo "+for_each_activity_type+ " con nombre "+ for_each_activity_name + "no se encuentra mapeada." )
        elif (pipeline_activity_type=='Lookup'):
            pipeline_activity_lookup_dataset = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['dataset']['referenceName']
            lookup_count = lookup_count + 1
            dataset_reference_count = dataset_reference_count+1
            for k in range(0,datasets_count):
                if(total_response_datasets[k]['name']==pipeline_activity_lookup_dataset):
                    dataset_linkedservice_name = total_response_datasets[k]['properties']['linkedServiceName']['referenceName']
                    print("La actividad "+ str(j+1) +"/" +str(pipeline_activities_count) + " con nombre " + pipeline_activity_name + " es de tipo " + pipeline_activity_type + " y consume el dataset "+ pipeline_activity_lookup_dataset + " usando el linked service "+dataset_linkedservice_name)
                    for l in range(0,ls_count):
                        if(response_ls['value'][l]['name'] == dataset_linkedservice_name ):
                            dataset_linkedservice_type=response_ls['value'][l]['properties']['type']
                            if(dataset_linkedservice_type == 'FileServer' or dataset_linkedservice_type == 'Sftp' or dataset_linkedservice_type == 'Oracle'):
                                pipeline_activity_lookup_ir_name = response_ls['value'][l]['properties']['connectVia']['referenceName']
                                for m in range (0, integration_runtimes_count):
                                    if (response_integration_runtimes['value'][m]['name']==pipeline_activity_lookup_ir_name):
                                        dataset_linkedservice_ir_type=response_integration_runtimes['value'][m]['properties']['type']
                                        if(dataset_linkedservice_ir_type=='SelfHosted'):
                                            linked_service_sh_count=linked_service_sh_count+1
                                            lookup_sh_count=lookup_sh_count+1
                            else:
                                linked_services_count=linked_services_count+1
        elif (pipeline_activity_type=='SetVariable'):
            pipeline_activity_variable = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['variableName']
            print("La actividad "+ str(j+1) +"/" +str(pipeline_activities_count) + " con nombre " + pipeline_activity_name + " es de tipo " + pipeline_activity_type + " y genera la variable "+ pipeline_activity_variable)
            setvariable_count = setvariable_count +1
        elif (pipeline_activity_type=='Filter'):
            pipeline_activity_condition = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['condition']['value']
            print("La actividad "+ str(j+1) +"/" +str(pipeline_activities_count) + " con nombre " + pipeline_activity_name + " es de tipo " + pipeline_activity_type + " y contiene la siguiente condicion "+ pipeline_activity_condition)
            filter_count = filter_count+1
        elif (pipeline_activity_type=='Script'):
            pipeline_activity_sp_ls_name = total_response_pipelines[i]['properties']['activities'][j]['linkedServiceName']['referenceName']
            print("La actividad "+ str(j+1) +"/" +str(pipeline_activities_count) + " con nombre " + pipeline_activity_name + " es de tipo " + pipeline_activity_type + " se ejecuta en "+ pipeline_activity_sp_ls_name)
            for k in range(0,ls_count):
                if(response_ls['value'][k]['name'] == pipeline_activity_sp_ls_name ):
                    dataset_linkedservice_type=response_ls['value'][k]['properties']['type']
                    if(dataset_linkedservice_type == 'FileServer' or dataset_linkedservice_type == 'Sftp' or dataset_linkedservice_type == 'Oracle'):
                        pipeline_activity_lookup_ir_name = response_ls['value'][k]['properties']['connectVia']['referenceName']
                        for l in range (0, integration_runtimes_count):
                            if (response_integration_runtimes['value'][l]['name']==pipeline_activity_lookup_ir_name):
                                dataset_linkedservice_ir_type=response_integration_runtimes['value'][l]['properties']['type']
                                if(dataset_linkedservice_ir_type=='SelfHosted'):
                                    script_sh_count = script_sh_count +1
                                    linked_service_sh_count = linked_service_sh_count +1
                    else:
                        script_count= script_count+1
                        linked_services_count=linked_services_count+1
        elif (pipeline_activity_type=='Delete'):
            pipeline_activity_delete_dataset = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['dataset']['referenceName']
            dataset_reference_count = dataset_reference_count +1
            for k in range(0,datasets_count):
                if(total_response_datasets[k]['name']==pipeline_activity_delete_dataset):
                    dataset_linkedservice_name = total_response_datasets[k]['properties']['linkedServiceName']['referenceName']
                    print("La actividad "+ str(j+1) +"/" +str(pipeline_activities_count) + " con nombre " + pipeline_activity_name + " es de tipo " + pipeline_activity_type + " elimina objetos en el ds "+ pipeline_activity_delete_dataset + " usando el linked serice " + dataset_linkedservice_name)
                    for l in range(0,ls_count):
                        if (response_ls['value'][l]['name'] == dataset_linkedservice_name):
                            dataset_linkedservice_type=response_ls['value'][l]['properties']['type']
                            if(dataset_linkedservice_type == 'FileServer' or dataset_linkedservice_type == 'Sftp' or dataset_linkedservice_type == 'Oracle'):
                                pipeline_activity_delete_ir_name = response_ls['value'][k]['properties']['connectVia']['referenceName']
                                for m in range(0,integration_runtimes_count):
                                    if(response_integration_runtimes['value'][l]['name']==pipeline_activity_delete_ir_name):
                                        dataset_linkedservice_ir_type=response_integration_runtimes['value'][l]['properties']['type']
                                        if(dataset_linkedservice_ir_type=='SelfHosted'):
                                            delete_sh_count = delete_sh_count +1
                                            linked_service_sh_count = linked_service_sh_count +1
                            else: 
                                delete_count=delete_count+1
                                linked_services_count=linked_services_count+1
        elif (pipeline_activity_type=='Copy'):
            pipeline_activity_copy_source_ds = total_response_pipelines[i]['properties']['activities'][j]['inputs']
            pipeline_activity_copy_source_ds_count = len(pipeline_activity_copy_source_ds)
            inputs = []
            for k in range(0,pipeline_activity_copy_source_ds_count):
                input_name = total_response_pipelines[i]['properties']['activities'][j]['inputs'][k]['referenceName']
                inputs.append(input_name)
                dataset_reference_count = dataset_reference_count+1
            pipeline_activity_copy_sink_ds = total_response_pipelines[i]['properties']['activities'][j]['inputs']
            pipeline_activity_copy_sink_ds_count = len(pipeline_activity_copy_sink_ds)
            outputs = []
            for k in range(0,pipeline_activity_copy_source_ds_count):
                output_name = total_response_pipelines[i]['properties']['activities'][j]['outputs'][k]['referenceName']
                outputs.append(output_name)
                dataset_reference_count = dataset_reference_count+1
            linked_services = []
            for k in range(0,len(inputs)):
                for l in range(0,datasets_count):
                    if(total_response_datasets[l]['name']==inputs[k]):
                        dataset_linkedservice_name = total_response_datasets[l]['properties']['linkedServiceName']['referenceName']
                        linked_services.append(dataset_linkedservice_name)
                        for m in range(0, ls_count):
                            if (response_ls['value'][m]['name'] == dataset_linkedservice_name):
                                dataset_linkedservice_type=response_ls['value'][m]['properties']['type']
                                if(dataset_linkedservice_type == 'FileServer' or dataset_linkedservice_type == 'Sftp' or dataset_linkedservice_type == 'Oracle'):
                                    dataset_linkedservice_ir_name=response_ls['value'][m]['properties']['connectVia']['referenceName']
                                    for n in range(0,integration_runtimes_count):
                                        dataset_linkedservice_ir_type=response_integration_runtimes['value'][n]['properties']['type']
                                        if(dataset_linkedservice_ir_type=='SelfHosted'):
                                            linked_service_sh_count = linked_service_sh_count+1
                                            activity_run_sh = activity_run_sh+1
                                            copy_sh_count = copy_sh_count+1
                                else:
                                    dataset_linkedservice_ir_name='AutoResolveIntegrationRuntime'
                                    copy_DIU_IR = copy_DIU_IR + response_integration_runtimes['value'][2]['properties']['typeProperties']['computeProperties']['dataFlowProperties']['coreCount']
                                    linked_services_count = linked_services_count +1
                                    copy_count = copy_count+1                     
            for k in range(0,len(outputs)):
                for l in range(0,datasets_count):
                    if(total_response_datasets[l]['name']==outputs[k]):
                        dataset_linkedservice_name = total_response_datasets[l]['properties']['linkedServiceName']['referenceName']
                        linked_services.append(dataset_linkedservice_name)
                        for m in range(0, ls_count):
                            if (response_ls['value'][m]['name'] == dataset_linkedservice_name):
                                dataset_linkedservice_type=response_ls['value'][m]['properties']['type']
                                print(dataset_linkedservice_type)
                                if(dataset_linkedservice_type == 'FileServer' or dataset_linkedservice_type == 'Sftp' or dataset_linkedservice_type == 'Oracle'):
                                    dataset_linkedservice_ir_name=response_ls['value'][m]['properties']['connectVia']['referenceName']
                                    for n in range(0,integration_runtimes_count):
                                        dataset_linkedservice_ir_type=response_integration_runtimes['value'][n]['properties']['type']
                                        if(dataset_linkedservice_ir_type=='SelfHosted'):
                                            linked_service_sh_count = linked_service_sh_count+1
                                            activity_run_sh = activity_run_sh+1
                                            copy_sh_count = copy_sh_count+1
                                else:
                                    linked_services_count = linked_services_count +1
                                    copy_DIU_IR = copy_DIU_IR + response_integration_runtimes['value'][2]['properties']['typeProperties']['computeProperties']['dataFlowProperties']['coreCount']
                                    copy_count = copy_count+1
            print("La actividad "+ str(j+1) +"/" +str(pipeline_activities_count) + " con nombre " + pipeline_activity_name + " es de tipo " + pipeline_activity_type + " cuenta con los siguientes inputs: "+ ', '.join(map(str,inputs)) + ", outputs: " + ', '.join(map(str,outputs)) + " y linked services: "+ ', '.join(map(str,linked_services)) + ".") 
        elif (pipeline_activity_type=='Switch'):
            pipeline_activity_switch_cases = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['cases']
            pipeline_activity_switch_cases_count = len(pipeline_activity_switch_cases)
            print("La actividad "+ str(j+1) +"/" +str(pipeline_activities_count) + " con nombre " + pipeline_activity_name + " es de tipo " + pipeline_activity_type + " cuenta con las siguientes subactividades: ") 
            for k in range(0,pipeline_activity_switch_cases_count):
                pipeline_activity_switch_cases_activities = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['cases'][k]['activities']
                pipeline_activity_switch_cases_activities_count= len(pipeline_activity_switch_cases_activities)
                for l in range(0,pipeline_activity_switch_cases_activities_count):
                    execution_time = 0
                    pipeline_activity_switch_cases_activities_name = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['cases'][k]['activities'][l]['name']
                    pipeline_activity_switch_cases_activities_type = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['cases'][k]['activities'][l]['type']
                    if(pipeline_activity_switch_cases_activities_type=='ExecutePipeline'):
                        print("La subactividad corresponde a " + pipeline_activity_switch_cases_activities_type + " ejecutando el pipeline "+pipeline_activity_switch_cases_activities_name)
                        execute_pipeline_count = execute_pipeline_count+1
                    elif (pipeline_activity_switch_cases_activities_type=='ExecuteDataFlow'):
                        pipeline_activity_switch_cases_activities_type_IR = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['cases'][k]['activities'][l]['typeProperties']['integrationRuntime']['referenceName']
                        pipeline_activity_switch_cases_activities_dataflow_name = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['cases'][k]['activities'][l]['typeProperties']['dataflow']['referenceName']
                        #print(pipeline_activity_switch_cases_activities_type_IR)
                        for k in range (0, dataflow_count):
                            if(total_response_dataflow[k]['name'] == dataflow_reference_name):
                                sources_count=len(total_response_dataflow[k]['properties']['typeProperties']['sources'])
                                sinks_count=len(total_response_dataflow[k]['properties']['typeProperties']['sinks'])      
                                linked_services_count = sources_count + sinks_count
                                dataset_reference_count = sources_count + sinks_count
                        for m in range(0,integration_runtimes_count):
                            if response_integration_runtimes['value'][m]['name'] == pipeline_activity_switch_cases_activities_type_IR:
                                integration_runtime_type = response_integration_runtimes['value'][m]['properties']['typeProperties']['computeProperties']['dataFlowProperties']['computeType']
                                integration_runtime_coreCount = response_integration_runtimes['value'][m]['properties']['typeProperties']['computeProperties']['dataFlowProperties']['coreCount']
                                integration_runtime_ttl = response_integration_runtimes['value'][m]['properties']['typeProperties']['computeProperties']['dataFlowProperties']['timeToLive']
                                if (integration_runtime_type == 'MemoryOptimized'):
                                    execution_time = 10
                                    op_data_flow_activity_memory_unidades  = execution_time*integration_runtime_coreCount
                                    dataflow_memory_activities_count = dataflow_memory_activities_count+1
                                if(integration_runtime_type=='General'):
                                    execution_time = 10
                                    op_data_flow_activity_general_unidades  = execution_time*integration_runtime_coreCount
                                    dataflow_general_activities_count = dataflow_general_activities_count +1
                        print("La subactividad corresponde a " + pipeline_activity_switch_cases_activities_type + " ejecutando el dataflow "+pipeline_activity_switch_cases_activities_name + " utilizando el IR " + pipeline_activity_switch_cases_activities_type_IR + " con una capacidad de " + str(integration_runtime_coreCount) + " y tiene un TTL de " + str(integration_runtime_ttl) + ". Además, cuenta con " + str(sources_count) + " datasets de input y " +  str(sinks_count) + " datasets de output." )
                        print("Dataflow IR Capacity: "+ str(integration_runtime_coreCount))
                        print("Dataflow IR TTL: "+ str(integration_runtime_ttl))
                        execution_time = execution_time + integration_runtime_ttl
                        dataflow_activities_count=dataflow_activities_count+1
                    else:
                        print("La subactividad ccorresponde a " + pipeline_activity_switch_cases_activities_type + " con nombre "+pipeline_activity_switch_cases_activities_name + " no se encuentra identificada")
            switch_count=switch_count+1
        elif (pipeline_activity_type=='IfCondition'):
            try:
                pipeline_activity_ifcondition_true = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['ifTrueActivities']
                true_actividades = 1
            except Exception as e:
                pipeline_activity_ifcondition_false = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['ifFalseActivities']
                true_actividades = 0
                false_activities = 1
            if (true_actividades == 1):
                pipeline_activity_ifcondition_true_count = len(pipeline_activity_ifcondition_true)
                print("La actividad "+ str(j+1) +"/" +str(pipeline_activities_count) + " con nombre " + pipeline_activity_name + " es de tipo " + pipeline_activity_type + " cuenta con las siguientes subactividades: ") 
                for k in range(0,pipeline_activity_ifcondition_true_count):
                    execution_time = 0
                    pipeline_activity_ifcondition_activities_name = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['ifTrueActivities'][k]['name']
                    pipeline_activity_ifcondition_activities_type = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['ifTrueActivities'][k]['type']
                    if(pipeline_activity_ifcondition_activities_type=='ExecutePipeline'):
                        print("La subactividad corresponde a " + pipeline_activity_ifcondition_activities_type + " ejecutando el pipeline "+pipeline_activity_ifcondition_activities_name)
                        execute_pipeline_count = execute_pipeline_count+1
                    elif (pipeline_activity_ifcondition_activities_type=='ExecuteDataFlow'):
                        try:
                            pipeline_activity_ifcondition_activities_type_IR = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['ifTrueActivities'][k]['typeProperties']['integrationRuntime']['referenceName']
                            pipeline_activity_ifcondition_activities_dataflow_name = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['ifTrueActivities'][k]['typeProperties']['dataflow']['referenceName']
                            dataflow_integration_runtime=pipeline_activity_ifcondition_activities_type_IR
                            #print(pipeline_activity_ifcondition_activities_type_IR)
                            for k in range (0, dataflow_count):
                                if(total_response_dataflow[k]['name'] == dataflow_reference_name):
                                    sources_count=len(total_response_dataflow[k]['properties']['typeProperties']['sources'])
                                    sinks_count=len(total_response_dataflow[k]['properties']['typeProperties']['sinks'])      
                                    linked_services_count = sources_count + sinks_count
                                    dataset_reference_count = sources_count + sinks_count
                            for m in range(0,integration_runtimes_count):
                                if response_integration_runtimes['value'][m]['name'] == pipeline_activity_ifcondition_activities_type_IR:
                                    integration_runtime_type = response_integration_runtimes['value'][m]['properties']['typeProperties']['computeProperties']['dataFlowProperties']['computeType']
                                    integration_runtime_coreCount = response_integration_runtimes['value'][m]['properties']['typeProperties']['computeProperties']['dataFlowProperties']['coreCount']
                                    integration_runtime_ttl = response_integration_runtimes['value'][m]['properties']['typeProperties']['computeProperties']['dataFlowProperties']['timeToLive']
                                    if (integration_runtime_type == 'MemoryOptimized'):
                                        execution_time = 10
                                        op_data_flow_activity_memory_unidades  = execution_time*integration_runtime_coreCount
                                        dataflow_memory_activities_count = dataflow_memory_activities_count+1
                                    if(integration_runtime_type=='General'):
                                        execution_time = 10
                                        op_data_flow_activity_general_unidades  = execution_time*integration_runtime_coreCount
                                        dataflow_general_activities_count = dataflow_general_activities_count +1
                        except Exception as e:
                            dataflow_integration_runtime = 'Custom'
                            #ir_type_configure = 'compute'
                            integration_runtime_type = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['ifTrueActivities'][k]['typeProperties']['compute']['computeType']
                            integration_runtime_coreCount = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['ifTrueActivities'][k]['typeProperties']['compute']['coreCount']
                            integration_runtime_ttl = 5
                            if (integration_runtime_type == 'MemoryOptimized'): 
                                execution_time = 10
                                op_data_flow_activity_memory_unidades  = execution_time*integration_runtime_coreCount
                                dataflow_memory_activities_count = dataflow_memory_activities_count+1
                            if(integration_runtime_type=='General'):
                                execution_time = 10
                                op_data_flow_activity_general_unidades  = execution_time*integration_runtime_coreCount
                                dataflow_general_activities_count = dataflow_general_activities_count +1
                        print("La subactividad corresponde a " + pipeline_activity_ifcondition_activities_type + " ejecutando el dataflow "+pipeline_activity_ifcondition_activities_name + " utilizando el IR " + dataflow_integration_runtime + " con una capacidad de " + str(integration_runtime_coreCount) + " y tiene un TTL de " + str(integration_runtime_ttl) + ". Además, cuenta con " + str(sources_count) + " datasets de input y " +  str(sinks_count) + " datasets de output." )
                        print("Dataflow IR Capacity: "+ str(integration_runtime_coreCount))
                        print("Dataflow IR TTL: "+ str(integration_runtime_ttl))
                        execution_time = execution_time + integration_runtime_ttl
                        dataflow_activities_count=dataflow_activities_count+1
                    else:
                        print("La subactividad ccorresponde a " + pipeline_activity_ifcondition_activities_type + " con nombre "+pipeline_activity_ifcondition_activities_name + " no se encuentra identificada")
                ifcondition_count=ifcondition_count+1
            elif(false_activities == 1):
                pipeline_activity_ifcondition_false_count = len(pipeline_activity_ifcondition_false)
                print("La actividad "+ str(j+1) +"/" +str(pipeline_activities_count) + " con nombre " + pipeline_activity_name + " es de tipo " + pipeline_activity_type + " cuenta con las siguientes subactividades: ") 
                for k in range(0,pipeline_activity_ifcondition_false_count):
                    execution_time = 0
                    pipeline_activity_ifcondition_activities_name = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['ifFalseActivities'][k]['name']
                    pipeline_activity_ifcondition_activities_type = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['ifFalseActivities'][k]['type']
                    if(pipeline_activity_ifcondition_activities_type=='ExecutePipeline'):
                        print("La subactividad corresponde a " + pipeline_activity_ifcondition_activities_type + " ejecutando el pipeline "+pipeline_activity_ifcondition_activities_name)
                        execute_pipeline_count = execute_pipeline_count+1
                    elif (pipeline_activity_ifcondition_activities_type=='ExecuteDataFlow'):
                        try:
                            pipeline_activity_ifcondition_activities_type_IR = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['ifFalseActivities'][k]['typeProperties']['integrationRuntime']['referenceName']
                            pipeline_activity_ifcondition_activities_dataflow_name = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['ifFalseActivities'][k]['typeProperties']['dataflow']['referenceName']
                            dataflow_integration_runtime=pipeline_activity_ifcondition_activities_type_IR
                            #print(pipeline_activity_ifcondition_activities_type_IR)
                            for k in range (0, dataflow_count):
                                if(total_response_dataflow[k]['name'] == dataflow_reference_name):
                                    sources_count=len(total_response_dataflow[k]['properties']['typeProperties']['sources'])
                                    sinks_count=len(total_response_dataflow[k]['properties']['typeProperties']['sinks'])      
                                    linked_services_count = sources_count + sinks_count
                                    dataset_reference_count = sources_count + sinks_count
                            for m in range(0,integration_runtimes_count):
                                if response_integration_runtimes['value'][m]['name'] == pipeline_activity_ifcondition_activities_type_IR:
                                    integration_runtime_type = response_integration_runtimes['value'][m]['properties']['typeProperties']['computeProperties']['dataFlowProperties']['computeType']
                                    integration_runtime_coreCount = response_integration_runtimes['value'][m]['properties']['typeProperties']['computeProperties']['dataFlowProperties']['coreCount']
                                    integration_runtime_ttl = response_integration_runtimes['value'][m]['properties']['typeProperties']['computeProperties']['dataFlowProperties']['timeToLive']
                                    if (integration_runtime_type == 'MemoryOptimized'):
                                        execution_time = 10
                                        op_data_flow_activity_memory_unidades  = execution_time*integration_runtime_coreCount
                                        dataflow_memory_activities_count = dataflow_memory_activities_count+1
                                    if(integration_runtime_type=='General'):
                                        execution_time = 10
                                        op_data_flow_activity_general_unidades  = execution_time*integration_runtime_coreCount
                                        dataflow_general_activities_count = dataflow_general_activities_count +1
                        except Exception as e:
                            dataflow_integration_runtime = 'Custom'
                            #ir_type_configure = 'compute'
                            integration_runtime_type = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['ifFalseActivities'][k]['typeProperties']['compute']['computeType']
                            integration_runtime_coreCount = total_response_pipelines[i]['properties']['activities'][j]['typeProperties']['ifFalseActivities'][k]['typeProperties']['compute']['coreCount']
                            integration_runtime_ttl = 5
                            if (integration_runtime_type == 'MemoryOptimized'): 
                                execution_time = 10
                                op_data_flow_activity_memory_unidades  = execution_time*integration_runtime_coreCount
                                dataflow_memory_activities_count = dataflow_memory_activities_count+1
                            if(integration_runtime_type=='General'):
                                execution_time = 10
                                op_data_flow_activity_general_unidades  = execution_time*integration_runtime_coreCount
                                dataflow_general_activities_count = dataflow_general_activities_count +1
                        print("La subactividad corresponde a " + pipeline_activity_ifcondition_activities_type + " ejecutando el dataflow "+pipeline_activity_ifcondition_activities_name + " utilizando el IR " + dataflow_integration_runtime + " con una capacidad de " + str(integration_runtime_coreCount) + " y tiene un TTL de " + str(integration_runtime_ttl) + ". Además, cuenta con " + str(sources_count) + " datasets de input y " +  str(sinks_count) + " datasets de output." )
                        print("Dataflow IR Capacity: "+ str(integration_runtime_coreCount))
                        print("Dataflow IR TTL: "+ str(integration_runtime_ttl))
                        execution_time = execution_time + integration_runtime_ttl
                        dataflow_activities_count=dataflow_activities_count+1
                    else:
                        print("La subactividad ccorresponde a " + pipeline_activity_ifcondition_activities_type + " con nombre "+pipeline_activity_ifcondition_activities_name + " no se encuentra identificada")
                ifcondition_count=ifcondition_count+1
        else:
            print("La actividad "+ str(j+1) +"/" +str(pipeline_activities_count) + " con nombre " + pipeline_activity_name + " es de tipo " + pipeline_activity_type + " no se encuenetra identificada.") 
    print("DataFlow General Activites: "+ str(dataflow_general_activities_count))
    print("DataFlow Memory Activites: "+ str(dataflow_memory_activities_count))
    print("Dataflow input datasets: "+ str(sources_count))
    print("Dataflow output datasets: "+ str(sinks_count))
    print("Dataflow execution time: "+ str(execution_time))
    print("ForEach Activites: " + str(for_each_activities_count))
    print("ExecutePipeline Activites: " + str(execute_pipeline_count))
    print("switch Activities: "+ str(switch_count))
    print("copy Activities: "+ str(copy_count))
    print("copy SH Activities: "+ str(copy_sh_count))
    print("lookup Activities: "+ str(lookup_count))
    print("lookup SH Activities: "+ str(lookup_sh_count))
    print("setvariable Activities: "+ str(setvariable_count))
    print("delete Activities: "+ str(delete_count))
    print("delete SH Activities: "+ str(delete_sh_count))
    print("script Activities: "+ str(script_count))
    print("script SH Activities: "+ str(script_sh_count))
    print("filter Activities: "+ str(filter_count))
    print("Dataset References: "+ str(dataset_reference_count))
    print("Linkedservice References: "+ str(linked_services_count))
    print("Linkedservice SH References:" + str(linked_service_sh_count))
    print("Activity Run SH references:" + str(activity_run_sh))
    print("Copy SH references:" + str(copy_sh_count))
    print("Ifcondition Activities" + str(ifcondition_count))
    
    copy_duracion=10
    print("Copy DIUs: "+str(copy_DIU_IR))
        
    #operacion_unidades
    print("\n")
    op_create_linked_service_unidades  = linked_services_count
    print("Create Linked Service Operations: " + str(op_create_linked_service_unidades))
    op_create_dataset_unidades  = dataset_reference_count+linked_services_count
    print("Create Datasets Operations: " + str(op_create_dataset_unidades))
    op_create_pipeline_unidades  = dataset_reference_count+ int(len(str(i)))
    print("Create Pipeline Operations: " + str(op_create_pipeline_unidades))
    op_get_pipeline_unidades  = int(len(str(i)))
    print("Get Pipeline Operations: " + str(op_get_pipeline_unidades))
    op_run_pipeline_pipeline_unidades  = numero_ejecuciones
    print("Run Pipeline Operations: " + str(op_run_pipeline_pipeline_unidades))
    op_run_pipeline_activity_unidades  = copy_count
    print("run_pipeline_activity_unidades: "+ str(op_run_pipeline_activity_unidades))
    op_copy_data_exec_time_unidades  = copy_duracion * copy_DIU_IR
    print("op_copy_data_exec_time_unidades: "+ str(op_copy_data_exec_time_unidades))
    op_monitor_pipeline_runs_unidades  = numero_ejecuciones + dataflow_general_activities_count+dataflow_memory_activities_count+for_each_activities_count+execute_pipeline_count+switch_count + copy_count + lookup_count + setvariable_count + delete_count + script_count + filter_count
    print("Monitor Pipeline Assumptions: "+str(op_monitor_pipeline_runs_unidades))
    op_execute_external_activity_unidades  = 0
    print("op_execute_external_activity_unidades: "+ str(op_execute_external_activity_unidades))
    op_execute_internal_activity_unidades  = for_each_activities_count+execute_pipeline_count+switch_count + copy_count + lookup_count + setvariable_count + delete_count + script_count + filter_count
    print("op_execute_internal_activity_unidades: "+str(op_execute_internal_activity_unidades))
    #op_data_flow_activity_general_unidades  = execution_time*integration_runtime_coreCount
    print("Data Flow General Assumptions: " + str(op_data_flow_activity_general_unidades))
    #op_data_flow_activity_memory_unidades  = execution_time*integration_runtime_coreCount
    print("Data Flow Memory Assumptions: " + str(op_data_flow_activity_memory_unidades))
    op_run_pipeline_activity_sh_unidades  = copy_count
    print("op_run_pipeline_activity_sh_unidades: "+str(op_run_pipeline_activity_sh_unidades))
    op_copy_data_exec_time_unidades_sh  = copy_duracion * copy_DIU_IR
    print("op_copy_data_exec_time_unidades_sh: "+ str(op_copy_data_exec_time_unidades_sh))
    op_execute_external_activity_unidades_sh  = 0
    print("op_execute_external_activity_unidades_sh: "+str(op_execute_external_activity_unidades_sh))
    op_execute_internal_activity_unidades_sh  = for_each_activities_count+execute_pipeline_count+switch_count + copy_count + lookup_count + setvariable_count + delete_count + script_count + filter_count
    print("op_execute_internal_activity_unidades_sh: "+str(op_execute_internal_activity_unidades_sh))
    
    #operacion_subtotal
    print("\n")
    op_create_linked_service = round(op_create_linked_service_unidades*st_read_write,5)
    #print(op_create_linked_service)
    op_create_dataset = round(op_create_dataset_unidades*st_read_write,5)
    #print(op_create_dataset)
    op_create_pipeline = round(op_create_pipeline_unidades*st_read_write,5)
    #print(op_create_pipeline)
    op_get_pipeline = round(op_get_pipeline_unidades*st_read_write,5)
    #print(op_get_pipeline)
    op_run_pipeline_pipeline = round(op_run_pipeline_pipeline_unidades*st_activity_run,3)
    #print(op_run_pipeline_pipeline)
    op_run_pipeline_activity = round(op_run_pipeline_activity_unidades*st_activity_run,3)
    #print(op_run_pipeline_activity)
    op_copy_data_exec_time = round_down(op_copy_data_exec_time_unidades*st_data_mov_Azure_IR,3)
    #print(op_copy_data_exec_time)
    op_monitor_pipeline_runs = round(op_monitor_pipeline_runs_unidades*st_monitoring,6)
    #print(op_monitor_pipeline_runs)
    op_execute_adb_activity = round_down(op_execute_external_activity_unidades*st_ext_pipeline_act_exec,6)
    #print(op_execute_adb_activity)
    op_execute_lookup_activity = round(op_execute_internal_activity_unidades*st_pipeline_activity,4)
    #print(op_execute_lookup_activity)
    op_data_flow_activity_general = round(op_data_flow_activity_general_unidades*st_dataflow_activity_general,4)
    #print(op_data_flow_activity_general)
    op_data_flow_activity_memory = round(op_data_flow_activity_memory_unidades*st_dataflow_activity_memory,4)
    #print(op_data_flow_activity_memory)
    op_run_pipeline_activity_sh = round(op_run_pipeline_activity_sh_unidades*st_activity_run_sh,4)
    #print(op_run_pipeline_activity_sh)
    op_copy_data_exec_time_sh = round(op_copy_data_exec_time_unidades_sh*st_data_mov_SH_IR,4)
    #print(op_copy_data_exec_time_sh)
    op_execute_adb_activity_sh = round(op_execute_external_activity_unidades_sh*st_ext_pipeline_act_exec_SH,4)
    #print(op_execute_adb_activity_sh)
    op_execute_lookup_activity_sh = round(op_execute_internal_activity_unidades_sh*st_pipeline_activity_SH,4)
    #print(op_execute_lookup_activity_sh)
    #total
    pipeline_total = round_up(op_create_linked_service + op_create_dataset + op_create_pipeline + op_get_pipeline + op_run_pipeline_pipeline + op_run_pipeline_activity + op_copy_data_exec_time + op_monitor_pipeline_runs + op_execute_adb_activity + op_execute_lookup_activity + op_data_flow_activity_general + op_data_flow_activity_memory + op_run_pipeline_activity_sh + op_copy_data_exec_time_sh + op_execute_adb_activity_sh + op_execute_lookup_activity_sh,4)
    print("el costo del pipeline es " + str(pipeline_total))

