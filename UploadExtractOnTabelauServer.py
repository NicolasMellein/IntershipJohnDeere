
import tableauserverclient as TSC

#Open password From File
with open("C:\\Nico\\Password.txt") as f:

    password = f.read()



# create an auth object
tableau_auth = TSC.TableauAuth(username='junxonm', password=password)

# create an instance for your server
server = TSC.Server('')

# call the sign-in method with the auth object
server.auth.sign_in(tableau_auth)

with server.auth.sign_in(tableau_auth):

    #searching for information on tableau-server Equals= kemper Dashboard

    req_option = TSC.RequestOptions()
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                     TSC.RequestOptions.Operator.Equals,
                                     'KemperDashboard'))


    matching_workbooks, pagination_item = server.workbooks.get(req_option)

    print(matching_workbooks[0].name)
    print(matching_workbooks[0].id)


    #datasources.publish(datasource_item, file_path, mode, connection_credentials=None)

    #Documentation: https://tableau.github.io/server-client-python/docs/api-ref#datasourcespublish

    #projectID = Project-ID von “Supply Management Shared Services Region 2 (Restricted)”: 

    project_id = ''
    file_path = 'C:/Users/junxonm/Desktop/test_hyper_extract_api.hyper'

    # Use the project id to create new datsource_item
    new_datasource = TSC.DatasourceItem(project_id)

    # publish data source (specified in file_path)
    new_datasource = server.datasources.publish(new_datasource, file_path, 'CreateNew')




