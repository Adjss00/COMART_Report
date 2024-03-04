from helpers.ObjectExtractor import ObjectExtractor
from controllers.SetOpps import SetOpps
from controllers.SetMeets import SetMeets 

if __name__ == "__main__":
    #* Access data to SF
    username = 'jesus.sanchez@engen.com.mx'
    password = '21558269Antonio#'
    security_token = 'WqRoCDbMwhMPZ62iWUcXnmbmg'

    #* helpers/ObjectExtractor
    extractor = ObjectExtractor(username, password, security_token)

    objects_to_extract = [
        ('Account', 'accounts_df', ['Id', 'Name', 'ParentId', 'CreatedDate', 'OwnerId', 'ACC_tx_EXT_REF_ID__c', 'Region__c', 'ACC_tx_Account_Source__c', 'ACC_dv_Sales_Annual_Revenue__c' ]),
        ('Opportunity', 'opportunities_df', ['Id', 'AccountId', 'Name', 'StageName', 'Amount', 'CurrencyIsoCode', 'CreatedDate', 'OwnerId', 'OPP_ls_Region__c', 'OPP_tx_EXT_REF_ID__c', 'Target_Market__c']),
        ('User', 'users_df', ['Id', 'Name']),
        ('Event', 'events_df', ['Id', 'Subject', 'ActivityDate', 'AccountId', 'OwnerId', 'CreatedDate', 'CreatedById', 'ACT_ls_Activity_Outcome__c', 'Meeting_Source__c', 'OwnerName__c']),
    ]

    extracted_dataframes = extractor.extract_objects_to_dataframes(objects_to_extract)

    #* controllers/SetOpps
    # Inicializa la clase SetOpps pasando los DataFrames como argumentos
    data_processor = SetOpps(extracted_dataframes['accounts_df'], extracted_dataframes['users_df'], extracted_dataframes['opportunities_df'])

    # Procesa los datos y agrega campos vacíos al DataFrame de oportunidades
    empty_fields_list = ['Opportunity Owner', 'EXT REF ID ACC', 'Fecha Cita', 'Fecha Opp', 'Cuenta Opp', 'Cuenta Unica', 'Mes Cita', 'Meeting Source', 'Source', 'Pipeline', 'Ventas', 'Mes Fecha', 'Validate', 'Medio', 'Segmento Consejo', 'Tipo de Cambio', 'Amount MXN']
    nuevo_df = data_processor.process_data(empty_fields_list, 'set_opps_df')
    data_processor.export_to_excel(nuevo_df, 'out/oportunidades.xlsx')


    #* controllers/SetMeets
    # Inicializa la clase SetOpps pasando los DataFrames como argumentos
    data_processor_meets = SetMeets(extracted_dataframes['accounts_df'], extracted_dataframes['users_df'], extracted_dataframes['events_df'])

    # Procesa los datos y agrega campos vacíos al DataFrame de oportunidades
    empty_fields_list_meets = ['Cuenta Telemarketing', 'Unica', 'Created date 2', 'Mes Cita', 'Inbound', ' Inbound Digital', 'Tipo Outbound', 'Ventas', 'Target Market', 'medio', 'debajo de 50MM', 'Año', ' x ', 'Segmento Consejo', 'Column2' ]
    nuevo_df_meets = data_processor_meets.process_data_meets(empty_fields_list_meets, 'set_meets_df')
    data_processor_meets.export_to_excel(nuevo_df_meets, 'out/citas telemarketing.xlsx')



