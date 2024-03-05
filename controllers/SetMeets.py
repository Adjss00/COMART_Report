import pandas as pd
from datetime import datetime, timedelta

class SetMeets:
    def __init__(self, accounts_df, users_df, events_df):
        self.accounts_df = accounts_df
        self.users_df = users_df
        self.events_df = events_df
        self.nombre_df = None 
        


    def process_data_meets(self, empty_fields, nombre_df):
        # Copiar todos los datos de oevents_df
        set_events_df = self.events_df.copy()
        
        # Agregar campos vacíos con los nombres proporcionados en la lista empty_fields
        for field in empty_fields:
            set_events_df[field] = None
        
        # Asignar el nombre del DataFrame
        self.nombre_df = nombre_df
        
        # Llamar a merge_owner_names para asignar los nombres de los propietarios de events
        set_events_df = self.merge_AccountId_names(set_events_df)

        # Llamar a merge_owner_names para asignar los nombres de los propietarios de events
        set_events_df = self.merge_AccountId_Ext_Ref_Id(set_events_df)

        # Llamar a merge_owner_names para asignar los nombres de los propietarios de events
        set_events_df = self.merge_AccountId_Region(set_events_df)

        # Llamar a merge_owner_names para asignar los nombres de los propietarios de events
        set_events_df = self.merge_CreatedById_Name(set_events_df)

        # Llamar a merge_owner_names para asignar los nombres de los propietarios de events
        set_events_df = self.merge_AccountId_Source(set_events_df)

        # Llamar a merge_owner_names para asignar los nombres de los propietarios de events
        set_events_df = self.merge_AccountId_Source(set_events_df)

        set_events_df = self.merge_AccountId_TopParent(set_events_df)

        set_events_df = self.merge_AccountId_Sale(set_events_df)

        set_events_df = self.merge_AccountId_AcoountTelemarketing(set_events_df)

        set_events_df = self.merge_ExtRefId_Unica(set_events_df)

        set_events_df = self.merge_CreateDate2(set_events_df)

        set_events_df = self.merge_Date(set_events_df)

        set_events_df = self.merge_Inbound(set_events_df)

        set_events_df = self.merge_Inbound_Digital(set_events_df)

        set_events_df = self.merge_Tipo_Outbound(set_events_df)

        set_events_df = self.merge_Target_Market(set_events_df)

        set_events_df = self.merge_Medio(set_events_df)

        set_events_df = self.merge_Debajo50MM(set_events_df)

        set_events_df = self.merge_Año(set_events_df)

        set_events_df = self.merge_x(set_events_df)

        set_events_df = self.merge_Semento_Consejo(set_events_df)
        
        set_events_df = self.merge_column1(set_events_df)
        
        # Retornar el nuevo DataFrame
        print(f'Dataframe [{self.nombre_df}] generado...')
        return set_events_df
    
    def merge_AccountId_names(self, events_df):
        # Crear un diccionario de mapeo entre Id y Name del DataFrame users_df
        id_to_name_mapping = self.accounts_df.set_index('Id')['Name'].to_dict()
        
        # Mapear los valores de 'OwnerId' en opportunities_df utilizando el diccionario de mapeo
        events_df['Account Name'] = events_df['AccountId'].map(id_to_name_mapping)
        
        return events_df

    def merge_AccountId_Ext_Ref_Id(self, events_df):
        # Crear un diccionario de mapeo entre Id y Name del DataFrame users_df
        id_to_name_mapping = self.accounts_df.set_index('Id')['ACC_tx_EXT_REF_ID__c'].to_dict()
        
        # Mapear los valores de 'OwnerId' en opportunities_df utilizando el diccionario de mapeo
        events_df['Ext Ref Id'] = events_df['AccountId'].map(id_to_name_mapping)
        
        return events_df

    def merge_AccountId_Region(self, events_df):
        # Crear un diccionario de mapeo entre Id y Name del DataFrame users_df
        id_to_name_mapping = self.accounts_df.set_index('Id')['Region__c'].to_dict()
        
        # Mapear los valores de 'OwnerId' en opportunities_df utilizando el diccionario de mapeo
        events_df['Region'] = events_df['AccountId'].map(id_to_name_mapping)
        
        return events_df
    
    def merge_CreatedById_Name(self, events_df):
        # Crear un diccionario de mapeo entre Id y Name del DataFrame users_df
        id_to_name_mapping = self.users_df.set_index('Id')['Name'].to_dict()
        
        # Mapear los valores de 'OwnerId' en opportunities_df utilizando el diccionario de mapeo
        events_df['CreateByName'] = events_df['CreatedById'].map(id_to_name_mapping)
        
        return events_df
    
    def merge_AccountId_Source(self, events_df):
        # Crear un diccionario de mapeo entre Id y Name del DataFrame users_df
        id_to_name_mapping = self.accounts_df.set_index('Id')['ACC_tx_Account_Source__c'].to_dict()
        
        # Mapear los valores de 'OwnerId' en opportunities_df utilizando el diccionario de mapeo
        events_df['Account Source'] = events_df['AccountId'].map(id_to_name_mapping)
        
        return events_df

    def merge_AccountId_TopParent(self, events_df):
        # Crear un diccionario de mapeo entre Id y Name del DataFrame users_df
        id_to_name_mapping = self.accounts_df.set_index('Id')['Name'].to_dict()
        
        # Mapear los valores de 'OwnerId' en opportunities_df utilizando el diccionario de mapeo
        events_df['Top Parent Account'] = self.accounts_df['ParentId'].map(id_to_name_mapping)
        
        return events_df

    def merge_AccountId_Sale(self, events_df):
        # Crear un diccionario de mapeo entre Id y Name del DataFrame users_df
        id_to_name_mapping = self.accounts_df.set_index('Id')['ACC_dv_Sales_Annual_Revenue__c'].to_dict()
        
        # Mapear los valores de 'OwnerId' en opportunities_df utilizando el diccionario de mapeo
        events_df['Ventas'] = events_df['AccountId'].map(id_to_name_mapping)
        
        return events_df

    def merge_AccountId_AcoountTelemarketing(self, events_df):
        for index, row in events_df.iterrows():
            Assigned = row['OwnerName__c']
            Date = row['ActivityDate']
            Region = row['Region']
            Ext_Ref_Id = row['Ext Ref Id']
            Ventas = row['Ventas']

            # Verificar si Ventas es NaN o None
            if pd.isna(Ventas) or Ventas is None:
                # Asignar un valor predeterminado o dejarlo como está
                # Aquí, establecemos Ventas como 0, pero puedes cambiarlo según tus necesidades
                Ventas = 0
            else:
                Ventas = int(Ventas)

            # Verificar si la fecha es None
            if Date is None:
                # Asignar una fecha por defecto o dejarla como está
                # En este caso, asignamos la fecha actual
                Date = datetime.now().strftime('%Y-%m-%d')
        
            # Convertir la cadena de fecha a objeto de fecha
            Date = datetime.strptime(Date, '%Y-%m-%d')
            Date_number = Date.toordinal()  # Obtener el número ordinal de la fecha

            if Assigned == "Alberto Mendoza" and Date >= 44562:
                events_df.at[index, 'Cuenta Telemarketing'] = "Digital"
            else:
                if isinstance(Region, str) and Region[:3] == "LMM":
                    events_df.at[index, 'Cuenta Telemarketing'] = "LMM"
                else:
                    if Region == "Digital":
                        events_df.at[index, 'Cuenta Telemarketing'] = Region
                    else:
                        telemarketing_row = events_df[events_df['Ext Ref Id'] == Ext_Ref_Id]
                        if not telemarketing_row.empty:
                            if telemarketing_row['Cuenta Telemarketing'].str[:7].values[0] != "Medical":
                                if Ventas < 50000000:
                                    events_df.at[index, 'Cuenta Telemarketing'] = "LMM"
                                elif int(Ventas) <= (350000000 if Date_number < 44713 else 450000000):
                                    events_df.at[index, 'Cuenta Telemarketing'] = "LMM"
                                else:
                                    events_df.at[index, 'Cuenta Telemarketing'] = "Core"
                            else:
                                events_df.at[index, 'Cuenta Telemarketing'] = telemarketing_row['Cuenta Telemarketing'].values[0][:7]
        return events_df

    def merge_ExtRefId_Unica(self, events_df):
        # Ordenar el DataFrame por 'Ext Ref Id' y restablecer el índice
        events_df = events_df.sort_values(by='Ext Ref Id').reset_index(drop=True)
        
        # Inicializar una lista para almacenar los valores de la columna 'Unica'
        unica_values = []
        # Inicializar un conjunto para almacenar los valores únicos de 'Ext Ref Id' hasta la fila actual
        unique_ext_ref_ids = set()
        
        # Iterar sobre el DataFrame
        for ext_ref_id in events_df['Ext Ref Id']:
            # Comprobar si el valor actual de 'Ext Ref Id' ya se ha encontrado antes
            if ext_ref_id in unique_ext_ref_ids:
                unica_values.append(0)  # Si se encuentra, asignar 0
            else:
                unica_values.append(1)  # Si no se encuentra, asignar 1
                # Agregar el valor actual de 'Ext Ref Id' al conjunto de valores únicos
                unique_ext_ref_ids.add(ext_ref_id)
        
        # Agregar la columna 'Unica' al DataFrame
        events_df['Unica'] = unica_values
        
        return events_df

    def merge_CreateDate2(self, events_df):

        events_df['Created date 2'] = events_df['CreatedDate']

        return events_df

    def merge_Date(self, events_df):
        # Convertir la columna 'ActivityDate' a tipo datetime si no lo es
        events_df['ActivityDate'] = pd.to_datetime(events_df['ActivityDate'])
        
        # Extraer el año y el mes de la columna 'ActivityDate'
        events_df['Mes Cita'] = events_df['ActivityDate'].dt.strftime('%Y%m')
        
        return events_df

    def merge_Inbound(self, events_df):
            # Definir una función para verificar la presencia de una cadena en Meeting Source
        def check_string_presence(meeting_source, keyword):
            return keyword.lower() in str(meeting_source).lower()

        # Aplicar la función para cada palabra clave y asignar el resultado a una nueva columna 'Inbound'
        events_df['Inbound'] = events_df['Meeting_Source__c'].apply(lambda x: 1 if (
            check_string_presence(x, "landing") or
            check_string_presence(x, "formulario") or
            check_string_presence(x, "google") or
            check_string_presence(x, "facebook") or
            check_string_presence(x, "website")
        ) else 0)

        return events_df

    def merge_Inbound_Digital(self, events_df):
        # Definir una función para verificar la presencia de una cadena en Meeting Source
        def check_string_presence(meeting_source, keyword):
            return keyword.lower() in str(meeting_source).lower()
        # Aplicar la función para cada palabra clave y asignar el resultado a una nueva columna 'Inbound Digital'
        events_df[' Inbound Digital'] = events_df['Meeting_Source__c'].apply(lambda x: 1 if (
            check_string_presence(x, "landing") or
            check_string_presence(x, "formulario") or
            check_string_presence(x, "google") or
            check_string_presence(x, "facebook") or
            check_string_presence(x, "website") or
            check_string_presence(x, "micro")
        ) else 0)
        return events_df
    
    def merge_Tipo_Outbound(self, events_df):
            # Definir una función para verificar la presencia de una cadena en Meeting Source
        def check_string_presence(meeting_source, keyword):
            return keyword.lower() in str(meeting_source).lower()

        # Aplicar la función para cada palabra clave y asignar el resultado a una nueva columna 'Tipo Outbound'
        events_df['Tipo Outbound'] = events_df.apply(lambda row: (
            '' if row['Inbound'] == 1 else (
                'BD' if check_string_presence(row['Meeting_Source__c'], "base de datos") else (
                    'Webinar' if check_string_presence(row['Meeting_Source__c'], "webinar") else (
                        'Mailing' if check_string_presence(row['Meeting_Source__c'], "mailing") else (
                            'Vendor' if check_string_presence(row['Meeting_Source__c'], "vendor") else (
                                'TIP' if check_string_presence(row['Meeting_Source__c'], "tip") else (
                                    'Correo' if check_string_presence(row['Meeting_Source__c'], "correo") else (
                                        'Agenda' if check_string_presence(row['Meeting_Source__c'], "agenda") else (
                                            'Completa' if check_string_presence(row['Meeting_Source__c'], "completa") else (
                                                'Llamada' if check_string_presence(row['Meeting_Source__c'], "llamada") else (
                                                    'Evento' if check_string_presence(row['Meeting_Source__c'], "evento") else 'Otro'
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        ), axis=1)

        return events_df

    def merge_Target_Market(self, events_df):

            # Definir una función para categorizar las ventas
        def categorize_sales(value):
            if value == 0:
                return "Sin Ventas"
            elif value < 20000000:
                return "Menor a 20 MM"
            elif value < 50000000:
                return "20 a 50 MM"
            elif value < 350000000:
                return "50 a 350 MM"
            else:
                return "Mayor a 350 MM"

        # Aplicar la función categorize_sales a la columna 'Ventas' y guardar el resultado en 'Target Market'
        events_df['Target Market'] = events_df['Ventas'].apply(categorize_sales)

        return events_df

    def merge_Medio(self, events_df):
            # Definir una función para verificar la presencia de una cadena en Meeting Source
        def check_string_presence(meeting_source, keyword):
            return keyword.lower() in str(meeting_source).lower()
        # Aplicar la función para cada palabra clave y asignar el resultado a una nueva columna 'medio'
        events_df['medio'] = events_df['Meeting_Source__c'].apply(lambda x: "Facebook" if check_string_presence(x, "Facebook")
                                                                else "Google" if check_string_presence(x, "Google")
                                                                else "LinkedIn" if check_string_presence(x, "LinkedIn")
                                                                else "Expansion" if check_string_presence(x, "Expansión")
                                                                else "Website" if check_string_presence(x, "Website") or check_string_presence(x, "Cotizador")
                                                                else "Bing" if check_string_presence(x, "Bing")
                                                                else "Micrositio" if check_string_presence(x, "Micrositio")
                                                                else "Eventos" if check_string_presence(x, "Evento")
                                                                else "Otro")
        return events_df

    def merge_Debajo50MM(self, events_df):

        # Aplicar la función de comparación a la columna 'Ventas' y asignar el resultado a la columna 'debajo de 50MM'
        events_df['debajo de 50MM'] = events_df['Ventas'].apply(lambda x: 1 if x < 50000000 else 0)

        return events_df

    def merge_Año(self, events_df):

            # Convertir la columna 'Created date 2' a tipo datetime si no lo es
        events_df['Created date 2'] = pd.to_datetime(events_df['Created date 2'])
        
        # Convertir las fechas a un formato sin zona horaria
        events_df['Created date 2'] = events_df['Created date 2'].dt.tz_localize(None)
        
        # Extraer el año de la columna 'Created date 2' y guardar el resultado en la columna 'Año'
        events_df['Año'] = events_df['Created date 2'].dt.year
        
        return events_df

    def merge_x(self, events_df):

            # Definir una función para verificar la presencia de una cadena en Meeting Source
        def check_string_presence(meeting_source, keyword):
            return keyword.lower() in str(meeting_source).lower()

        # Aplicar la función para cada palabra clave y asignar el resultado a una nueva columna 'x'
        events_df[' x '] = events_df['medio'].apply(lambda x: "Otro" if x == "Otro"
                                                else "Leasing" if check_string_presence(events_df['Meeting_Source__c'], "Leasing")
                                                else "Cogeneracion" if check_string_presence(events_df['Meeting_Source__c'], "Co-generación") or check_string_presence(events_df['Meeting_Source__c'], "cogeneración")
                                                else "Montacargas" if check_string_presence(events_df['Meeting_Source__c'], "Montacargas")
                                                else "Salud" if check_string_presence(events_df['Meeting_Source__c'], "Salud")
                                                else "Website" if check_string_presence(events_df['Meeting_Source__c'], "Website") or check_string_presence(events_df['Meeting_Source__c'], "Cotizador")
                                                else "Generacion" if check_string_presence(events_df['Meeting_Source__c'], "Generacion") or check_string_presence(events_df['Meeting_Source__c'], "Energía")
                                                else "Remarketing" if check_string_presence(events_df['Meeting_Source__c'], "Remarketing")
                                                else "Transporte" if check_string_presence(events_df['Meeting_Source__c'], "Transporte")
                                                else "Retargeting" if check_string_presence(events_df['Meeting_Source__c'], "Retargeting")
                                                else "Leasing")
    
        return events_df

    def merge_Semento_Consejo(self, events_df):
        def determine_segmento_consejo(row):
            if pd.isnull(row['OwnerName__c']) or pd.isnull(row['ActivityDate']):
                return "Unknown"
            
            if row['OwnerName__c'] == "Alberto Mendoza" and row['ActivityDate'] >= datetime(1900, 1, 1) + timedelta(days=44562):
                return "Digital"
            elif row['OwnerName__c'] == "Digital":
                return "Digital"
            elif pd.notnull(row['Region']) and row['Region'][:3] == "LMM":
                return "LMM"
            elif row['ActivityDate'] < datetime(1900, 1, 1) + timedelta(days=44713):
                if not pd.isnull(row['Ventas']) and row['Ventas'] <= 350000000:
                    return "LMM"
            else:
                if not pd.isnull(row['Ventas']) and row['Ventas'] <= 450000000:
                    return "LMM"
                else:
                    return "Core"
            
            return "Unknown"  # Por si acaso ninguno de los casos anteriores se cumple

        events_df['Segmento Consejo'] = events_df.apply(determine_segmento_consejo, axis=1)
        return events_df

    def merge_column1(self, events_df):
        events_df['ActivityDate'] = pd.to_datetime(events_df['ActivityDate'])

        # Definir la fecha de referencia como 1 de enero de 1900
        reference_date = datetime(1900, 1, 1)

        def check_date(row):
            # Restar la fecha de referencia de la fecha actual y convertir a días
            days_difference = (row['ActivityDate'] - reference_date).days
            if days_difference < 44713:
                return 1
            else:
                return 0
        
        events_df['Column2'] = events_df.apply(check_date, axis=1)

        return events_df




    def export_to_excel(self, df, filename):
        # Exportar el DataFrame a un archivo Excel
        df.to_excel(filename, index=False)
        print('Excel generado...')
