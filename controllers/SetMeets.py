import pandas as pd
from datetime import datetime

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
        print('Region' in events_df.columns)

        for index, row in events_df.iterrows():
            Assigned = row['OwnerName__c']
            Date = row['ActivityDate']
            Region = row['Region']
            Ext_Ref_Id = row['Ext Ref Id']
            Ventas = row['Ventas']

            print("Assigned",Assigned, "-", type(Assigned))
            print("Date",Date, "-", type(Date))
            print("Region",Region, "-", type(Region))
            print("Ext_Ref_Id",Ext_Ref_Id, "-", type(Ext_Ref_Id))
            print("Ventas",Ventas, "-", type(Ventas))

            if Assigned == "Alberto Mendoza" and Date >= 44562:
                events_df.at[index, 'Cuenta Telemarketing'] = "Digital"
            else:
                if Region[:3] == "LMM":
                    events_df.at[index, 'Cuenta Telemarketing'] = "LMM"
                else:
                    if Region == "Digital":
                        events_df.at[index, 'Cuenta Telemarketing'] = Region
                    else:
                        telemarketing_row = events_df[events_df['Ext Ref Id'] == Ext_Ref_Id]
                        if not telemarketing_row.empty:
                            if telemarketing_row['Cuentas Telemarketing'].str[:7].values[0] != "Medical":
                                if Ventas < 50000000:
                                    events_df.at[index, 'Cuenta Telemarketing'] = "LMM"
                                elif Ventas <= (350000000 if Date < 44713 else 450000000):
                                    events_df.at[index, 'Cuenta Telemarketing'] = "LMM"
                                else:
                                    events_df.at[index, 'Cuenta Telemarketing'] = "Core"
                            else:
                                events_df.at[index, 'Cuenta Telemarketing'] = telemarketing_row['Cuentas Telemarketing'].values[0][:7]

        return events_df

    def export_to_excel(self, df, filename):
        # Exportar el DataFrame a un archivo Excel
        df.to_excel(filename, index=False)
        print('Excel generado...')
