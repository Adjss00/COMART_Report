import pandas as pd

class SetOpps:
    def __init__(self, accounts_df, users_df, opportunities_df):
        self.accounts_df = accounts_df
        self.users_df = users_df
        self.opportunities_df = opportunities_df
        self.nombre_df = None 

    def process_data(self, empty_fields, nombre_df):
        # Copiar todos los datos de opportunities_df
        set_oportunidades_df = self.opportunities_df.copy()
        
        # Agregar campos vacíos con los nombres proporcionados en la lista empty_fields
        for field in empty_fields:
            set_oportunidades_df[field] = None
        
        # Asignar el nombre del DataFrame
        self.nombre_df = nombre_df
        
        # Llamar a merge_owner_names para asignar los nombres de los propietarios de oportunidades
        set_oportunidades_df = self.merge_owner_names(set_oportunidades_df)
        
        # Retornar el nuevo DataFrame
        print(f'Dataframe [{self.nombre_df}] generado...')
        return set_oportunidades_df
    
    def merge_owner_names(self, opportunities_df):
        # Crear un diccionario de mapeo entre Id y Name del DataFrame users_df
        id_to_name_mapping = self.users_df.set_index('Id')['Name'].to_dict()
        
        # Mapear los valores de 'OwnerId' en opportunities_df utilizando el diccionario de mapeo
        opportunities_df['Opportunity Owner'] = opportunities_df['OwnerId'].map(id_to_name_mapping)
        
        return opportunities_df

    def export_to_excel(self, df, filename):
        # Exportar el DataFrame a un archivo Excel
        df.to_excel(filename, index=False)
        print('Excel generado...')
