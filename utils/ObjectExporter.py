from simple_salesforce import Salesforce
import pandas as pd

class SalesforceDataExtractor:
    def __init__(self, username, password, security_token, domain='login'):
        self.username = username
        self.password = password
        self.security_token = security_token
        self.domain = domain
        self.sf = Salesforce(username=self.username, password=self.password, security_token=self.security_token, domain=self.domain)

    def extract_and_save_to_csv(self, file_name, save_path, object_name):
        print(f"**Recuperando metadatos para {object_name}...**")  # Indicador de recuperación de metadatos
        metadata_objeto = self.sf.__getattr__(object_name).describe()

        print(f"**Extrayendo campos de {object_name}...**")  # Indicador de extracción de campos
        nombres_campos = [campo['name'] for campo in metadata_objeto['fields']]

        print(f"**Consultando registros de {object_name}...**")  # Indicador de consulta de registros
        registros = self.sf.query_all("SELECT {} FROM {}".format(", ".join(nombres_campos), object_name))

        print(f"**Procesando registros de {object_name}...**")  # Indicador de procesamiento de registros
        registros_data = [registro for registro in registros['records']]
        df = pd.DataFrame(registros_data)

        print(f"**Guardando datos de {object_name} en CSV...**")  # Indicador de guardado de datos
        file_path = save_path + '/' + file_name
        df.to_csv(file_path, index=False)
        print(f"**Datos guardados correctamente en '{file_path}'**")

if __name__ == "__main__":
    username = 'jesus.sanchez@engen.com.mx'
    password = '21558269Antonio#'
    security_token = 'WqRoCDbMwhMPZ62iWUcXnmbmg'

    extractor = SalesforceDataExtractor(username, password, security_token)
    extractor.extract_and_save_to_csv('salesforce_data_account.csv', 'utils/data', 'Account')
    extractor.extract_and_save_to_csv('salesforce_data_user.csv', 'utils/data', 'User')
    extractor.extract_and_save_to_csv('salesforce_data_opportunity.csv', 'utils/data', 'Opportunity')
    extractor.extract_and_save_to_csv('salesforce_data_event.csv', 'utils/data', 'Event')

