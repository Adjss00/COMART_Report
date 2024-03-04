from simple_salesforce import Salesforce

class SalesforceObjectLister:
    def __init__(self, username, password, security_token, domain='login'):
        self.username = username
        self.password = password
        self.security_token = security_token
        self.domain = domain
        self.sf = Salesforce(username=self.username, password=self.password, security_token=self.security_token, domain=self.domain)

    def list_all_objects(self):
        # Obtiene la descripción de todos los objetos de Salesforce
        objects = self.sf.describe()['sobjects']
        object_names = [obj['name'] for obj in objects]
        return object_names

if __name__ == "__main__":
    username = 'jesus.sanchez@engen.com.mx'
    password = '21558269Antonio#'
    security_token = 'WqRoCDbMwhMPZ62iWUcXnmbmg'

    lister = SalesforceObjectLister(username, password, security_token)
    object_names = lister.list_all_objects()
    
    print("Objetos en Salesforce:")
    for name in object_names:
        print(name)
