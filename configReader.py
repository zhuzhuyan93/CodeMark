import configparser 

class configReader:
    def __init__(self, configFilePath="../config.ini") -> None:
        self.configFilePath = configFilePath 

    def initReader(self):
        iniReader = configparser.ConfigParser() 
        iniReader.read(self.configFilePath) 
        return iniReader
    
    def readDBInfo(self, DBName):
        iniReader = self.initReader() 
        host = iniReader.get(DBName, 'host')
        port = iniReader.get(DBName, 'port')
        username = iniReader.get(DBName, 'username')
        password = iniReader.get(DBName, 'password')
        return host, port, username, password
