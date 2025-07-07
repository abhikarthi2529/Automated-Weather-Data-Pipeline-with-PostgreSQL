import os 
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine
from transform import Transform

class Load:
    def __init__(self):
        dotenv_path = find_dotenv()
        load_dotenv(dotenv_path)
        self.data = Transform().transform_raw_data()
        
    def get_credentials(self):
        username = os.getenv('username_postgres')
        password = os.getenv('password')
        return {'username':username, 'password':password}
    
    def load_data_to_db(self, get_credentials):
       engine =  create_engine(f"postgresql+psycopg2://{get_credentials['username']}:{get_credentials['password']}@localhost/Projects_data")
       self.data.to_sql('weather_historical_data', engine, if_exists='replace', index=False)
    

l = Load()
l.load_data_to_db(l.get_credentials())




