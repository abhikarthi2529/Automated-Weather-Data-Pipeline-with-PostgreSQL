from extract import ExtractWeatherData

class Transform:
    def __init__(self):
        ewd = ExtractWeatherData()
        response = ewd.get_request()
        self.data = ewd.process_hourly_data(response)
    def removeNull(self):
        # if there is a null value in a row, we drop that row.
        self.data = self.data.dropna()
        
    def timezone_to_pacific(self):
        self.data['date'] = self.data['date'].dt.tz_convert('America/Vancouver')
    
    def add_twelve_hour_clock(self):
        time = self.data['date'].dt.strftime('%I:%M%p')
        self.data['time'] = time
      
    def add_year(self):
        year = self.data['date'].dt.date
        self.data['year'] = year
    
    def remove_unnecessary_columns(self, columns_to_remove):
        self.data = self.data.drop(columns=columns_to_remove, axis=1)
        
    
    def get_data(self):
        return self.data
    
    def transform_raw_data(self):
        self.removeNull()
        self.timezone_to_pacific()
        self.add_twelve_hour_clock()
        self.add_year()
        self.remove_unnecessary_columns('date')
        return self.get_data()
    











