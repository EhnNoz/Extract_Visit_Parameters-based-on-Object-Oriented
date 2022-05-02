class ManualEPG:
    def __init__(self,data_frame,start_time):
        self.start_time = start_time
        self.data_frame = data_frame

    def insert_start_time(self,input_row):
        if input_row == 2000:
            output = self.start_time + ' ' + '12:10:00 AM'
        elif input_row == 2001:
            output = self.start_time + ' ' + '02:00:00 AM'
        elif input_row == 2002:
            output = self.start_time + ' ' + '04:00:00 AM'
        elif input_row == 2003:
            output = self.start_time + ' ' + '06:00:00 AM'
        elif input_row == 2004:
            output = self.start_time + ' ' + '08:00:00 AM'
        elif input_row == 2005:
            output = self.start_time + ' ' + '10:00:00 AM'
        elif input_row == 2006:
            output = self.start_time + ' ' + '12:01:00 PM'
        elif input_row == 2007:
            output = self.start_time + ' ' + '02:00:00 PM'
        elif input_row == 2008:
            output = self.start_time + ' ' + '04:00:00 PM'
        elif input_row == 2009:
            output = self.start_time + ' ' + '06:00:00 PM'
        elif input_row == 2010:
            output = self.start_time + ' ' + '08:00:00 PM'
        elif input_row == 2011:
            output = self.start_time + ' ' + '10:00:00 PM'
        return output

    def insert_end_time(self, input_row):
        if input_row == 2000:
            output = self.start_time + ' ' + '02:00:00 AM'
        elif input_row == 2001:
            output = self.start_time + ' ' + '04:00:00 AM'
        elif input_row == 2002:
            output = self.start_time + ' ' + '06:00:00 AM'
        elif input_row == 2003:
            output = self.start_time + ' ' + '08:00:00 AM'
        elif input_row == 2004:
            output = self.start_time + ' ' + '10:00:00 AM'
        elif input_row == 2005:
            output = self.start_time + ' ' + '12:01:00 PM'
        elif input_row == 2006:
            output = self.start_time + ' ' + '02:00:00 PM'
        elif input_row == 2007:
            output = self.start_time + ' ' + '04:00:00 PM'
        elif input_row == 2008:
            output = self.start_time + ' ' + '06:00:00 PM'
        elif input_row == 2009:
            output = self.start_time + ' ' + '08:00:00 PM'
        elif input_row == 2010:
            output = self.start_time + ' ' + '10:00:00 PM'
        elif input_row == 2011:
            output = self.start_time + ' ' + '11:59:00 PM'
        return output



