import sys
import csv
from datetime import datetime
import math

class Tariff:
    def __init__(self, data):
        self.price_incoming_up_to = float(data['price_incoming_up_to']);
        self.price_incoming_after_to = float(data['price_incoming_after_to']);
        self.price_outgoing_up_to = float(data['price_outgoing_up_to']);
        self.price_outgoing_after_to = float(data['price_outgoing_after_to']);
        self.price_sms = float(data['price_sms']);
        self.time = datetime.strptime(data['time'], '%H:%M').time();


class Subscriber:
    def __init__(self, number, tariff):
        self.number = number;
        self.tariff = tariff;
        self.calls_incoming_duration = {"up_to": 0, "after_to": 0};
        self.calls_outgoing_duration = {"up_to": 0, "after_to": 0};
        self.sms_count = 0;
        self.data = [];

    def add_call_incoming(self, data):
        self.data.append(', '.join(data.values()));
        t = datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S').time();
        if ( t < self.tariff.time ):
            self.calls_incoming_duration['up_to'] += float(data['call_duration']);
        else:
            self.calls_incoming_duration['after_to'] += float(data['call_duration']);
            
    def add_call_outgoing(self, data):
        self.data.append(', '.join(data.values()));
        t = datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S').time();
        if ( t < self.tariff.time ):
            self.calls_outgoing_duration['up_to'] += float(data['call_duration']);
        else:
            self.calls_outgoing_duration['after_to'] += float(data['call_duration']);
        
    def add_sms(self, count):
        self.sms_count += int(count);

    def calculate(self):
        incoming_up_to = ( math.ceil(self.calls_incoming_duration['up_to'])
                            * self.tariff.price_incoming_up_to );
        incoming_after_to = ( math.ceil(self.calls_incoming_duration['after_to'])
                            * self.tariff.price_incoming_after_to );
        outgoing_up_to = ( math.ceil(self.calls_outgoing_duration['up_to'])
                            * self.tariff.price_outgoing_up_to );
        outgoing_after_to = ( math.ceil(self.calls_outgoing_duration['after_to'])
                            * self.tariff.price_outgoing_after_to );
        sms = self.sms_count * self.tariff.price_sms;

        return incoming_up_to + incoming_after_to + outgoing_up_to + outgoing_after_to + sms;

    def print_tariffing(self):
        price = self.calculate();
        print( 'Subscriber\'s tariffing: {} RUB.'.format(price) );
        print( 'Data records:' );
        print( '\n'.join(self.data) );
        
        #for s in self.data:
        #    print(s);



def parse(data_path, number, tariff_path):

    f_tariff = open(tariff_path, 'r');
    tariff_data = csv.DictReader(f_tariff);
    tariff = Tariff(  list(tariff_data)[0]  );    
    f_tariff.close();

    
    subscriber = Subscriber(number, tariff);

    
    f_data = open(data_path, 'r');
    data = csv.DictReader(f_data);
    
    for row in data:
        if ( subscriber.number == row['msisdn_origin'] ):
            subscriber.add_call_outgoing(row);
            subscriber.add_sms(row['sms_number']);
        if ( subscriber.number == row['msisdn_dest'] ):
            subscriber.add_call_incoming(row);
            subscriber.add_sms(row['sms_number']);

    f_data.close();

    subscriber.print_tariffing();


def main():

    if ( len( sys.argv ) > 1 ):
        data_path = sys.argv[1];
        number = sys.argv[2];
        tariff_path = sys.argv[3];
    else:
        data_path = 'data.csv';
        number = '933156729';
        tariff_path = 'tariff.csv';
         
    parse(data_path, number, tariff_path)


main();
